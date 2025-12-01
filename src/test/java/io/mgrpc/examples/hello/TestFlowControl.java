package io.mgrpc.examples.hello;


import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.errors.CancelableObserver;
import io.mgrpc.mqtt.MqttChannelBuilder;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
class TestFlowControl {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;


    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClients() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
    }

    @Test
    void testBasicFlow() throws Exception {


        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String serverTopic = Id.shortRandom();
        MessageServer server = new MessageServer(new MqttServerConduit(serverMqtt, serverTopic));
        server.start();

        server.addService(new HelloServiceForTest());

        MessageChannel messageChannel = new MqttChannelBuilder().setClient(clientMqtt).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverTopic);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub =
                ExampleHelloServiceGrpc.newBlockingStub(channel);
        final HelloRequest request = HelloRequest.newBuilder().
                setName("joe")
                .setNumResponses(55).build();

        final Iterator<HelloReply> helloReplyIterator = stub.lotsOfReplies(request);
        while(helloReplyIterator.hasNext()) {
            final HelloReply reply = helloReplyIterator.next();
            log.debug(reply.getMessage());
            Thread.sleep(50);
        }

        messageChannel.close();
        server.close();

    }

    @Test
    void testCancel() throws Exception {


        class CancelableService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

            public CountDownLatch contextListenerCancelled = new CountDownLatch(1);
            final CountDownLatch serverCancelHandlerCalled = new CountDownLatch(1);
            //Wait for 21 messages after which the service should block waiting for flow message
            final CountDownLatch twentyOneMessagesSent = new CountDownLatch(21);

            boolean cancelled = false;

            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {

                final Context current = Context.current();
                current.addListener(context -> {
                    contextListenerCancelled.countDown();
                    log.debug("Context CancellationListener called");
                }, Executors.newSingleThreadExecutor());


                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(() -> {
                    log.debug("ServerCallStreamObserver cancel handler called");
                    cancelled = true;
                    serverCancelHandlerCalled.countDown();
                    log.debug("Latch toggled");
                });
                return new StreamObserver<HelloRequest>() {
                    @Override
                    public void onNext(HelloRequest value) {
                        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + value.getName()).build();
                        while(!cancelled) {
                            responseObserver.onNext(reply);
                            log.debug("Sent reply");
                            twentyOneMessagesSent.countDown();
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    @Override
                    public void onError(Throwable t) {
                        log.error("Error in client stream", t);
                    }

                    @Override
                    public void onCompleted() {
                        responseObserver.onCompleted();
                    }
                };
            }
        }

        final CancelableService cancelableService = new CancelableService();

        final String serverTopic = Id.shortRandom();
        MessageServer server = new MessageServer(new MqttServerConduit(serverMqtt, serverTopic));
        server.addService(cancelableService);
        server.start();

        MessageChannel messageChannel = new MqttChannelBuilder().setClient(clientMqtt).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverTopic);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final CancelableObserver cancelableObserver = new CancelableObserver(200);

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(cancelableObserver);
        inStream.onNext(joe);

        //Wait for at least one message to go through before canceling to make sure the call is fully started.
        cancelableService.twentyOneMessagesSent.await(5, TimeUnit.SECONDS);
        log.debug("Sending cancel");
        cancelableObserver.cancel("tryit");

        //The server cancel handler should get called
        assertTrue(cancelableService.serverCancelHandlerCalled.await(5, TimeUnit.SECONDS));

        //Verify that the Context.CancellationListener gets called
        assertTrue(cancelableService.contextListenerCancelled.await(5, TimeUnit.SECONDS));


        //Verify that on the client side the CancelableObserver.onError gets called with CANCEL
        assertTrue(cancelableObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(cancelableObserver.exception.getStatus().getCode(), Status.CANCELLED.getCode());

        Thread.sleep(20000);

    }
}
