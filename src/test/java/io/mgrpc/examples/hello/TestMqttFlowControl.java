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
import io.mgrpc.mqtt.MqttServerBuilder;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

//@Disabled
class TestMqttFlowControl {

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
    public void  testServerQueueCapacityExceeded() throws Exception {

        final String serverId = Id.shortRandom();
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setQueueSize(10)
                .setCreditSize(Integer.MAX_VALUE) //no effective base flow control
                .setTopic(serverId).build();
        server.start();

        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(10000)
                .setCreditSize(Integer.MAX_VALUE).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        FlowControlTests.testServerQueueCapacityExceeded(server, channel);

        messageChannel.close();
        server.close();

    }

    @Test
    public void testClientQueueCapacityExceeded() throws Exception {
        final String serverId = Id.shortRandom();
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setQueueSize(1000)
                .setCreditSize(Integer.MAX_VALUE) //no effective base flow control
                .setTopic(serverId).build();
        server.start();

        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(10)
                .setCreditSize(Integer.MAX_VALUE).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        FlowControlTests.testClientQueueCapacityExceeded(server, channel);

        messageChannel.close();
        server.close();

    }


    @Test
    public void testClientStreamFlowControl() throws Exception {

        final String serverId = Id.shortRandom();
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setQueueSize(10) // small queue but it should not overflow because of flow control
                .setCreditSize(10) //Only issue 10 credits each time to prevent queue from overflowing
                .setTopic(serverId).build();
        server.start();

        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(99)
                .setCreditSize(99).build();

        FlowControlTests.testClientStreamFlowControl(server, messageChannel.forTopic(serverId), Status.OK);

        messageChannel.close();
        server.close();

    }

    @Test
    public void testClientStreamFlowControlWithFail() throws Exception {


        final String serverId = Id.shortRandom();
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setQueueSize(10) // small queue but that should overflow
                .setCreditSize(99) //Issue 99 credits each time to cause overflow
                .setTopic(serverId).build();
        server.start();

        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(99)
                .setCreditSize(99).build();

        FlowControlTests.testClientStreamFlowControl(server, messageChannel.forTopic(serverId), Status.RESOURCE_EXHAUSTED);

        messageChannel.close();
        server.close();

    }


    @Test
    public void testServerStreamFlowControl() throws Exception {


        //Make a service that blocks until the test flips a latch
        //While the service is blocked try to overlflow the internal MessageServer queue and verify
        //That it doesn't cause a problem because flow control is enabled
        //Then verify that when the service is unblocked it eventually pulls all the messages from the server

        final String serverId = Id.shortRandom();
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setQueueSize(1000)
                .setCreditSize(1000)
                .setTopic(serverId).build();
        server.start();

        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(1000)
                .setCreditSize(10).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        FlowControlTests.testServerStreamFlowControl(server, channel);

        messageChannel.close();
        server.close();

    }
    @Test
    void testCancelServerStream() throws Exception {

        //This tests a badly written service that doesn't check if the call was
        //cancelled before calling onNext().
        //In this case the if the onNext() call blocked because of lack of flow credit
        //Then the call thread would hang forever.
        //This test verifies that the MsgServerCall will not block the onNext() when it knows that
        //the call has been cancelled. Instead it just does not send on the message and allows the onNext() to proceed.

        class CancelableService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

            public CountDownLatch contextListenerCancelled = new CountDownLatch(1);
            final CountDownLatch serverCancelHandlerCalled = new CountDownLatch(1);
            //Wait for 6 messages after which the service should block waiting for flow message
            final CountDownLatch messagesSent = new CountDownLatch(5);
            final CountDownLatch oneMessageSent = new CountDownLatch(1);

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
                        //In normal code this loop should check if the call is cancelled before calling onNext()
                        for (int i = 1; i < 6; i++) {
                            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + value.getName() + i).build();
                            responseObserver.onNext(reply);
                            log.debug("Sent reply " + i);
                            oneMessageSent.countDown();
                            messagesSent.countDown();
                            try {
                                //Wait for cancel before sending more than one message
                                //After this all other onNext() calls will be ignored by MsgServerCall
                                contextListenerCancelled.await(5, TimeUnit.SECONDS);
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
        MessageServer server = new MqttServerBuilder().setClient(serverMqtt).setTopic(serverTopic).build();
        server.addService(cancelableService);
        server.start();

        //Set the flow credit to 3 so that the service will not have enough credit to send all 6 messages
        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setCreditSize(3)
                .build();

        Channel channel = TopicInterceptor.intercept(messageChannel, serverTopic);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final CancelableObserver cancelableObserver = new CancelableObserver();

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(cancelableObserver);
        inStream.onNext(joe);

        assertTrue((cancelableService.oneMessageSent.await(5, TimeUnit.SECONDS)));
        log.debug("Sending cancel");
        cancelableObserver.cancel("acancel");

        //Verify that the service does not get blocked and the for loop completes despite not having flow credit.
        //MsgServerCall will simply ignore the messages because the call was cancelled.
        assertTrue(cancelableService.messagesSent.await(5, TimeUnit.SECONDS));

        //The server cancel handler should get called
        assertTrue(cancelableService.serverCancelHandlerCalled.await(5, TimeUnit.SECONDS));

        //Verify that the Context.CancellationListener gets called
        assertTrue(cancelableService.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //Verify that on the client side the CancelableObserver.onError gets called with CANCEL
        assertTrue(cancelableObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(cancelableObserver.exception.getStatus().getCode(), Status.CANCELLED.getCode());


    }

    @Test
    public void testClientStreamOnReady() throws Exception {

        final String serverId = Id.shortRandom();
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setQueueSize(1000)
                .setCreditSize(10)
                .setTopic(serverId).build();
        server.start();

        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(1000)
                .setCreditSize(10).build();

        FlowControlTests.testClientStreamOnReady(server, messageChannel.forTopic(serverId));

        messageChannel.close();
        server.close();

    }
}
