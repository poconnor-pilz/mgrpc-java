package io.mgrpc.examples.hello;


import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.errors.CancelableObserver;
import io.mgrpc.mqtt.MqttChannelBuilder;
import io.mgrpc.mqtt.MqttServerBuilder;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import io.mgrpc.utils.StatusObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

//@Disabled
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
    void testServerStreamFlow() throws Exception {


        final String serverTopic = Id.shortRandom();
        MessageServer server = new MessageServer(new MqttServerConduit(serverMqtt, serverTopic));
        server.start();

        server.addService(new HelloServiceForTest());

        //Set up a call with the flow credit larger than the queue size and verify that
        //the queue overflows and causes the call to fail
        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(10)
                .setFlowCredit(20).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverTopic);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub =
                ExampleHelloServiceGrpc.newBlockingStub(channel);
        final HelloRequest request = HelloRequest.newBuilder().
                setName("joe")
                .setNumResponses(30).build();

        final Iterator<HelloReply> helloReplyIterator = stub.lotsOfReplies(request);
        Exception ex = null;
        try{
            Thread.sleep(500); //Make sure the queue gets filled up
            while(helloReplyIterator.hasNext()) {
                final HelloReply reply = helloReplyIterator.next();
            }
        } catch (Exception e) {
            ex = e;
        }

        //The queue size is smaller than the flow credit so the call should fail when the queue overflows
        assertNotNull(ex);
        assertTrue(ex instanceof StatusRuntimeException);
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, ((StatusRuntimeException)ex).getStatus().getCode());

        messageChannel.close();


        //Set up a call with the flow credit smaller than the queue size and verify that
        //it works and all messages get through
        MessageChannel messageChannel2 = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(10)
                .setFlowCredit(5).build();
        Channel channel2 = TopicInterceptor.intercept(messageChannel2, serverTopic);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub2 =
                ExampleHelloServiceGrpc.newBlockingStub(channel2);
        final HelloRequest request2 = HelloRequest.newBuilder().
                setName("joe")
                .setNumResponses(32).build();

        final Iterator<HelloReply> helloReplyIterator2 = stub2.lotsOfReplies(request2);

        int count = 0;
        while(helloReplyIterator2.hasNext()) {
            final HelloReply reply = helloReplyIterator2.next();
            count ++;
        }

        assertEquals(32, count);

        server.close();

    }


    @Test
    public void testClientStreamFlow() throws Exception {

        final String serverTopic = Id.shortRandom();

        //Make a server with queue size 10 but flow credit 100. This will cause the queue to overflow
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setTopic(serverTopic)
                .setQueueSize(10)
                .setFlowCredit(100).build();
        server.start();


        class BlockedService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            public int messagesReceived = 0;
            public CountDownLatch blockingLatch = new CountDownLatch(1);
            public CountDownLatch completedLatch = new CountDownLatch(1);
            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    @Override
                    public void onNext(HelloRequest request) {
                        log.debug("Service received: " + request.getName());
                        messagesReceived++;
                        //block the queue
                        try {
                            //make sure we are beyond the initial flow credit qouta
                            //so that the clientStream gets sent credits of 100 and then has enough credit
                            //to send enough messages to flood the queue
                            if(messagesReceived > 10) {
                                blockingLatch.await();
                            }
                        } catch (InterruptedException e) {
                            log.error("", e);
                        }
                    }
                    @Override
                    public void onError(Throwable throwable) {
                        log.error("server onError()", throwable);
                    }
                    @Override
                    public void onCompleted() {
                        completedLatch.countDown();
                    }
                };
            }
        }

        BlockedService blockedService = new BlockedService();
        server.addService(blockedService);

        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setQueueSize(100)
                .setFlowCredit(99).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverTopic);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        StatusObserver clientStatusObserver = new StatusObserver("clientStream");
        final StreamObserver clientStream = stub.lotsOfGreetings(clientStatusObserver);

        for (int i = 0; i < 30; i++) {
            HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
            clientStream.onNext(request);
        }

        Status clientStatus = clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS);
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), clientStatus.getCode());
        assertEquals("Service queue capacity = 10 exceeded.", clientStatus.getDescription());
        blockedService.blockingLatch.countDown();

        server.close();

        //Make a server with queue size greater than flow credit and verify that it does not overflow the queue
        //because the client will stop sending when it runs out of credit.
        server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setTopic(serverTopic)
                .setQueueSize(100)
                .setFlowCredit(15).build();
        server.start();

        blockedService = new BlockedService();
        server.addService(blockedService);

        clientStatusObserver = new StatusObserver("clientStream");
        final StreamObserver clientStream2 = stub.lotsOfGreetings(clientStatusObserver);

        final int[] numSent = new int[1];

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 30; i++) {
                    HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
                    clientStream2.onNext(request);
                    numSent[0]++;
                }
            }
        });
        t1.start();

        //Wait for some time. The thread above should be blocked on onNext() until we unlock the blockingLatch
        //at which point the service can continue to process messages.
        Thread.sleep(500);

        //16 messages should be sent because the channel has an initial credit of 1 and then
        //gets credit of 15 more
        assertEquals(numSent[0], 16);
        blockedService.blockingLatch.countDown();
        blockedService.completedLatch.await(5, TimeUnit.SECONDS);

        assertEquals(30, blockedService.messagesReceived);
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
            //Wait for 21 messages after which the service should block waiting for flow message
            final CountDownLatch messagesSent = new CountDownLatch(6);
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
        MessageServer server = new MessageServer(new MqttServerConduit(serverMqtt, serverTopic));
        server.addService(cancelableService);
        server.start();

        //Set the flow credit to 3 so that the service will not have enough credit to send all 6 messages
        MessageChannel messageChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setFlowCredit(3)
                .build();

        Channel channel = TopicInterceptor.intercept(messageChannel, serverTopic);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final CancelableObserver cancelableObserver = new CancelableObserver();

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(cancelableObserver);
        inStream.onNext(joe);

        cancelableService.oneMessageSent.await(5, TimeUnit.SECONDS);
        log.debug("Sending cancel");
        cancelableObserver.cancel("acancel");

        //Verify that the service does not get blocked and the for loop completes despite not having flow credit.
        //MsgServerCall will simply ignore the messages because the call was cancelled.
        cancelableService.messagesSent.await(5, TimeUnit.SECONDS);

        //The server cancel handler should get called
        assertTrue(cancelableService.serverCancelHandlerCalled.await(5, TimeUnit.SECONDS));

        //Verify that the Context.CancellationListener gets called
        assertTrue(cancelableService.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //Verify that on the client side the CancelableObserver.onError gets called with CANCEL
        assertTrue(cancelableObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(cancelableObserver.exception.getStatus().getCode(), Status.CANCELLED.getCode());


    }
}
