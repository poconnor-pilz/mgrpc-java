package com.pilz.examples.hello;

import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import com.pilz.utils.ToList;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestHello {

    private static Logger log = LoggerFactory.getLogger(TestHello.class);

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private MqttChannel channel;
    private MqttServer server;

    private static final String DEVICE = "device1";

    private static final long REQUEST_TIMEOUT = 2000;

    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();

        serverMqtt = MqttUtils.makeClient(Topics.systemStatus(DEVICE), "tcp://localhost:1883");
        clientMqtt = MqttUtils.makeClient(null, "tcp://localhost:1883");
    }

    @AfterAll
    public static void stopClientsAndBroker() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
        MqttUtils.stopEmbeddedBroker();
    }

    @BeforeEach
    void setup() throws Exception{

        //Set up the serverb
        server = new MqttServer(serverMqtt, DEVICE);
        server.init();
        server.addService(new HelloServiceForTest());
        channel = new MqttChannel(clientMqtt, DEVICE);
        channel.init();
    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
    }

    public void checkForLeaks(int numActiveCalls){
        assertEquals(numActiveCalls, channel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, server.getStats().getActiveCalls());
    }

    @Test
    public void testSayHello() {
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = blockingStub.sayHello(joe);
        assertEquals("Hello joe", helloReply.getMessage());
        checkForLeaks(0);
    }


    @Test
    public void testLotsOfReplies() throws Throwable{

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc
                .newBlockingStub(channel)
                .withDeadlineAfter(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        HelloRequest joe = HelloRequest.newBuilder().setName("2").build();
        List<HelloReply> responseList = ToList.toList(stub.lotsOfReplies(joe));
        assertEquals(responseList.size(), 2);
        assertEquals("Hello 0", responseList.get(0).getMessage());
        assertEquals("Hello 1", responseList.get(1).getMessage());
        checkForLeaks(0);
    }

    @Test
    public void testParallelReplies() throws Throwable{

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("10").build();
        int numRequests = 100;
        final CountDownLatch latch = new CountDownLatch(numRequests);
        for(int i = 0; i < numRequests; i++) {
            final int index = i;
            stub.lotsOfReplies(joe, new NoopStreamObserver<HelloReply>() {
                @Override
                public void onNext(HelloReply value) {
                    log.debug(index + " - " + value.getMessage());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            });
        }

        latch.await();
        checkForLeaks(0);
    }


    @Test
    public void testLotsOfGreetings(){

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>(REQUEST_TIMEOUT);
        StreamObserver<HelloRequest> clientStreamObserver = stub.lotsOfGreetings(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onNext(jane);
        clientStreamObserver.onCompleted();
        final HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe,jane,", reply.getMessage());
        checkForLeaks(0);
    }


    @Test
    public void testBidiHello() throws Throwable{

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);

        class TestHelloReplyObserver implements StreamObserver<HelloReply> {
            public HelloReply lastReply;
            public CountDownLatch latch = new CountDownLatch(1);

            @Override
            public void onNext(HelloReply value) {
                lastReply = value;
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        }
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        TestHelloReplyObserver replyObserver = new TestHelloReplyObserver();
        StreamObserver<HelloRequest> clientStreamObserver = stub.bidiHello(replyObserver);
        clientStreamObserver.onNext(joe);
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello joe", replyObserver.lastReply.getMessage());
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onNext(jane);
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello jane", replyObserver.lastReply.getMessage());
        //close the call cleanly
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onCompleted();
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        checkForLeaks(0);
    }




    @Test
    public void testInProcess() throws Exception{
        String uniqueName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(uniqueName)
                .directExecutor()
                .addService(new HelloServiceForTest() {
                })
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName)
                .directExecutor()
                .build();

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        final HelloRequest name = HelloRequest.newBuilder().setName("test name").build();
        HelloReply reply = blockingStub.sayHello(name);
        assertEquals("Hello test name", reply.getMessage());

        channel.shutdown();
        server.shutdown();
        checkForLeaks(0);
    }




}
