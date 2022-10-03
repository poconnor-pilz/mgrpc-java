package com.pilz.examples.hello;

import com.pilz.mqttgrpc.MqttGrpcClient;
import com.pilz.mqttgrpc.MqttGrpcServer;
import com.pilz.mqttgrpc.StreamWaiter;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHello {


    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private HelloService service;
    private HelloStub stub;

    private static final String DEVICE = "device1";
    private static final String SERVICE_NAME = "helloservice";

    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();

        serverMqtt = MqttUtils.makeClient(Topics.systemStatus(DEVICE));
        clientMqtt = MqttUtils.makeClient(null);
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

        String serviceBaseTopic = "helloservice";

        //Set up the server
        MqttGrpcServer mqttGrpcServer = new MqttGrpcServer(serverMqtt, DEVICE);
        mqttGrpcServer.init();
        service = new HelloService();
        HelloSkeleton skeleton = new HelloSkeleton(service);
        mqttGrpcServer.subscribeService(serviceBaseTopic, skeleton);

        //Setup the client stub
        MqttGrpcClient mgClient = new MqttGrpcClient(clientMqtt, DEVICE);
        mgClient.init();
        stub = new HelloStub(mgClient, SERVICE_NAME);
    }



    @Test
    public void testSayHello() throws Throwable{
        //Test local and remote calls to the service
        testSayHello(service);
        testSayHello(stub);
    }

    public void testSayHello(IHelloService helloService) throws Throwable{

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        helloService.sayHello(joe, waiter);
        HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe", reply.getMessage());
    }

    @Test
    public void testLotsOfReplies() throws Throwable{
        //Test local and remote calls to the service
        testLotsOfReplies(service);
        testLotsOfReplies(stub);
    }
    public void testLotsOfReplies(IHelloService helloService) throws Throwable{

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        helloService.lotsOfReplies(joe, waiter);
        List<HelloReply> responseList = waiter.getList();
        assertEquals(responseList.size(), 2);
        assertEquals("Hello joe", responseList.get(0).getMessage());
        assertEquals("Hello again joe", responseList.get(1).getMessage());

    }

    @Test
    public void testLotsOfGreetings() throws Throwable {
        //Test local and remote calls to the service
        testLotsOfGreetings(service);
        testLotsOfGreetings(stub);
    }


    public void testLotsOfGreetings(IHelloService helloService) throws Throwable{

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        StreamObserver<HelloRequest> clientStreamObserver = helloService.lotsOfGreetings(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onNext(jane);
        clientStreamObserver.onCompleted();
        final HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe,jane,", reply.getMessage());
    }

    @Test
    public void testBidiHello() throws Throwable {
        //Test local and remote calls to the service
        testBidiHello(service);
        testBidiHello(stub);
    }

    public void testBidiHello(IHelloService helloService) throws Throwable{


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

            }
        }
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        TestHelloReplyObserver replyObserver = new TestHelloReplyObserver();
        StreamObserver<HelloRequest> clientStreamObserver = helloService.bidiHello(replyObserver);
        clientStreamObserver.onNext(joe);
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello joe", replyObserver.lastReply.getMessage());
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onNext(jane);
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello jane", replyObserver.lastReply.getMessage());
    }


    @Test
    public void testBlockingStub() throws Throwable{
        //Test local and remote calls to the service
        testBlockingStub(service);
        testBlockingStub(stub);
    }

    public void testBlockingStub(IHelloService helloService) throws Throwable{

        HelloBlockingStub blockingStub = new HelloBlockingStub(service);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloReply reply = blockingStub.requestResponse(joe);
        assertEquals("Hello joe", reply.getMessage());

        List<HelloReply> responseList = new ArrayList<>();
        final Iterator<HelloReply> helloReplyIterator = blockingStub.serverStream(joe);
        while (helloReplyIterator.hasNext()) {
            responseList.add(helloReplyIterator.next());
        }
        assertEquals(responseList.size(), 2);
        assertEquals("Hello joe", responseList.get(0).getMessage());
        assertEquals("Hello again joe", responseList.get(1).getMessage());

    }


}
