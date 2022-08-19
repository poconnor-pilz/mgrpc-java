package com.pilz.examples.hello;

import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.ProtoServiceManager;
import com.pilz.mqttgrpc.StreamWaiter;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHello {


    private static MqttAsyncClient makeClient() throws MqttException{
        final MqttAsyncClient client;
        client = new MqttAsyncClient(
                "tcp://localhost:1883",
                MqttAsyncClient.generateClientId(),
                new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        client.connect(mqttConnectOptions).waitForCompletion();
        return client;
    }


    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private HelloService service;
    private HelloStub stub;



    @BeforeAll
    public static void makeMqttClients() throws MqttException {
        serverMqtt = makeClient();
        clientMqtt = makeClient();
    }

    @AfterAll
    public static void closeMqttClients() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
    }

    @BeforeEach
    void setup() throws Exception{

        String serviceBaseTopic = "helloservice";

        //Set up the server
        ProtoServiceManager protoServiceManager = new ProtoServiceManager(serverMqtt);
        service = new HelloService();
        HelloSkeleton skeleton = new HelloSkeleton(service);
        protoServiceManager.subscribeService(serviceBaseTopic, skeleton);

        //Setup the client stub
        ProtoSender sender = new ProtoSender(clientMqtt, serviceBaseTopic);
        stub = new HelloStub(sender);
    }



    @Test
    public void testRequestResponse() throws Throwable{
        //Test local and remote calls to the service
        testRequestResponse(service);
        testRequestResponse(stub);
    }

    public void testRequestResponse(IHelloService helloService) throws Throwable{

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        helloService.requestResponse(joe, waiter);
        HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe", reply.getMessage());
    }

    @Test
    public void testServerStream() throws Throwable{
        //Test local and remote calls to the service
        testServerStream(service);
        testServerStream(stub);
    }
    public void testServerStream(IHelloService helloService) throws Throwable{

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        helloService.serverStream(joe, waiter);
        List<HelloReply> responseList = waiter.getList();
        assertEquals(responseList.size(), 2);
        assertEquals("Hello joe", responseList.get(0).getMessage());
        assertEquals("Hello joe", responseList.get(1).getMessage());

    }

    @Test
    public void testClientStream() throws Throwable {
        //Test local and remote calls to the service
        testClientStream(service);
        testClientStream(stub);
    }


    public void testClientStream(IHelloService helloService) throws Throwable{

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        StreamObserver<HelloRequest> clientStreamObserver = helloService.clientStream(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onNext(jane);
        clientStreamObserver.onCompleted();
        final HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe,jane,", reply.getMessage());
    }

    @Test
    public void testClientAndServerStream() throws Throwable {
        //Test local and remote calls to the service
        testClientAndServerStream(service);
        testClientAndServerStream(stub);
    }

    public void testClientAndServerStream(IHelloService helloService) throws Throwable{


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
        StreamObserver<HelloRequest> clientStreamObserver = helloService.clientAndServerStream(replyObserver);
        clientStreamObserver.onNext(joe);
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello joe", replyObserver.lastReply.getMessage());
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onNext(jane);
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello jane", replyObserver.lastReply.getMessage());
    }


}
