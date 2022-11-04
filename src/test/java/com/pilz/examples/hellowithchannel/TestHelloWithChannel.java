package com.pilz.examples.hellowithchannel;

import com.pilz.examples.hello.HelloService;
import com.pilz.examples.hello.HelloSkeleton;
import com.pilz.examples.hello.HelloStub;
import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerStream;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestHelloWithChannel {

    private static Logger log = LoggerFactory.getLogger(TestHelloWithChannel.class);

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private HelloService service;
    private HelloStub stub;

    private static final String DEVICE = "device1";

    private static final String SERVICE_BASE_TOPIC = "helloworld.ExampleHelloService";

    private static final long REQUEST_TIMEOUT = 2000;

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


        //Set up the server
        MqttGrpcServer mqttGrpcServer = new MqttGrpcServer(serverMqtt, DEVICE);
        mqttGrpcServer.init();
        service = new HelloService();
        HelloSkeleton skeleton = new HelloSkeleton(service);
        mqttGrpcServer.subscribeService(SERVICE_BASE_TOPIC, skeleton);

    }


    static class ExampleHelloServiceImpl extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase{
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            log.debug("ExampleHelloServiceImpl received: " + request);
            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        }
    }

    @Test
    public void testServerCall(){

        //Make an InputStream from a HelloRequest
        final HelloRequest hr = HelloRequest.newBuilder().setName("joe").build();
        InputStream stream = new ByteArrayInputStream(hr.toByteArray());


        final ExampleHelloServiceImpl exampleHelloService = new ExampleHelloServiceImpl();
        final ServerServiceDefinition serverServiceDefinition = exampleHelloService.bindService();
        final ServerMethodDefinition<?, ?> sayHello = serverServiceDefinition.getMethod("helloworld.ExampleHelloService/SayHello");
        final Object theHelloRequest = sayHello.getMethodDescriptor().parseRequest(stream);
        final ServerCallHandler serverCallHandler = sayHello.getServerCallHandler();
        ServerCall serverCall = new ServerCallTry(sayHello.getMethodDescriptor());
        final ServerCall.Listener listener = serverCallHandler.startCall(serverCall, new Metadata());
        listener.onMessage(theHelloRequest);
        //notify this is the end of the client stream after which the onMessage() will go through
        listener.onHalfClose();


    }

    static class ServerCallTry<ReqT, RespT> extends ServerCall<ReqT,RespT>{

        final MethodDescriptor<ReqT, RespT> methodDescriptor;

        ServerCallTry(MethodDescriptor<ReqT, RespT> methodDescriptor) {
            this.methodDescriptor = methodDescriptor;
        }

        @Override
        public void request(int numMessages) {

        }

        @Override
        public void sendHeaders(Metadata headers) {

        }

        @Override
        public void sendMessage(RespT message) {
            log.debug("Got a response");
        }

        @Override
        public void close(Status status, Metadata trailers) {

        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return methodDescriptor;
        }
    }

    @Test
    public void testInProcess() throws Exception{
        String uniqueName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(uniqueName)
                .directExecutor()
                .addService(new ExampleHelloServiceImpl() {
                })
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName)
                .directExecutor()
                .build();

        ChannelWrapper wrapper = new ChannelWrapper(channel);
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(wrapper);
        final HelloRequest name = HelloRequest.newBuilder().setName("test name").build();
        HelloReply reply = blockingStub.sayHello(name);
        assertEquals("Hello test name", reply.getMessage());


        channel.shutdown();
        server.shutdown();

    }


    @Test
    public void testSayHello() {
        //Test local and remote calls to the service
        //Setup the client stub
        MqttChannel channel = new MqttChannel(clientMqtt, DEVICE);
        channel.init();
        ChannelWrapper wrapper = new ChannelWrapper(channel);
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(wrapper);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = blockingStub.sayHello(joe);
        assertEquals("Hello joe", helloReply.getMessage());

    }

    //@Test
    public void testSayHelloCallBack() {
        //Test local and remote calls to the service
        //Setup the client stub
        MqttChannel mqttChannel = new MqttChannel(clientMqtt, DEVICE);
        mqttChannel.init();
        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(mqttChannel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        CountDownLatch latch = new CountDownLatch(1);
        String[] result = {""};
        stub.sayHello(joe, new StreamObserver<HelloReply>() {
            @Override
            public void onNext(HelloReply value) {
                result[0] = value.getMessage();
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }



        assertEquals("Hello joe", result[0]);

    }


}
