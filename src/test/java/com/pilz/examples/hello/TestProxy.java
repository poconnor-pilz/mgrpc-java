package com.pilz.examples.hello;

import com.google.protobuf.MessageLite;
import com.pilz.mqttgrpc.GrpcProxy;
import com.pilz.mqttgrpc.MqttChannel;
import com.pilz.mqttgrpc.MqttServer;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestProxy {

    private static final Logger log = LoggerFactory.getLogger(TestProxy.class);


    @Test
    public void testClientSideProxy() throws Exception{


        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                log.debug("Interceptor called");
                return next.startCall(call, headers);
            }
        };

        final CountDownLatch latch = new CountDownLatch(1);
        class ListenForHello extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                final HelloReply reply = HelloReply.newBuilder().setMessage("hi").build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                latch.countDown();
            }
        }

        //HttpChannel -> HttpServer -> GrpcProxy -> MqttChannel -> Broker -> MqttServer

        int port = 50051;
        String target = "localhost:" + port;
        final String DEVICE = "device1";

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        final String brokerUrl = "tcp://localhost:1883";
        final MqttAsyncClient clientMqttConnection = MqttUtils.makeClient(null, brokerUrl);
        MqttChannel mqttChannel = new MqttChannel(clientMqttConnection, DEVICE);
        mqttChannel.init();

        GrpcProxy<byte[], byte[]> proxy = new GrpcProxy<>(mqttChannel);

        Server httpServer = ServerBuilder.forPort(port)
                .fallbackHandlerRegistry(new GrpcProxy.Registry(proxy))
                .build().start();

        final MqttAsyncClient serverMqttConnection = MqttUtils.makeClient(null, brokerUrl);
        MqttServer mqttServer = new MqttServer(serverMqttConnection, DEVICE);
        mqttServer.addService(new ListenForHello());
        mqttServer.init();

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(httpChannel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = stub.sayHello(joe);

        assertEquals("hi", helloReply.getMessage());

        latch.await();

        httpChannel.shutdown();
        httpServer.shutdown();
        mqttChannel.close();
        mqttServer.close();
//        clientMqttConnection.close();
//        serverMqttConnection.close();
    }


    @Test
    public void testServerSideProxy() throws Exception{


        final CountDownLatch latch = new CountDownLatch(1);
        class ListenForHello extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                final HelloReply reply = HelloReply.newBuilder().setMessage("hi").build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                latch.countDown();
            }
        }

        //MqttChannel -> Broker -> MqttServer ->  GrpcProxy -> HttpChannel -> HttpServer

        int port = 50051;
        String target = "localhost:" + port;
        final String DEVICE = "device1";

        final String brokerUrl = "tcp://localhost:1883";
        final MqttAsyncClient clientMqttConnection = MqttUtils.makeClient(null, brokerUrl);
        MqttChannel mqttChannel = new MqttChannel(clientMqttConnection, DEVICE);
        mqttChannel.init();

        Server httpServer = ServerBuilder.forPort(port)
                .addService(new ListenForHello())
                .build().start();

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();
        final GrpcProxy<byte[], byte[]> proxy = new GrpcProxy<>(httpChannel);


        final MqttAsyncClient serverMqttConnection = MqttUtils.makeClient(null, brokerUrl);
        MqttServer mqttServer = new MqttServer(serverMqttConnection, DEVICE);
        mqttServer.setFallBackRegistry(new GrpcProxy.Registry(proxy));
        mqttServer.init();

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(mqttChannel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = stub.sayHello(joe);

        assertEquals("hi", helloReply.getMessage());

        latch.await();

        httpChannel.shutdown();
        httpServer.shutdown();
        mqttChannel.close();
        mqttServer.close();
//        clientMqttConnection.close();
//        serverMqttConnection.close();
    }


}
