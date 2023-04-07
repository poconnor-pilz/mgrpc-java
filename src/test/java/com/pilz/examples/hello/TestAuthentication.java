package com.pilz.examples.hello;

import com.pilz.errors.HttpServerCancelsAndShutdowns;
import com.pilz.mqttgrpc.MqttChannel;
import com.pilz.mqttgrpc.MqttServer;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.pilz.utils.Pause.pause;

public class TestAuthentication {

    private static final Logger log = LoggerFactory.getLogger(TestAuthentication.class);

    @Test
    void testHttpInterceptor() throws Exception {


        //Test a normal http interceptor to see how it works
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

        int port = 50051;
        Server httpServer = ServerBuilder.forPort(port)
                .addService(new ListenForHello())
                .intercept(interceptor)
                .build().start();
        String target = "localhost:" + port;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        try {
            final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
            HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
            stub.sayHello(joe);
            latch.await();

        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }


    @Test
    void tryMqttInterceptor() throws Exception {

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

        final String DEVICE = "device1";
        MqttServer server = new MqttServer(MqttUtils.makeClient(Topics.systemStatus(DEVICE), "tcp://localhost:1883"), DEVICE);
        server.init();

        final ListenForHello helloService = new ListenForHello();
        final ServerServiceDefinition serviceWithIntercept = ServerInterceptors.intercept(
                helloService,
                interceptor);
        server.addService(serviceWithIntercept);
        MqttChannel channel = new MqttChannel(MqttUtils.makeClient(null, "tcp://localhost:1883"), DEVICE);
        channel.init();


        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.sayHello(joe);

        channel.close();
        server.close();

    }


}
