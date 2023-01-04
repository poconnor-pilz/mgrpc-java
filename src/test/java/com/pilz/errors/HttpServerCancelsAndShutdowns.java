package com.pilz.errors;

import com.pilz.mqttgrpc.NoopStreamObserver;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This is used to observe the behaviour of standard grpc for cancels and shutdowns as this
 * behaviour does not seem to be documented clearly anywhere.
 */
public class HttpServerCancelsAndShutdowns {

    private static final Logger log = LoggerFactory.getLogger(HttpServerCancelsAndShutdowns.class);

    @Test
    void testClientShutdown() throws Exception{

        //The log from this test shows that when the channel is shutdown before the client sends an onCompleted
        //then the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);

        class ListenForCancel extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {

                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(()->{
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                });

                return new StreamObserver<HelloRequest>() {
                    private ArrayList<String> names = new ArrayList<>();
                    @Override
                    public void onNext(HelloRequest value) {
                        log.debug("lotsOfGreetings received " + value);
                        names.add(value.getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Error in client stream", t);
                    }

                    @Override
                    public void onCompleted() {
                        log.debug("lotsOfGreetings onCompleted()");
                        String everyone = "";
                        for(String name: names){
                            everyone += name + ",";
                        }
                        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + everyone).build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    }
                };
            }
        }

        int port = 50051;
        Server httpServer = ServerBuilder.forPort(port)
                .addService(new ListenForCancel())
                .build()
                .start();

        String target = "localhost:" + port;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

       try {
        ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest request = HelloRequest.newBuilder().setName("joe").build();
        HelloReply response;
        try {
            final StreamObserver<HelloRequest> inStream = stub.lotsOfGreetings(new NoopStreamObserver<HelloReply>());
            inStream.onNext(request);
            inStream.onNext(request);
            Thread.sleep(1000);
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        } catch (StatusRuntimeException e) {
            log.error("RPC failed: {0}", e.getStatus());
            return;
        }
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }

}
