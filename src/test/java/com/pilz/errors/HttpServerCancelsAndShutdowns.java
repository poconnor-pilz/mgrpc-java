package com.pilz.errors;

import com.pilz.mqttgrpc.NoopStreamObserver;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is used to observe the behaviour of standard grpc for cancels and shutdowns as this
 * behaviour does not seem to be documented clearly anywhere.
 */
public class HttpServerCancelsAndShutdowns {

    private static final Logger log = LoggerFactory.getLogger(HttpServerCancelsAndShutdowns.class);

    class CancelableObserver implements ClientResponseObserver<HelloRequest, HelloReply> {
        private ClientCallStreamObserver requestStream;
        private final CountDownLatch latch;

        CancelableObserver(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver reqStream) {
            requestStream = reqStream;
        }

        public void cancel(String message) {
            log.debug("CancelableObserver cancel()");
            requestStream.cancel(message, null);
        }

        @Override
        public void onNext(HelloReply value) {
            log.debug("next");
        }

        @Override
        public void onError(Throwable t) {
            log.debug("CancelableObserver onError()", t);
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            log.debug("CancelableObserver onCompleted()");
            latch.countDown();
        }
    }

    class ObserverLogger implements StreamObserver{
        private final String name;
        ObserverLogger(String name) {
            this.name = name;
        }
        @Override
        public void onNext(Object o) {
            log.debug("onNext() " + name);
        }
        @Override
        public void onError(Throwable throwable) {
            log.debug("onError() " + name, throwable);

        }
        @Override
        public void onCompleted() {
            log.debug("onNext() " + name);
        }
    }



    @Test
    void clientSendsCancel() throws Exception {


        //The log from this test shows that on timeout
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {
                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(() -> {
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                });
                return new ObserverLogger("server");
            }
        }

        int port = 50051;
        Server httpServer = ServerBuilder.forPort(port)
                .addService(new ListenForTimeout())
                .build().start();
        String target = "localhost:" + port;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        try {
            final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
            HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

            CountDownLatch clientCancelledLatch = new CountDownLatch(1);
            final CancelableObserver cancelableObserver = new CancelableObserver(clientCancelledLatch);
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.debug("Sending cancel");
                cancelableObserver.cancel("tryit");
            });

            final StreamObserver<HelloRequest> inStream = stub.bidiHello(cancelableObserver);
            inStream.onNext(joe);


            log.debug("waiting");
            //Verify that on the client side the CancelableObserver.onError gets called
            assert (clientCancelledLatch.await(5, TimeUnit.SECONDS));
            //Verify that on the server side the errors service cancel handler gets called
            assert (serverCancelledLatch.await(5, TimeUnit.SECONDS));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }


    @Test
    void timeout() throws Exception {

        //The log from this test shows that on timeout
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client will receive onError() with
        //io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: deadline exceeded after 0.460260700s. [closed=[], open=[[buffered_nanos=128620000, remote_addr=localhost/127.0.0.1:50051]]]

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);

        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {
                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(() -> {
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                });
                return new ObserverLogger("server");
            }
        }

        int port = 50051;
        Server httpServer = ServerBuilder.forPort(port)
                .addService(new ListenForTimeout())
                .build()
                .start();

        String target = "localhost:" + port;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        try {
            ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc
                    .newStub(channel).withDeadlineAfter(500, TimeUnit.MILLISECONDS);
            HelloRequest request = HelloRequest.newBuilder().setName("joe").build();
            try {
                final StreamObserver<HelloRequest> inStream = stub.bidiHello(new ObserverLogger("client"));
                inStream.onNext(request);
                Thread.sleep(1000);
            } catch (StatusRuntimeException e) {
                log.error("RPC failed: {0}", e.getStatus());
                return;
            }
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }


    @Test
    void clientShutdown() throws Exception {

        //The log from this test shows that when the channel is shutdown before the client sends an onCompleted
        //then the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);

        class ListenForCancel extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {

                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(() -> {
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                });

                return new ObserverLogger("server");
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
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }


}
