package io.mgrpc.errors;

import io.grpc.Context;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

class ListenForCancel extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public ErrorObserver errorObserver = new ErrorObserver("server");
    public CountDownLatch contextListenerCancelled = new CountDownLatch(1);
    final CountDownLatch serverCancelHandlerCalled = new CountDownLatch(1);

    @Override
    public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {

        final Context current = Context.current();
        current.addListener(new Context.CancellationListener() {
            @Override
            public void cancelled(Context context) {
                contextListenerCancelled.countDown();
                log.debug("Context CancellationListener called");
            }
        }, Executors.newSingleThreadExecutor());


        ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
        serverObserver.setOnCancelHandler(() -> {
            log.debug("ServerCallStreamObserver cancel handler called");
            serverCancelHandlerCalled.countDown();
            log.debug("Latch toggled");
        });
        return this.errorObserver;
    }
}
