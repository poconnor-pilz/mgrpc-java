package io.mgrpc.errors;

import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.NoopStreamObserver;
import io.mgrpc.utils.DirectExecutor;
import io.mgrpc.utils.Pause;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is used to observe the behaviour of standard grpc for cancels and shutdowns as this
 * behaviour does not seem to be documented clearly anywhere.
 */
@Disabled
public class HttpServerCancelsAndShutdowns {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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


    @Test
    void clientSendsCancel() throws Exception {


        //The log from this test shows that on timeout
        //the server cancel handler will be called, CancellationListener will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {
                Context.current().addListener(new Context.CancellationListener() {
                    @Override
                    public void cancelled(Context context) {
                        log.error("Context CancellationListener called");
                    }
                }, new DirectExecutor());

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
            final StreamObserver<HelloRequest> inStream = stub.bidiHello(cancelableObserver);
            inStream.onNext(joe);
            //Wait before cancelling, for the call to setup and the first message to go through
            Pause.pause(500);
            log.debug("Sending cancel");
            cancelableObserver.cancel("tryit");


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
    void clientSendsCancelUsingContext() throws Exception {


        //The log from this test shows that on timeout
        //the server cancel handler will be called, CancellationListener will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {
                Context.current().addListener(new Context.CancellationListener() {
                    @Override
                    public void cancelled(Context context) {
                        log.error("Context CancellationListener called");
                    }
                }, new DirectExecutor());
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
            final Context.CancellableContext withCancellation = Context.current().withCancellation();
            final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
            withCancellation.run(()->{
                HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
                final StreamObserver<HelloRequest> inStream = stub.bidiHello(new ObserverLogger("client"));
                //Wait before cancelling, for the call to setup and the first message to go through
                Pause.pause(500);
                inStream.onNext(joe);
                withCancellation.cancel(new Exception("atestexception"));
            });
            Thread.sleep(10000);
            log.debug("waiting");
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }

    @Test
    void clientSendsError() throws Exception {

        //This is an invalid scenario. The client sends an error on an input stream. It should never have a reason
        //to do this that is not satisfied by a cancel
        //The behaviour that grpc does is that the "client" Observer logger will get onError()
        //CANCELLED: Cancelled by client with StreamObserver.onError()
        //But the server cancel handler does not get called and the server reqeust stream ObserverLogger
        //does not get onError() called.

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {

                Context.current().addListener(new Context.CancellationListener() {
                    @Override
                    public void cancelled(Context context) {
                        log.error("Context CancellationListener called");
                    }
                }, new DirectExecutor());


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


            final StreamObserver<HelloRequest> requestStream = stub.bidiHello(new ObserverLogger("client"));
            requestStream.onNext(joe);
            final StatusRuntimeException sre = new StatusRuntimeException(Status.INTERNAL.withDescription("atesterrror"));
            //Wait before cancelling, for the call to setup and the first message to go through
            Pause.pause(500);
            requestStream.onError(sre);
            log.debug("waiting");
            Thread.sleep(1000);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }


    @Test
    void timeout() throws Exception {

        //The log from this test shows that on timeout
        //the server cancel handler will be called, the CancellationListener will be called
        //and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client will receive onError() with
        //io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: deadline exceeded after 0.460260700s. [closed=[], open=[[buffered_nanos=128620000, remote_addr=localhost/127.0.0.1:50051]]]

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);



        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {

                Context.current().addListener(new Context.CancellationListener() {
                    @Override
                    public void cancelled(Context context) {
                        log.error("Context CancellationListener called");
                    }
                }, new DirectExecutor());


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
                    .newStub(channel).withDeadlineAfter(500, TimeUnit.HOURS);
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
        //then the server cancel handler will be called, the CancellationListener will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);

        class ListenForCancel extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {

                Context.current().addListener(new Context.CancellationListener() {
                    @Override
                    public void cancelled(Context context) {
                        log.error("Context CancellationListener called");
                    }
                }, Executors.newSingleThreadExecutor());

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

    //@Test
    void slowServerFastClient() throws Exception {

        //This is an invalid scenario. The client sends an error on an input stream. It should never have a reason
        //to do this that is not satisfied by a cancel
        //The behaviour that grpc does is that the "client" Observer logger will get onError()
        //CANCELLED: Cancelled by client with StreamObserver.onError()
        //But the server cancel handler does not get called and the server reqeust stream ObserverLogger
        //does not get onError() called.

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class SlowService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    private ArrayList<String> names = new ArrayList<>();
                    @Override
                    public void onNext(HelloRequest value) {
                        log.debug("lotsOfGreetings received " + value);
                        names.add(value.getName());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Error in client stream", t);
                    }

                    @Override
                    public void onCompleted() {
                        log.debug("lotsOfGreetings onCompleted()");
                        HelloReply reply = HelloReply.newBuilder().setMessage("Hello there").build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    }
                };
            }
        }

        int port = 50051;
        Server httpServer = ServerBuilder.forPort(port)
                .addService(new SlowService())
                .build().start();
        String target = "localhost:" + port;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        class ResponseWaiter extends ObserverLogger{

            public CountDownLatch latch = new CountDownLatch(1);
            ResponseWaiter(String name) {
                super(name);
            }

            @Override
            public void onCompleted() {
                super.onCompleted();
                latch.countDown();
            }
        }

        try {
            final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
            HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
            ResponseWaiter responseWaiter = new ResponseWaiter("responsewaiter");
            final StreamObserver<HelloRequest> requestStream = stub.lotsOfGreetings(responseWaiter);
            for (int i = 0; i < 100000; i++) {
                requestStream.onNext(joe);
            }
            log.debug("Sent all requests");
            responseWaiter.latch.await();
        }
        catch (Exception ex){
            log.error("Failed", ex);
        }
        finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            httpServer.shutdown();
        }
    }


}
