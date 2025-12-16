package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.MessageServer;
import io.mgrpc.utils.StatusObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowControlTests {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void testServerQueueCapacityExceeded(MessageServer server, Channel channel) throws Exception {

        //Verify that when flow control is not set then it is possible to make the internal server
        //buffer/queue overflow

        final CountDownLatch serviceLatch = new CountDownLatch(1);

        class BlockedService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    @Override
                    public void onNext(HelloRequest request) {
                        log.debug("Service received: " + request.getName());
                        //block the queue
                        try {
                            serviceLatch.await();
                        } catch (InterruptedException e) {
                            log.error("", e);
                        }
                    }
                    @Override
                    public void onError(Throwable throwable) {
                        log.error("server onError()", throwable);
                    }
                    @Override
                    public void onCompleted() {}
                };
            }
        }
        server.addService(new BlockedService());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        StatusObserver clientStatusObserver = new StatusObserver("client");
        final StreamObserver client = stub.lotsOfGreetings(clientStatusObserver);


        for (int i = 0; i < 15; i++) {
            HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
            try {
                client.onNext(request);
            } catch (StatusRuntimeException e) {
                //We may or may not get this failure depending on timing but if we do
                //it is expected because the call will be closed when the queue overflows
                log.debug("Exception thrown during onNext()");
                assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), e.getStatus().getCode());
                break;
            }
        }


        Status clientStatus = clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS);
        checkStatus(Status.RESOURCE_EXHAUSTED, clientStatus);
        assertEquals("Service queue capacity = 10 exceeded.", clientStatus.getDescription());
        serviceLatch.countDown();

    }


    public static void testClientQueueCapacityExceeded(MessageServer server, Channel channel) throws Exception{


        //Verify that if the server sends a lot of messages to a client that is blocked and there is no
        // flow control then the client queue limit is reached.
        //The test code should get an error and the server should get a cancel so that it stops sending messages.
        //and the input stream to the server should get an error.


        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class BlockingListenForCancel extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

            class StatusObserverX extends StatusObserver {
                public StreamObserver<HelloReply> responseObserver;
                StatusObserverX(String name) {
                    super(name);
                }
                public void setResponseObserver(StreamObserver<HelloReply> responseObserver){
                    this.responseObserver = responseObserver;
                }

                @Override
                public void onNext(Object o) {
                    HelloReply reply = HelloReply.newBuilder().setMessage("a reply").build();
                    //Cause the blocked client's queue to overflow
                    for(int i = 0; i < 15; i++){
                        responseObserver.onNext(reply);
                    }
                }
            }
            StatusObserverX errorObserver = new StatusObserverX("server");

            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {

                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(() -> {
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                });
                this.errorObserver.setResponseObserver(responseObserver);
                return this.errorObserver;
            }
        }

        final BlockingListenForCancel listenForCancel = new BlockingListenForCancel();
        server.addService(listenForCancel);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest request = HelloRequest.newBuilder().setName("a request").build();
        final CountDownLatch clientLatch = new CountDownLatch(1);
        StatusObserver clientStatusObserver = new StatusObserver("client") {
            @Override
            public void onNext(Object o) {
                try {
                    //Block so that queue fills up
                    clientLatch.await();
                } catch (InterruptedException e) {
                    log.error("", e);
                }
                super.onNext(o);
            }
        };

        StreamObserver<HelloRequest> requestStream = stub.bidiHello(clientStatusObserver);

        requestStream.onNext(request);

        //The server cancel handler should get called
        assertTrue(serverCancelledLatch.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        checkStatus(Status.CANCELLED, listenForCancel.errorObserver.waitForStatus(10, TimeUnit.SECONDS));

        //The client should receive an onError() RESOURCE_EXHAUSTED
        checkStatus(Status.RESOURCE_EXHAUSTED, clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS));

    }

    public static void testClientStreamFlowControl(MessageServer server, Channel channel) throws Exception {

        //Make a service that blocks until the test flips a latch
        //While the service is blocked try to overlflow the internal MessageServer queue and verify
        //That it doesn't cause a problem because the broker has buffered the messages
        //Then verify that when the service is unblocked it eventually pulls all the messages from the server

        final CountDownLatch serviceLatch = new CountDownLatch(1);
        final CountDownLatch firstMessageLatch = new CountDownLatch(1);
        class BlockedService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            public List<HelloRequest> list = new ArrayList<>();

            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    @Override
                    public void onNext(HelloRequest request) {
                        log.debug("Service received: " + request.getName());
                        list.add(request);
                        firstMessageLatch.countDown();
                        if(list.size() == 15){
                            responseObserver.onNext(HelloReply.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                        //block the internal queue
                        try {
                            serviceLatch.await();
                        } catch (InterruptedException e) {
                            log.error("", e);
                        }
                    }
                    @Override
                    public void onError(Throwable throwable) {
                        log.error("server onError()", throwable);
                    }
                    @Override
                    public void onCompleted() {}
                };
            }
        }

        final BlockedService blockedService = new BlockedService();
        server.addService(blockedService);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        StatusObserver clientStatusObserver = new StatusObserver("client");
        final StreamObserver client = stub.lotsOfGreetings(clientStatusObserver);

        //Run the client stream on a thread because client.onNext() will block when it runs out of credit
        //
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 15; i++) {
                    HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
                    client.onNext(request);
                }
            }
        });
        t.start();

        //Wait until at least one message is received
        firstMessageLatch.await(5, TimeUnit.SECONDS);
        assertEquals(1, blockedService.list.size());


        //Allow some time to make sure messages got to broker
        Thread.sleep(100);

        //There should still only be one message because the service is blocked
        assertEquals(1, blockedService.list.size());
        log.debug("Unblocking client stream");

        //unblock the service
        serviceLatch.countDown();

        checkStatus(Status.OK, clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS));

        //client.onCompleted();

        assertEquals(15, blockedService.list.size());

    }


    public static void testServerStreamFlowControl(MessageServer server, Channel channel) throws Exception {

        //Make a service that blocks until the test flips a latch
        //While the service is blocked try to overlflow the internal MessageServer queue and verify
        //That it doesn't cause a problem because the broker has buffered the messages or flow control is enabled
        //Then verify that when the service is unblocked it eventually pulls all the messages from the server

       class ServiceThatTriesToCauseOverflow extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                for (int i = 0; i < 15; i++) {
                    HelloReply reply = HelloReply.newBuilder().setMessage("reply " + i).build();
                    responseObserver.onNext(reply);
                }
                responseObserver.onCompleted();
            }
        }

        final ServiceThatTriesToCauseOverflow serviceThatTriesToCauseOverflow = new ServiceThatTriesToCauseOverflow();
        server.addService(serviceThatTriesToCauseOverflow);

        final CountDownLatch blockingLatch = new CountDownLatch(1);
        class BlockingServerStream implements StreamObserver<HelloReply> {

            public List<HelloReply> list = new ArrayList<>();

            public CountDownLatch completedLatch = new CountDownLatch(1);

            @Override
            public void onNext(HelloReply reply) {
                log.debug("Client received: " + reply.getMessage());
                list.add(reply);
                //block the internal queue
                try {
                    blockingLatch.await();
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("onError()", throwable);
            }

            @Override
            public void onCompleted() {
                completedLatch.countDown();
            }
        };

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);

        BlockingServerStream blockingServerStream = new BlockingServerStream();
        stub.lotsOfReplies(HelloRequest.newBuilder().setName("test").build(), blockingServerStream);

        //Allow some time to make sure messages got to broker
        Thread.sleep(100);

        log.debug("Unblocking server stream");
        //unblock the service
        blockingLatch.countDown();

        assertTrue(blockingServerStream.completedLatch.await(5, TimeUnit.SECONDS));
        assertEquals(15, blockingServerStream.list.size());

    }

    private static void checkStatus(Status expected, Status actual){
        assertEquals(expected.getCode(), actual.getCode());
    }
}
