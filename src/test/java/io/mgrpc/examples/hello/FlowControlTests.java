package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.MessageServer;
import io.mgrpc.utils.StatusObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

public class FlowControlTests {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void testServerQueueCapacityExceeded(MessageServer server, Channel channel) throws Exception {

        //Verify that when flow control is not set then it is possible to make the internal server
        //buffer/queue overflow

        final CountDownLatch serviceLatch = new CountDownLatch(1);

        class BlockedService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

            private int numMessagesReceived = 0;
            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    @Override
                    public void onNext(HelloRequest request) {
                        log.debug("Service received: " + request.getName());
                        numMessagesReceived++;
                        //block the queue
                        try {
                            //Allow at least 2 messages through before blocking
                            //because the server only sends flow credit after it gets 2 messages.
                            if(numMessagesReceived == 2) {
                                serviceLatch.await();
                            }
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

            private int numMessagesReceived = 0;

            @Override
            public void onNext(HelloReply reply) {
                log.debug("Client received: " + reply.getMessage());
                list.add(reply);
                //block the internal queue
                numMessagesReceived++;
                try {
                    //Allow at least 2 messages through before blocking
                    //because the server only sends flow credit after it gets 2 messages.
                    if(numMessagesReceived == 2) {
                        blockingLatch.await();
                    }
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

    static class LockedService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
        public int numMessagesReceived = 0;
        //Use a lock and condition instead of a java object with wait/notify because only a lock supports
        //testing whether a timeout fails
        public final Lock lock = new ReentrantLock();
        public final Condition unblock = lock.newCondition();

        private final int blockTimeoutMillis;

        public Exception exception = null;

        LockedService(int blockTimeoutMillis) {
            this.blockTimeoutMillis = blockTimeoutMillis;
        }

        public void unblock(){
            try{
                lock.lock();
                log.debug("Notifying unblock");
                unblock.signal();
            } finally {
                lock.unlock();
            }

        }

        @Override
        public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {
            return new StreamObserver<HelloRequest>() {
                @Override
                public void onNext(HelloRequest request) {
                    numMessagesReceived++;
                    //The first batch will be 2 messages because that's the initial credit for a client stream
                    //Further batches will be 10 messages because that's what the test will set the server
                    //credit size to be
                    if(numMessagesReceived == 2 || ((numMessagesReceived > 2) && (numMessagesReceived - 2) % 10  == 0)) {
                        //block and wait for a new batch of messages to be sent
                        try {
                            lock.lock();
                            //Block the sender
                            log.debug("Waiting for unblock");
                            if(!unblock.await(blockTimeoutMillis, TimeUnit.MILLISECONDS)){
                                log.warn("unblock timed out");
                            }
                        } catch (InterruptedException e) {
                            log.error("interrupted waiting for unblock");
                            exception = e;
                        } finally {
                            lock.unlock();
                        }
                    }
                    log.debug("server received: " + request.getName());
                }
                @Override
                public void onError(Throwable throwable) {
                    log.error("server onError()", throwable);
                }
                @Override
                public void onCompleted() {
                    responseObserver.onNext(HelloReply.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };
        }
    }

    public static void testClientStreamFlowControl(MessageServer server, Channel channel, Status expectedStatus) throws Exception {


        final LockedService blockedService = new LockedService(50);
        server.addService(blockedService);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        StatusObserver clientStatusObserver = new StatusObserver("client");
        final StreamObserver client = stub.lotsOfGreetings(clientStatusObserver);

        //Run the client stream on a thread because client.onNext() will block when it runs out of credit
        //after 2 or 10 messages
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 25; i++) {
                    HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
                    client.onNext(request);
                    log.debug("Thread sent message: " + i);
                }
                client.onCompleted();
            }
        });
        t.start();

        final Status status = clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS);

        checkStatus(expectedStatus, status);

        if(status.isOk()) {
            assertNull(blockedService.exception);
            assertEquals(25, blockedService.numMessagesReceived);
        }
    }


    public static void testClientStreamOnReady(MessageServer server, Channel channel) throws Exception {

        /*
        This test must be run with a MessageServer that has a credit size of 10

        The server blocks until client sends all the messages it has credit for.
        At this point the isReady() on the client side should be false and the client isReadyHandler should finish.
        The client then notifies the server that it has sent all it's messages and the server processes them
         */

        final LockedService lockedService = new LockedService(10000);
        server.addService(lockedService);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);

        final List<HelloRequest> requests = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
            requests.add(request);
        }

        final Iterator<HelloRequest> sourceData = requests.iterator();
        final int[] numMessagesSent = {0, 0, 0, 0};

        final CountDownLatch finishLatch = new CountDownLatch(1);

        //This weird construct is how gRPC java does manual flow control
        ClientResponseObserver<HelloRequest, HelloReply> clientResponseObserver =
                new ClientResponseObserver<HelloRequest, HelloReply>() {
                    int timesOnReadyHandlerCalled = 0;
                    @Override
                    public void beforeStart(ClientCallStreamObserver<HelloRequest> requestObserver) {
                        requestObserver.setOnReadyHandler(() -> {
                            int numMessages = 0;
                            while (requestObserver.isReady() && sourceData.hasNext()) {
                                requestObserver.onNext(sourceData.next());
                                numMessages++;
                            }
                            //The while loop above will finish when isReady() is false which occurs when the client
                            //has run out of credit.
                            //At this point signal the server to unblock.
                            //The server will then process its existing batch of messages before sending more credit.
                            //At that point this onReadyHandler() should get called again.
                            lockedService.unblock();
                            numMessagesSent[timesOnReadyHandlerCalled++] = numMessages;
                            if (!sourceData.hasNext()) {
                                requestObserver.onCompleted();
                            }
                        });
                    }

                    @Override
                    public void onNext(HelloReply value) {
                        log.debug("Server sent: " + value.getMessage());
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("", t);
                        finishLatch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        log.debug("Done.");
                        finishLatch.countDown();
                    }
                };


        ClientCallStreamObserver<HelloRequest> requestObserver =
                (ClientCallStreamObserver<HelloRequest>) stub.lotsOfGreetings(clientResponseObserver);


        assertTrue(finishLatch.await(20, TimeUnit.SECONDS));
        assertEquals(25, lockedService.numMessagesReceived);
        assertNull(lockedService.exception);
        //The on ready handler should get called 4 times.
        //First time is the default 2 messages credit for a client
        assertEquals(2, numMessagesSent[0]);
        assertNull(lockedService.exception);
        //Second time is 10 messages because server sends credit of 10
        assertEquals(10, numMessagesSent[1]);
        assertNull(lockedService.exception);
        //Third time is 10 messages because server sends credit of 10
        assertEquals(10, numMessagesSent[2]);
        assertNull(lockedService.exception);
        //Last time server sends credit of 10 but there are only 3 messages left to send.
        assertEquals(3, numMessagesSent[3]);
        assertNull(lockedService.exception);

    }



    private static void checkStatus(Status expected, Status actual){
        assertEquals(expected.getCode(), actual.getCode());
    }
}
