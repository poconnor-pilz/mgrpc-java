package io.mgrpc.jms;

import io.grpc.Status;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.EmbeddedBroker;
import io.mgrpc.Id;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
import io.mgrpc.utils.StatusObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJmsFlowControl {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Connection serverConnection;
    private static Connection clientConnection;

    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        InitialContext initialContext = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

        serverConnection = cf.createConnection();
        serverConnection.start();
        clientConnection = cf.createConnection();
        clientConnection.start();
    }

    @AfterAll
    public static void stopClients() throws JMSException {
        serverConnection.close();
        clientConnection.close();
    }


    @Test
    public void testServerQueueCapacityExceeded() throws Exception {

        //Verify that when broker flow control is not set then it is possible to make the internal server
        //buffer/queue overflow

        final String serverId = Id.shortRandom();
        //Make a server with queue size 10
        MessageServer server = new MessageServer(new JmsServerConduit(serverConnection, serverId), 10);
        server.start();

        //Set up a channel without broker flow control
        MessageChannel channel = new MessageChannel(new JmsChannelConduit(clientConnection, serverId, false));
        channel.start();


        final CountDownLatch serviceLatch = new CountDownLatch(1);

        class BlockedService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

            public String lastRequestReceived;

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
                    public void onCompleted() {

                    }
                };
            }
        }
        server.addService(new BlockedService());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        StatusObserver clientStatusObserver = new StatusObserver("client");
        final StreamObserver client = stub.lotsOfGreetings(clientStatusObserver);

        for (int i = 0; i < 15; i++) {
            HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
            client.onNext(request);
        }

        Status clientStatus = clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS);
        checkStatus(Status.RESOURCE_EXHAUSTED, clientStatus);
        assertEquals("Service queue capacity exceeded.", clientStatus.getDescription());
        serviceLatch.countDown();

        channel.close();
        server.close();
    }


    @Test
    public void testClientQueueCapacityExceeded() throws Exception{

        final String serverId = Id.shortRandom();

        //Verify that if the server sends a lot of messages to a client that is blocked and there is no
        //broker flow control then the client queue limit is reached.
        //The test code should get an error and the server should get a cancel so that it stops sending messages.
        //and the input stream to the server should get an error.

        MessageServer server = new MessageServer(new JmsServerConduit(serverConnection, serverId));
        server.start();

        //Make a channel with queue size 10 without broker flow control
        MessageChannel channel = new MessageChannel(new JmsChannelConduit(clientConnection, serverId, false), 10);
        channel.start();

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

        channel.close();
        server.close();

    }

    @Test
    public void testClientStreamFlowControl() throws Exception {

        //Make a service that blocks until the test flips a latch
        //While the service is blocked try to overlflow the internal MessageServer queue and verify
        //That it doesn't cause a problem because the broker has buffered the messages
        //Then verify that when the service is unblocked it eventually pulls all the messages from the server

        final String serverId = Id.shortRandom();
        //Make a server with queue size 10
        MessageServer server = new MessageServer(new JmsServerConduit(serverConnection, serverId), 10);
        server.start();

        //Set up a channel with broker flow control
        MessageChannel channel = new MessageChannel(new JmsChannelConduit(clientConnection, serverId, true));
        channel.start();

        final CountDownLatch serviceLatch = new CountDownLatch(1);
        class BlockedService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            public List<HelloRequest> list = new ArrayList<>();

            @Override
            public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    @Override
                    public void onNext(HelloRequest request) {
                        log.debug("Service received: " + request.getName());
                        list.add(request);
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
                    public void onCompleted() {

                    }
                };
            }
        }

        final BlockedService blockedService = new BlockedService();
        server.addService(blockedService);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        StatusObserver clientStatusObserver = new StatusObserver("client");
        final StreamObserver client = stub.lotsOfGreetings(clientStatusObserver);

        for (int i = 0; i < 15; i++) {
            HelloRequest request = HelloRequest.newBuilder().setName("request" + i).build();
            client.onNext(request);
        }

        assertEquals(1, blockedService.list.size());

        client.onCompleted();

        //Allow some time to make sure messages got to broker
        Thread.sleep(100);

        log.debug("Unblocking client stream");

        //unblock the service
        serviceLatch.countDown();

        checkStatus(Status.OK, clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS));
        assertEquals(15, blockedService.list.size());

        channel.close();
        server.close();
    }


    @Test
    public void testServerStreamFlowControl() throws Exception {

        //Make a service that blocks until the test flips a latch
        //While the service is blocked try to overlflow the internal MessageServer queue and verify
        //That it doesn't cause a problem because the broker has buffered the messages
        //Then verify that when the service is unblocked it eventually pulls all the messages from the server

        final String serverId = Id.shortRandom();
        //Make a server with queue size 10
        MessageServer server = new MessageServer(new JmsServerConduit(serverConnection, serverId), 10);
        server.start();

        //Set up a channel with broker flow control
        MessageChannel channel = new MessageChannel(new JmsChannelConduit(clientConnection, serverId, true));
        channel.start();

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

        channel.close();
        server.close();
    }


    private void checkStatus(Status expected, Status actual){
        assertEquals(expected.getCode(), actual.getCode());
    }
}
