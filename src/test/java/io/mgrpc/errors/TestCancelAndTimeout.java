package io.mgrpc.errors;

import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelConduitManager;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import io.mgrpc.utils.StatusObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCancelAndTimeout {


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());



    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private Channel channel;
    private MessageChannel messageChannel;
    private MessageServer server;


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();


    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClients() throws Exception {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
    }


    @BeforeEach
    void setup() throws Exception {

        //Set up the server
        server = new MessageServer(new MqttServerConduit(serverMqtt, SERVER));
        server.start();
        messageChannel = new MessageChannel(new MqttChannelConduitManager(clientMqtt));
        messageChannel.start();
        channel = ClientInterceptors.intercept(messageChannel, new TopicInterceptor(SERVER));
    }

    @AfterEach
    void tearDown() throws Exception {
        server.close();
    }

    public void checkForLeaks(int numActiveCalls){
        try {
            //Give the channel and server time to process messages and release resources
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(numActiveCalls, messageChannel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, server.getStats().getActiveCalls());
    }


    @Test
    void testCancel() throws Exception {
        //Verify that when we send a cancel then
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //(The above two are done by the grpc in the code for StreamingServerCallListener.onCancel())
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit


        final ListenForCancel listenForCancel = new ListenForCancel();
        server.addService(listenForCancel);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final CancelableObserver cancelableObserver = new CancelableObserver();

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(cancelableObserver);
        inStream.onNext(joe);

        //Wait for at least one message to go through before canceling to make sure the call is fully started.
        listenForCancel.statusObserver.waitForNext(5, TimeUnit.SECONDS);
        log.debug("Sending cancel");
        cancelableObserver.cancel("tryit");

        log.debug("waiting");

        //The server cancel handler should get called
        assertTrue(listenForCancel.serverCancelHandlerCalled.await(5, TimeUnit.SECONDS));

        //Verify that the Context.CancellationListener gets called
        assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertEquals(listenForCancel.statusObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        //Verify that on the client side the CancelableObserver.onError gets called with CANCEL
        assertTrue(cancelableObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(cancelableObserver.exception.getStatus().getCode(), Status.CANCELLED.getCode());

        checkForLeaks(0);
    }

    @Test
    void testCancelUsingContext() throws Exception {
        //Verify that when we send a cancel then
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        final ListenForCancel listenForCancel = new ListenForCancel();
        server.addService(listenForCancel);
        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        final Context.CancellableContext withCancellation = Context.current().withCancellation();
        final StatusObserver clientStatusObserver = new StatusObserver("client");
        withCancellation.run(()->{
            HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
            final StreamObserver<HelloRequest> inStream = stub.bidiHello(clientStatusObserver);
            inStream.onNext(joe);
            //Wait for at least one message to go through before canceling to make sure the call is fully started.
            listenForCancel.statusObserver.waitForNext(5, TimeUnit.SECONDS);
            withCancellation.cancel(new Exception("testexc"));
        });


        log.debug("waiting");

        //The server cancel handler should get called
        assertTrue(listenForCancel.serverCancelHandlerCalled.await(5, TimeUnit.SECONDS));

        //Verify that the Context.CancellationListener gets called
        assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertEquals(listenForCancel.statusObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        //Verify that on the client side the onError gets called with CANCEL
        assertEquals(clientStatusObserver.waitForStatus(5, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        checkForLeaks(0);
    }



    @Test
    void testClientSendsErrorCausesCancel() throws Exception {

        //This is an invalid scenario. The client sends an error on an input stream. It should never have a reason
        //to do this that is not satisfied by a cancel
        //However this should behave the same as a cancel or timeout
        //The grpc code will detect the error in the requset stream and send a cancel on the client side to the client
        //test code. Then it will send a Status.CANCELLED to the server input stream (even though
        //the test code tries to send a Status.INTERNAL to it.
        //In the real grpc this is sent as RST_STREAM
        //But for us the Status.CANCELLED will work because that is what we use anyway because we have
        //high level messages

        //Verify that when we send an error then
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit


        final ListenForCancel listenForCancel = new ListenForCancel();
        server.addService(listenForCancel);
        ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest request = HelloRequest.newBuilder().setName("joe").build();
        final StatusObserver clientStatusObserver = new StatusObserver("client");
        final StreamObserver<HelloRequest> inStream = stub.bidiHello(clientStatusObserver);
        inStream.onNext(request);

        final StatusRuntimeException sre = new StatusRuntimeException(Status.INTERNAL.withDescription("atesterrror"));

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.debug("Sending cancel");
            inStream.onError(sre);
        });

        //The server cancel handler should get called
        assertTrue(listenForCancel.serverCancelHandlerCalled.await(5, TimeUnit.SECONDS));

        //Verify that the Context.CancellationListener gets called
        assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertEquals(listenForCancel.statusObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        //The client should receive an onError() DEADLINE_EXCEEDED
        assertEquals(clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());
        checkForLeaks(0);

    }



    @Test
    public void testTimeout() throws Exception {

        //Verify that on timeout
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        final ListenForCancel listenForCancel = new ListenForCancel();
        server.addService(listenForCancel);
        ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc
                .newStub(channel).withDeadlineAfter(500, TimeUnit.MILLISECONDS);
        HelloRequest request = HelloRequest.newBuilder().setName("joe").build();
        final StatusObserver clientStatusObserver = new StatusObserver("client");
        final StreamObserver<HelloRequest> inStream = stub.bidiHello(clientStatusObserver);
        inStream.onNext(request);

        //The server cancel handler should get called
        assertTrue(listenForCancel.serverCancelHandlerCalled.await(5, TimeUnit.SECONDS));

        //Verify that the Context.CancellationListener gets called
        assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertEquals(listenForCancel.statusObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        //The client should receive an onError() DEADLINE_EXCEEDED
        assertEquals(clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.DEADLINE_EXCEEDED.getCode());
        checkForLeaks(0);
    }

    @Test
    public void testServerQueueCapacityExceeded() throws Exception {

        server.close();
        //Make a server with queue size 10
        server = new MessageServer(new MqttServerConduit(serverMqtt, SERVER), 10);
        server.start();

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
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), clientStatus.getCode());
        assertEquals("Service queue capacity exceeded.", clientStatus.getDescription());
        serviceLatch.countDown();

    }


    @Test
    public void testClientQueueCapacityExceeded() throws Exception{

        //Verify that if the server sends a lot of messages to a client that is blocked
        //then the client queue limit is reached.
        //The test code should get an error and the server should get a cancel so that it stops sending messages.
        //and the input stream to the server should get an error.
        messageChannel.close();
        //Make a channel with queue size 10
        messageChannel = new MessageChannelBuilder()
                .conduitManager(new MqttChannelConduitManager(serverMqtt))
                .queueSize(10).build();
        messageChannel.start();
        channel = ClientInterceptors.intercept(messageChannel, new TopicInterceptor(SERVER));


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
        assertEquals(listenForCancel.errorObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        //The client should receive an onError() RESOURCE_EXHAUSTED
        assertEquals(clientStatusObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.RESOURCE_EXHAUSTED.getCode());
        checkForLeaks(0);
    }


}
