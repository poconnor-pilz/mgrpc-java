package com.pilz.errors;

import com.pilz.mqttgrpc.MqttChannel;
import com.pilz.mqttgrpc.MqttServer;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCancelAndTimeout {


    private static final Logger log = LoggerFactory.getLogger(TestCancelAndTimeout.class);


    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private MqttChannel channel;
    private MqttServer server;


    private static final String DEVICE = "device";


    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();
        serverMqtt = MqttUtils.makeClient(Topics.systemStatus(DEVICE));
        clientMqtt = MqttUtils.makeClient(null);
    }

    @AfterAll
    public static void stopClientsAndBroker() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
        MqttUtils.stopEmbeddedBroker();
    }


    @BeforeEach
    void setup() throws Exception {

        //Set up the server
        server = new MqttServer(serverMqtt, DEVICE);
        server.init();
        channel = new MqttChannel(clientMqtt, DEVICE);
        channel.init();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.close();
    }

    class CancelableObserver implements ClientResponseObserver<HelloRequest, HelloReply> {
        private ClientCallStreamObserver requestStream;
        public final CountDownLatch latch = new CountDownLatch(1);
        public StatusRuntimeException exception = null;


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
            this.exception = (StatusRuntimeException) t;
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            log.debug("CancelableObserver onCompleted()");
        }

    }

    class ErrorObserver implements StreamObserver {

        public final CountDownLatch latch = new CountDownLatch(1);
        StatusRuntimeException exception = null;

        public Status.Code getStatusCode() {
            return exception.getStatus().getCode();
        }

        @Override
        public void onNext(Object o) {

        }

        @Override
        public void onError(Throwable throwable) {
            exception = (StatusRuntimeException) throwable;
            latch.countDown();
        }

        @Override
        public void onCompleted() {

        }
    }



    @Test
    void testCancel() throws Exception {


        //The log from this test shows that on timeout
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            public ErrorObserver errorObserver = new ErrorObserver();

            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {
                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(() -> {
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                });
                return this.errorObserver;
            }
        }

        final ListenForTimeout listenForTimeout = new ListenForTimeout();
        server.addService(listenForTimeout);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        final CancelableObserver cancelableObserver = new CancelableObserver();
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

        //The server cancel handler should get called
        assertTrue(serverCancelledLatch.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertTrue(listenForTimeout.errorObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(listenForTimeout.errorObserver.getStatusCode(), Status.CANCELLED.getCode());

        //Verify that on the client side the CancelableObserver.onError gets called with CANCEL
        assertTrue(cancelableObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(cancelableObserver.exception.getStatus().getCode(), Status.CANCELLED.getCode());
    }


    @Test
    public void testTimeout() throws Exception {

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);
        class ListenForTimeout extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            public ErrorObserver errorObserver = new ErrorObserver();

            @Override
            public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> responseObserver) {
                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(() -> {
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                });
                return this.errorObserver;
            }
        }

        final ListenForTimeout listenForTimeout = new ListenForTimeout();
        server.addService(listenForTimeout);
        ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc
                .newStub(channel).withDeadlineAfter(500, TimeUnit.MILLISECONDS);
        HelloRequest request = HelloRequest.newBuilder().setName("joe").build();
        final ErrorObserver clientErrorObserver = new ErrorObserver();
        final StreamObserver<HelloRequest> inStream = stub.bidiHello(clientErrorObserver);
        inStream.onNext(request);

        //The server cancel handler should get called
        assertTrue(serverCancelledLatch.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertTrue(listenForTimeout.errorObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(listenForTimeout.errorObserver.getStatusCode(), Status.CANCELLED.getCode());

        //The client should receive an onError() DEADLINE_EXCEEDED
        assertTrue(clientErrorObserver.latch.await(5, TimeUnit.SECONDS));
        assertEquals(clientErrorObserver.getStatusCode(), Status.DEADLINE_EXCEEDED.getCode());
    }

}
