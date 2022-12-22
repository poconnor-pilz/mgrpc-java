package com.pilz.errors;

import com.pilz.examples.hello.HelloServiceForTest;
import com.pilz.mqttgrpc.MqttChannel;
import com.pilz.mqttgrpc.MqttServer;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        private final CountDownLatch latch;

        CancelableObserver(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver reqStream) {
            requestStream = reqStream;
        }
        public void cancel(String message){
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
    public void testCancel() throws InterruptedException {

        final CountDownLatch serverCancelledLatch = new CountDownLatch(1);

        class CancelDuringServerStreaming extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                final boolean[] cancelled = {false};
                //Note that if we do not set this an exception will be thrown on cancel that is handled by the
                //MgMessagHandler.handleMessage and the service will terminate anyway.
                ServerCallStreamObserver<HelloReply> serverObserver = (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverObserver.setOnCancelHandler(()->{
                    log.debug("ServerCallStreamObserver cancel handler called");
                    serverCancelledLatch.countDown();
                    log.debug("Latch toggled");
                    cancelled[0] = true;
                });
                while(!cancelled[0]){
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if(!cancelled[0]) {
                        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
                        responseObserver.onNext(reply);
                    }
                }
            }
        }
        server.addService(new CancelDuringServerStreaming());
        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        CountDownLatch clientCancelledLatch = new CountDownLatch(1);
        final CancelableObserver cancelableObserver = new CancelableObserver(clientCancelledLatch);
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.debug("Sending cancel");
                cancelableObserver.cancel("tryit");
            }
        });


        stub.lotsOfReplies(joe, cancelableObserver);

        log.debug("waiting");

        //Verify that on the client side the CancelableObserver.onError gets called
        //This will test the cancel functionality in MqttChannel
        assert(clientCancelledLatch.await(5, TimeUnit.SECONDS));

        //Verify that on the server side the errors service cancel handler gets called
        //This will test the cancel functionality in MqttServer
        assert(serverCancelledLatch.await(5, TimeUnit.SECONDS));

    }


    @Test
    public void testTimeoutOnClient() throws Exception {

        class SingleResponseWithDelay extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        server.addService(new SingleResponseWithDelay());
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc
                .newBlockingStub(channel).withDeadlineAfter(100, TimeUnit.MILLISECONDS);;
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.sayHello(joe));
        Status status = statusRuntimeException.getStatus();
        assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getCode());
    }





}
