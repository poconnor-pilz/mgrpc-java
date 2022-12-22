package com.pilz.errors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.pilz.mqttgrpc.MqttChannel;
import com.pilz.mqttgrpc.MqttServer;
import com.pilz.mqttgrpc.StreamWaiter;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloCustomError;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestErrors {

    private static final Logger log = LoggerFactory.getLogger(TestErrors.class);

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


    @Test
    public void testSingleResponseWithError() throws InterruptedException {
        class SingleResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new SingleResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.sayHello(joe, new StreamObserver<HelloReply>() {
            @Override
            public void onNext(HelloReply value) {
            }

            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }

            @Override
            public void onCompleted() {
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException) ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testSingleResponseWithBlockingStub() throws InterruptedException {

        class SingleResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new SingleResponseWithError());
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> stub.sayHello(joe));
        Status status = statusRuntimeException.getStatus();
        assertEquals(Status.Code.OUT_OF_RANGE, status.getCode());
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testMultiResponseWithError() throws InterruptedException {

        class MultiResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
                responseObserver.onNext(reply);
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new MultiResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.lotsOfReplies(joe, new StreamObserver<HelloReply>() {
            @Override
            public void onNext(HelloReply value) {
            }

            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }

            @Override
            public void onCompleted() {
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException) ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testMultiResponseWithErrorIterator() {

        class MultiResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
                responseObserver.onNext(reply);
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new MultiResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Iterator<HelloReply> iter = stub.lotsOfReplies(joe);
        assertEquals("Hello joe", iter.next().getMessage());
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> iter.next());

        Status status = statusRuntimeException.getStatus();
        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testSingleResponseWithRichError() throws InvalidProtocolBufferException {

        class SingleResponseWithRichError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                        .setCode(Code.OUT_OF_RANGE.getNumber())
                        .setMessage("the value is out of range")
                        .addDetails(Any.pack(ErrorInfo.newBuilder()
                                .setReason("test failed")
                                .setDomain("com.pilz.errors")
                                .putMetadata("somekey", "somevalue")
                                .build()))
                        .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            }
        }
        server.addService(new SingleResponseWithRichError());
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> stub.sayHello(joe));
        Status status = statusRuntimeException.getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());

        //The io.grpc.status above is exactly the same as when in testSingleResponseWithError
        //To get the rich error we convert to com.google.rpc.status
        //The details are in an google.rpc.ErrorInfo
        //Usually to represent details it will be enough  to use one of the protocol buffers in
        //https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
        //described also at https://cloud.google.com/apis/design/errors
        //Note that the line below could also use StatusProto.fromThrowable(statusRuntimeException)
        com.google.rpc.Status rpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        assertEquals(Code.OUT_OF_RANGE, Code.forNumber(rpcStatus.getCode()));
        assertEquals("the value is out of range", rpcStatus.getMessage());
        for (Any any : rpcStatus.getDetailsList()) {
            if (any.is(ErrorInfo.class)) {
                ErrorInfo errorInfo = any.unpack(ErrorInfo.class);
                assertEquals("test failed", errorInfo.getReason());
                assertEquals("com.pilz.errors", errorInfo.getDomain());
                assertEquals("somevalue", errorInfo.getMetadataMap().get("somekey"));
            }
        }
    }


    @Test
    public void testSingleResponseWithRichCustomError() throws InvalidProtocolBufferException {

        class SingleResponseWithRichCustomError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                //Put in a HelloCustomError defined in helloworld.proto
                com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                        .setCode(Code.OUT_OF_RANGE.getNumber())
                        .setMessage("the value is out of range")
                        .addDetails(Any.pack(HelloCustomError.newBuilder()
                                .setHelloErrorCode(20)
                                .setHelloErrorDescription("an error description")
                                .build()))
                        .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            }
        }
        server.addService(new SingleResponseWithRichCustomError());
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> stub.sayHello(joe));
        Status status = statusRuntimeException.getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());

        //The io.grpc.status above is exactly the same as when in testSingleResponseWithError
        //To get the rich error we convert to com.google.rpc.status
        //Usually to represent details it will be enough  to use one of the protocol buffers in
        //https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
        //described also at https://cloud.google.com/apis/design/errors
        //But in this case the details are in a HelloCustomError
        com.google.rpc.Status rpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        assertEquals(Code.OUT_OF_RANGE, Code.forNumber(rpcStatus.getCode()));
        assertEquals("the value is out of range", rpcStatus.getMessage());
        for (Any any : rpcStatus.getDetailsList()) {
            if (any.is(HelloCustomError.class)) {
                HelloCustomError helloCustomError = any.unpack(HelloCustomError.class);
                assertEquals(20, helloCustomError.getHelloErrorCode());
                assertEquals("an error description", helloCustomError.getHelloErrorDescription());
            }
        }
    }



}


