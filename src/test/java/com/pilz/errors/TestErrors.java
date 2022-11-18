package com.pilz.errors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.pilz.examples.hello.HelloServiceForTest;
import com.pilz.mqttgrpc.MqttChannel;
import com.pilz.mqttgrpc.MqttServer;
import com.pilz.mqttgrpc.StreamWaiter;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.ErrorsServiceGrpc;
import io.grpc.examples.helloworld.HelloCustomError;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestErrors {

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private MqttChannel channel;
    private MqttServer server;



    private static final String DEVICE = "device";
    private static final String SERVICE_NAME = "errorsservice";

    private static final long REQUEST_TIMEOUT = 2000;


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
    void setup() throws Exception{

        //Set up the server
        server = new MqttServer(serverMqtt, DEVICE);
        server.init();
        server.addService(new HelloServiceForTest());
        channel = new MqttChannel(clientMqtt, DEVICE);
        channel.init();
    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
    }


    @Test
    public void testSingleResponseWithError() throws InterruptedException {

        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.singleResponseWithError(joe, new StreamObserver<HelloReply>() {
            @Override
            public void onNext(HelloReply value) {}

            @Override
            public void onError(Throwable t) {
               ex[0] = t;
               latch.countDown();
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException)ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testSingleResponseWithBlockingStub() throws InterruptedException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.singleResponseWithError(joe));
        Status status = statusRuntimeException.getStatus();
        assertEquals(Status.Code.OUT_OF_RANGE, status.getCode());
        assertEquals("the value is out of range", status.getDescription());
    }


    @Test
    public void testMultiResponseWithError() throws InterruptedException {

        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.multiResponseWithError(joe, new StreamObserver<HelloReply>() {
            @Override
            public void onNext(HelloReply value) {}

            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException)ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testMultiResponseWithErrorIterator() throws InterruptedException {

        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Iterator<HelloReply> iter = stub.multiResponseWithError(joe);
        assertEquals("Hello joe", iter.next().getMessage());
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> iter.next());

        Status status = statusRuntimeException.getStatus();
        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }
    @Test
    public void testErrorInClientStream(){
        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>(REQUEST_TIMEOUT);
        StreamObserver<HelloRequest> clientStreamObserver = stub.errorInClientStream(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onError(Status.OUT_OF_RANGE.withDescription("some description").asRuntimeException());
        final HelloReply reply = waiter.getSingle();
        assertEquals("ok", reply.getMessage());
    }


    @Test
    public void testSingleResponseWithRichError() throws InterruptedException, InvalidProtocolBufferException {

        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.singleResponseWithRichError(joe));
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
    public void testSingleResponseWithRichCustomError() throws InterruptedException, InvalidProtocolBufferException {

        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.singleResponseWithRichCustomError(joe));
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

    @Test
    public void testRichErrorInClientStream(){

        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>(REQUEST_TIMEOUT);
        StreamObserver<HelloRequest> clientStreamObserver = stub.richErrorInClientStream(waiter);

        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.OUT_OF_RANGE.getNumber())
                .setMessage("the value is out of range")
                .addDetails(Any.pack(ErrorInfo.newBuilder()
                        .setReason("test failed")
                        .setDomain("com.pilz.errors")
                        .putMetadata("somekey", "somevalue")
                        .build()))
                .build();
        clientStreamObserver.onError(StatusProto.toStatusRuntimeException(status));

        final HelloReply reply = waiter.getSingle();
        assertEquals("ok", reply.getMessage());
    }




}
