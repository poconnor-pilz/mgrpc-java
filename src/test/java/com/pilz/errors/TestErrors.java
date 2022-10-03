package com.pilz.errors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.HelloCustomError;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestErrors {

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private ErrorsService service;
    private ErrorsStub stub;

    private static final String DEVICE = "device";
    private static final String SERVICE_NAME = "errorsservice";

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
        MqttGrpcServer mqttGrpcServer = new MqttGrpcServer(serverMqtt, DEVICE);
        mqttGrpcServer.init();
        service = new ErrorsService();
        ErrorsSkeleton skeleton = new ErrorsSkeleton(service);
        mqttGrpcServer.subscribeService(SERVICE_NAME, skeleton);

        //Setup the client stub
        MqttGrpcClient mgClient = new MqttGrpcClient(clientMqtt, DEVICE);
        mgClient.init();
        stub = new ErrorsStub(mgClient, SERVICE_NAME);
    }


    @Test
    public void testSingleResponseWithError() throws InterruptedException {
        testSingleResponseWithError(service);
        testSingleResponseWithError(stub);
    }


    public void testSingleResponseWithError(IErrorsService errorsService) throws InterruptedException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        errorsService.singleResponseWithError(joe, new StreamObserver<HelloReply>() {
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
    public void testSingleResponseWithErrorStreamWaiter() throws InterruptedException {
        testSingleResponseWithErrorStreamWaiter(service);
        testSingleResponseWithErrorStreamWaiter(stub);
    }
    public void testSingleResponseWithErrorStreamWaiter(IErrorsService errorsService) throws InterruptedException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter waiter = new StreamWaiter(5000);
        errorsService.singleResponseWithError(joe, waiter);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> waiter.getSingle());


        Status status = statusRuntimeException.getStatus();
        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }


    @Test
    public void testMultiResponseWithError() throws InterruptedException {
        testMultiResponseWithError(service);
        testMultiResponseWithError(stub);
    }


    public void testMultiResponseWithError(IErrorsService errorsService) throws InterruptedException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        errorsService.multiResponseWithError(joe, new StreamObserver<HelloReply>() {
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
        testMultiResponseWithErrorIterator(service);
        testMultiResponseWithErrorIterator(stub);
    }
    public void testMultiResponseWithErrorIterator(IErrorsService errorsService) throws InterruptedException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamIterator<HelloReply> iter = new StreamIterator();
        errorsService.multiResponseWithError(joe, iter);
        assertEquals("Hello joe", iter.next().getMessage());
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> iter.next());

        Status status = statusRuntimeException.getStatus();
        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }
    @Test
    public void testErrorInClientStream(){
        testErrorInClientStream(service);
        testErrorInClientStream(stub);
    }

    public void testErrorInClientStream(IErrorsService errorsService){
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        StreamObserver<HelloRequest> clientStreamObserver = errorsService.errorInClientStream(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onError(Status.OUT_OF_RANGE.withDescription("some description").asRuntimeException());
        final HelloReply reply = waiter.getSingle();
        assertEquals("ok", reply.getMessage());
    }


    @Test
    public void testSingleResponseWithRichError() throws InterruptedException, InvalidProtocolBufferException {
        testSingleResponseWithRichError(service);
        testSingleResponseWithRichError(stub);
    }


    public void testSingleResponseWithRichError(IErrorsService errorsService) throws InterruptedException, InvalidProtocolBufferException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        StreamWaiter waiter = new StreamWaiter(5000);
        errorsService.singleResponseWithError(joe, waiter);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> waiter.getSingle());

        final Status status = statusRuntimeException.getStatus();
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
        testSingleResponseWithRichCustomError(service);
        testSingleResponseWithRichCustomError(stub);
    }


    public void testSingleResponseWithRichCustomError(IErrorsService errorsService) throws InterruptedException, InvalidProtocolBufferException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter waiter = new StreamWaiter(5000);
        errorsService.singleResponseWithError(joe, waiter);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> waiter.getSingle());

        final Status status = statusRuntimeException.getStatus();
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
        testRichErrorInClientStream(service);
        testRichErrorInClientStream(stub);
    }

    public void testRichErrorInClientStream(IErrorsService errorsService){
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        StreamObserver<HelloRequest> clientStreamObserver = errorsService.richErrorInClientStream(waiter);
        clientStreamObserver.onNext(joe);

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
