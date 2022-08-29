package com.pilz.errors;

import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.ProtoServiceManager;
import com.pilz.mqttgrpc.StreamIterator;
import com.pilz.mqttgrpc.StreamWaiter;
import com.pilz.utils.MqttUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
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


    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();

        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
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

        String serviceBaseTopic = "errorsservice";

        //Set up the server
        ProtoServiceManager protoServiceManager = new ProtoServiceManager(serverMqtt);
        service = new ErrorsService();
        ErrorsSkeleton skeleton = new ErrorsSkeleton(service);
        protoServiceManager.subscribeService(serviceBaseTopic, skeleton);

        //Setup the client stub
        ProtoSender sender = new ProtoSender(clientMqtt, serviceBaseTopic);
        stub = new ErrorsStub(sender);
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


}
