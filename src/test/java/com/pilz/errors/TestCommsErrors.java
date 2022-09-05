package com.pilz.errors;

import com.pilz.examples.hello.HelloService;
import com.pilz.examples.hello.HelloSkeleton;
import com.pilz.examples.hello.HelloStub;
import com.pilz.examples.hello.IHelloService;
import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestCommsErrors {

    private static Logger log = LoggerFactory.getLogger(TestCommsErrors.class);

    private static final String DEVICE = "device1";

    private static final String SERVICE_NAME = "helloservice";


    @BeforeAll
    public static void startBroker() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();

    }


    @Test
    public void testNoClientBrokerConnection() throws MqttException {
        //Setup the client stub
        String serviceBaseTopic = "helloservice";
        //Make a ProtoSender with an mqtt client that's not connected and verify that using a service with it
        //will cause UNAVAILABLE exceptions
        ProtoSender sender = new ProtoSender(new MqttAsyncClient("tcp://localhost:1884", Base64Utils.randomId()), DEVICE);
        HelloStub stub = new HelloStub(sender, SERVICE_NAME);


        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        stub.requestResponse(joe, waiter);

        StatusRuntimeException sre = assertThrows(StatusRuntimeException.class,
                () -> waiter.getSingle());

        assertEquals(sre.getStatus().getCode(), Status.Code.UNAVAILABLE);

        //Run the same test for StreamIterator
        StreamIterator<HelloReply> iter = new StreamIterator<>();
        stub.requestResponse(joe, iter);
        sre = assertThrows(StatusRuntimeException.class,
                () -> iter.next());
        assertEquals(sre.getStatus().getCode(), Status.Code.UNAVAILABLE);

    }

    @Test
    public void testNoServerBrokerConnection() throws MqttException, InterruptedException {

        String serviceBaseTopic = "helloservice";

        //Set up the server
        final MqttAsyncClient serverMqtt = MqttUtils.makeClient();
        MqttGrpcServer mqttGrpcServer = new MqttGrpcServer(serverMqtt, DEVICE);
        final HelloService service = new HelloService();
        HelloSkeleton skeleton = new HelloSkeleton(service);
        mqttGrpcServer.subscribeService(serviceBaseTopic, skeleton);


        //Setup the client stub
        final MqttAsyncClient clientMqtt = MqttUtils.makeClient();
        ProtoSender sender = new ProtoSender(clientMqtt, DEVICE);
        HelloStub stub = new HelloStub(sender, SERVICE_NAME);

        //send a message and make sure we get a response
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        stub.requestResponse(joe, waiter);
        HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe", reply.getMessage());

        //Disconnect the server broker connection, send a message and check the exception
        serverMqtt.disconnect().waitForCompletion();
        serverMqtt.close();

//        CountDownLatch latch = new CountDownLatch(1);
//        stub.requestResponse(joe, new StreamObserver<HelloReply>() {
//            @Override
//            public void onNext(HelloReply value) {
//                log.debug("next: " + value.toString());
//            }
//
//            @Override
//            public void onError(Throwable t) {
//                log.error("err", t);
//                latch.countDown();
//            }
//
//            @Override
//            public void onCompleted() {
//                log.debug("complete");
//                latch.countDown();
//            }
//        });
//
//        latch.await();

        StreamWaiter waiter2 = new StreamWaiter<>(1000);
        stub.requestResponse(joe, waiter);


        StatusRuntimeException sre = assertThrows(StatusRuntimeException.class,
                () -> waiter2.getSingle());


        //The waiter will just time out.
        //TODO: What if this was blocking stub? The protoSender needs to listen for the LWT of the server to fix this.
        assertEquals(sre.getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED);

        //Run the same test for StreamIterator
        //TODO: This will just wait forever. Again the protoSender needs to know about the LWT of the server
//        StreamIterator<HelloReply> iter = new StreamIterator<>();
//        stub.requestResponse(joe, iter);
//        sre = assertThrows(StatusRuntimeException.class,
//                () -> iter.next());
//        assertEquals(sre.getStatus().getCode(), Status.Code.UNAVAILABLE);

    }


}
