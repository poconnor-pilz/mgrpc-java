package com.pilz.errors;

import com.google.protobuf.MessageLite;
import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import io.grpc.Status;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOrderAndDuplicates {


    private static final Logger log = LoggerFactory.getLogger(TestOrderAndDuplicates.class);

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;


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


    class Accumulator extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
        public final ArrayList<HelloRequest> requests = new ArrayList<>();
        public final CountDownLatch latch = new CountDownLatch(1);
        @Override
        public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> singleResponse) {
            return new NoopStreamObserver<HelloRequest>() {
                @Override
                public void onNext(HelloRequest value) {
                    log.debug("Received " + value.getName());
                    requests.add(value);
                }
                @Override
                public void onCompleted() {
                    log.debug("onCompleted()");
                    latch.countDown();
                }
            };
        }
    }

    @Test
    public void testOutOfOrderClientStream() throws Exception{


        final Accumulator accumulator = new Accumulator();
        MqttServer server = new MqttServer(serverMqtt, DEVICE);
        server.init();
        server.addService(accumulator);

        String fullMethodName = "helloworld.ExampleHelloService/LotsOfGreetings";
        String callId = Base64Uuid.id();
        String topic = Topics.methodIn(DEVICE, fullMethodName);
        String replyTo = Topics.replyTo(DEVICE, fullMethodName, callId);
        publishAndPause(clientMqtt, topic, makeStartRequest(callId, 1, replyTo));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 5));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 2));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 3));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 4));
        publishAndPause(clientMqtt, topic, makeStatus(callId, 6, Status.OK));
        accumulator.latch.await();
        assertEquals(5, accumulator.requests.size());
        int seq = 0;
        for (HelloRequest request: accumulator.requests) {
            int current = Integer.parseInt(request.getName());
            if( current - seq != 1){
                assertTrue(false);
            }
            seq = current;
        }
        //Note cannot call server.getStats() and check for leaks here as there is no
        //listener/implementation of the service that will call close on it so it will remain in memory.
        server.close();
    }




    public void publishAndPause(MqttAsyncClient client, String topic, MessageLite messageLite) throws Exception{
        clientMqtt.publish(topic, new MqttMessage(messageLite.toByteArray()));
        //Introduce slight pause between messages to simulate a real system
        //If we don't do this then the thread pool that processes the messages won't get activated
        //until after all the messages are received by which time they are automatically ordered by the queue
        //so we don't get to test the out of order condition.
        Thread.sleep(50);
    }

    @Test
    public void testOutOfOrderClientStreamWithDuplicates() throws Exception{


        final Accumulator accumulator = new Accumulator();
        MqttServer server = new MqttServer(serverMqtt, DEVICE);
        server.init();
        server.addService(accumulator);

        String fullMethodName = "helloworld.ExampleHelloService/LotsOfGreetings";
        String callId = Base64Uuid.id();
        String topic = Topics.methodIn(DEVICE, fullMethodName);
        String replyTo = Topics.replyTo(DEVICE, fullMethodName, callId);
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 5));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 5));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 2));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 3));
        publishAndPause(clientMqtt, topic, makeStartRequest(callId, 1, replyTo));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 2));
        publishAndPause(clientMqtt, topic, makeStatus(callId, 6, Status.OK));
        publishAndPause(clientMqtt, topic, makeValueRequest(callId, 4));

        accumulator.latch.await();
        assertEquals(5, accumulator.requests.size());
        int seq = 0;
        for (HelloRequest request: accumulator.requests) {
            int current = Integer.parseInt(request.getName());
            if( current - seq != 1){
                assertTrue(false);
            }
            seq = current;
        }
        //Note cannot call server.getStats() and check for leaks here as there is no
        //listener/implementation of the service that will call close on it so it will remain in memory.
        server.close();
    }

    @Test
    public void testOutOfOrderServerStreamWithDuplicates() throws Exception {

        String allServicesIn = Topics.allServicesIn(DEVICE);
        log.debug("subscribe server at: " + allServicesIn);

        serverMqtt.subscribe(allServicesIn, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
            final RpcMessage message = RpcMessage.parseFrom(mqttMessage.getPayload());
            log.debug("Received {} with sequence {} message on : {}", new Object[]{message.getMessageCase(), message.getSequence(), topic});
            final String callId = message.getCallId();
            final String replyTo = message.getStart().getHeader().getReplyTo();
            publishAndPause(serverMqtt, replyTo, makeValueResponse(callId, 5));
            publishAndPause(serverMqtt, replyTo, makeValueResponse(callId, 5));
            publishAndPause(serverMqtt, replyTo, makeValueResponse(callId, 2));
            publishAndPause(serverMqtt, replyTo, makeValueResponse(callId, 3));
            publishAndPause(serverMqtt, replyTo, makeValueResponse(callId, 1));
            publishAndPause(serverMqtt, replyTo, makeValueResponse(callId, 2));
            publishAndPause(serverMqtt, replyTo, makeStatus(callId, 6, Status.OK));
            publishAndPause(serverMqtt, replyTo, makeValueResponse(callId, 4));
        })).waitForCompletion(20000);

        MqttChannel channel = new MqttChannel(clientMqtt, DEVICE);
        channel.init();

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest request = HelloRequest.newBuilder().setName("test").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        stub.lotsOfReplies(request, waiter);
        List<HelloReply> responseList = waiter.getList();
        assertEquals(responseList.size(), 5);

        int seq = 0;
        for (HelloReply reply: responseList) {
            int current = Integer.parseInt(reply.getMessage());
            if( current - seq != 1){
                assertTrue(false);
            }
            seq = current;
        }

        serverMqtt.unsubscribe(allServicesIn);
        assertEquals(0, channel.getStats().getActiveCalls());
        channel.close();

    }

    public RpcMessage makeStartRequest(String callId, int sequence, String replyTo){
        HelloRequest request = HelloRequest.newBuilder().setName(""+sequence).build();
        return makeStart(callId, sequence, replyTo, request);
    }
    public RpcMessage makeValueRequest(String callId, int sequence){
        HelloRequest request = HelloRequest.newBuilder().setName(""+sequence).build();
        return makeValue(callId, sequence,  request);
    }
    public RpcMessage makeValueResponse(String callId, int sequence){
        HelloReply reply = HelloReply.newBuilder().setMessage(""+sequence).build();
        return makeValue(callId, sequence,  reply);
    }
    
    public RpcMessage makeStart(String callId, int sequence, String replyTo, MessageLite payload){
        Header header = Header.newBuilder()
                .setReplyTo(replyTo)
                .build();
        Value value = Value.newBuilder().setContents(payload.toByteString()).build();
        Start start = Start.newBuilder()
                .setHeader(header)
                .setValue(value).build();
        RpcMessage rpcMessage = RpcMessage.newBuilder()
                .setCallId(callId)
                .setSequence(sequence)
                .setStart(start).build();
        return rpcMessage;
    }

    public RpcMessage makeValue(String callId, int sequence, MessageLite payload){
        Value value = Value.newBuilder().setContents(payload.toByteString()).build();
        RpcMessage rpcMessage = RpcMessage.newBuilder()
                .setCallId(callId)
                .setSequence(sequence)
                .setValue(value).build();
        return rpcMessage;
    }

    public RpcMessage makeStatus(String callId, int sequence, Status status){
        final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        RpcMessage rpcMessage = RpcMessage.newBuilder()
                .setCallId(callId)
                .setSequence(sequence)
                .setStatus(grpcStatus).build();
        return rpcMessage;
    }

}
