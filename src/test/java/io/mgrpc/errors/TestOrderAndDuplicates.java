package io.mgrpc.errors;

import io.grpc.Status;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelTransport;
import io.mgrpc.mqtt.MqttExceptionLogger;
import io.mgrpc.mqtt.MqttServerTransport;
import io.mgrpc.mqtt.MqttUtils;
import io.mgrpc.utils.TestRpcMessageBuilder;
import io.mgrpc.utils.ToList;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOrderAndDuplicates {


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();


    @BeforeAll
    public static void startClients() throws Exception{
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClients() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
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
        MessageServer server = new MessageServer(new MqttServerTransport(serverMqtt, SERVER));
        server.start();
        server.addService(accumulator);

        String fullMethodName = "helloworld.ExampleHelloService/LotsOfGreetings";
        String callId = Id.random();
        String channelId = Id.random();
        ServerTopics serverTopics = new ServerTopics(SERVER);
        String topic = serverTopics.methodIn(fullMethodName);
        log.debug(topic);
        String replyTo = serverTopics.replyTopic(channelId, fullMethodName);
        log.debug(replyTo);
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeStartRequest(fullMethodName, Start.MethodType.CLIENT_STREAMING, callId, 1, replyTo));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 5));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 2));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 3));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 4));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeStatus(callId, 6, Status.OK));
        accumulator.latch.await();
        assertEquals(4, accumulator.requests.size());
        int seq = 1;
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




    public void publishAndPause(MqttAsyncClient client, String topic, RpcMessage rpcMessage) throws Exception{

        final RpcSet.Builder batchBuilder = RpcSet.newBuilder();
        batchBuilder.addMessages(rpcMessage);
        clientMqtt.publish(topic, new MqttMessage(batchBuilder.build().toByteArray()));
        //Introduce slight pause between messages to simulate a real system
        //If we don't do this then the thread pool that processes the messages won't get activated
        //until after all the messages are received by which time they are automatically ordered by the queue
        //so we don't get to test the out of order condition.
        Thread.sleep(50);
    }



    @Test
    public void testOutOfOrderClientStreamWithDuplicates() throws Exception{

        //Send out of order requests to a service topic
        //Then verify that the MqttServer puts re-orders the requests correctly.

        final Accumulator accumulator = new Accumulator();
        MessageServer server = new MessageServer(new MqttServerTransport(serverMqtt, SERVER));
        server.start();
        server.addService(accumulator);

        String fullMethodName = "helloworld.ExampleHelloService/LotsOfGreetings";
        String callId = Id.random();
        String channelId = Id.random();
        ServerTopics serverTopics = new ServerTopics(SERVER);
        String topic = serverTopics.methodIn(fullMethodName);
        String replyTo = serverTopics.replyTopic(channelId, fullMethodName);
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 5));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 5));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 2));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 3));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeStartRequest(fullMethodName, Start.MethodType.CLIENT_STREAMING, callId, 1, replyTo));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 2));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeStatus(callId, 6, Status.OK));
        publishAndPause(clientMqtt, topic, TestRpcMessageBuilder.makeValueRequest(callId, 4));

        accumulator.latch.await();
        assertEquals(4, accumulator.requests.size());
        int seq = 1;
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

        //Make a mock server that sends back replies out of order when it gets a request
        //Then verify that the MqttChannel will re-order the replies correctly

        String servicesInFilter = new ServerTopics(SERVER).servicesIn + "/#";
        log.debug("subscribe server at: " + servicesInFilter);

        final String channelId = Id.random();

        serverMqtt.subscribe(servicesInFilter, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
            final RpcSet rpcSet = RpcSet.parseFrom(mqttMessage.getPayload());
            final RpcMessage message = rpcSet.getMessages(0);
            log.debug("Received {} with sequence {} message on : {}", new Object[]{message.getMessageCase(), message.getSequence(), topic});
            final String callId = message.getCallId();
            String methodName = "helloworld.ExampleHelloService/LotsOfReplies";
            final String replyTo = new ServerTopics(SERVER).replyTopic(channelId, methodName);
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeValueResponse(callId, 5));
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeValueResponse(callId, 5));
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeValueResponse(callId, 2));
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeValueResponse(callId, 3));
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeValueResponse(callId, 1));
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeValueResponse(callId, 2));
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeStatus(callId, 6, Status.OK));
            publishAndPause(serverMqtt, replyTo, TestRpcMessageBuilder.makeValueResponse(callId, 4));
        }));

        final MqttChannelTransport mqttChannelMessageProvider = new MqttChannelTransport(clientMqtt, SERVER);
        mqttChannelMessageProvider.fakeServerConnectedForTests();
        MessageChannel channel = new MessageChannel(mqttChannelMessageProvider, channelId);
        channel.start();


        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest request = HelloRequest.newBuilder().setName("test").build();
        List<HelloReply> responseList = ToList.toList(stub.lotsOfReplies(request));
        assertEquals(responseList.size(), 5);

        int seq = 0;
        for (HelloReply reply: responseList) {
            int current = Integer.parseInt(reply.getMessage());
            if( current - seq != 1){
                assertTrue(false);
            }
            seq = current;
        }

        serverMqtt.unsubscribe(servicesInFilter);
        assertEquals(0, channel.getStats().getActiveCalls());
        channel.close();

    }

}
