package com.pilz.examples.hello;

import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import com.pilz.utils.ToList;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestSubscription {

    private static Logger log = LoggerFactory.getLogger(TestSubscription.class);

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private MqttChannel channel;
    private MqttServer server;

    private static final String DEVICE = "device1";

    private static final long REQUEST_TIMEOUT = 2000;

    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();

        serverMqtt = MqttUtils.makeClient(Topics.systemStatus(DEVICE), "tcp://localhost:1883");
        clientMqtt = MqttUtils.makeClient(null, "tcp://localhost:1883");
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

        //Set up the serverb
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

    public void checkForLeaks(int numActiveCalls){
        assertEquals(numActiveCalls, channel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, server.getStats().getActiveCalls());
    }




    @Test
    public void testSubscription() throws InterruptedException {

        //Set up multiple subscribers to listen for responses on a service via pub sub

        class HelloObserver implements StreamObserver<HelloReply>{

            public final CountDownLatch countDownLatch;
            public final List<HelloReply> replies = new ArrayList<>();

            HelloObserver(CountDownLatch countDownLatch) {
                this.countDownLatch = countDownLatch;
            }

            @Override
            public void onNext(HelloReply helloReply) {
                replies.add(helloReply);
            }
            @Override
            public void onError(Throwable throwable) {

            }
            @Override
            public void onCompleted() {
                this.countDownLatch.countDown();
            }
        }

        final String responseTopic1 = Topics.out(DEVICE,"atesttopic");
        final String responseTopic2 = Topics.out(DEVICE,"atesttopic2");



        //Subscribe for responses
        CountDownLatch latch = new CountDownLatch(3);
        HelloObserver obs1 = new HelloObserver(latch);
        channel.subscribe(responseTopic1, HelloReply.parser(), obs1);
        HelloObserver obs2 = new HelloObserver(latch);
        channel.subscribe(responseTopic1, HelloReply.parser(), obs2);
        HelloObserver obsTemp = new HelloObserver(latch);
        channel.subscribe(responseTopic1, HelloReply.parser(), obsTemp);

        assertEquals(channel.getStats().getSubscribers(), 3);
        channel.unsubscribe(responseTopic1, obsTemp);
        assertEquals(channel.getStats().getSubscribers(), 2);

        HelloObserver obs3 = new HelloObserver(latch);
        channel.subscribe(responseTopic2, HelloReply.parser(), obs3);
        assertEquals(channel.getStats().getSubscribers(), 3);

        //Send two requests each with a different responseTopic

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub1 =
                ExampleHelloServiceGrpc.newBlockingStub(channel)
                        .withOption(MqttChannel.RESPONSE_TOPIC, responseTopic1);

        HelloRequest request1 = HelloRequest.newBuilder().setName("2").build();
        final Iterator<HelloReply> helloReplyIterator = blockingStub1.lotsOfReplies(request1);
        //The request should have no responses because all the responses are sent to the response topic
        //instead of the replyTo
        assertEquals(0, ToList.toList(helloReplyIterator).size());

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub2 =
                ExampleHelloServiceGrpc.newBlockingStub(channel)
                        .withOption(MqttChannel.RESPONSE_TOPIC, responseTopic2);
        HelloRequest request2 = HelloRequest.newBuilder().setName("3").build();
        blockingStub2.lotsOfReplies(request2);

        latch.await();

        //All subscriptions should be closed because the streams have completed
        assertEquals(channel.getStats().getSubscribers(), 0);

        assertEquals(obs1.replies.size(), 2);
        assertEquals("Hello 0", obs1.replies.get(0).getMessage());
        assertEquals("Hello 1", obs1.replies.get(1).getMessage());

        assertEquals(obs2.replies.size(), 2);
        assertEquals("Hello 0", obs2.replies.get(0).getMessage());
        assertEquals("Hello 1", obs2.replies.get(1).getMessage());

        assertEquals(obs3.replies.size(), 3);
        assertEquals("Hello 0", obs3.replies.get(0).getMessage());
        assertEquals("Hello 1", obs3.replies.get(1).getMessage());
        assertEquals("Hello 2", obs3.replies.get(2).getMessage());

        //obsTemp was unsubscribed and so should not receive any responses
        assertEquals(obsTemp.replies.size(), 0);

        checkForLeaks(0);

        //Test unsubscribe of all observers to a responseTopic
        channel.subscribe(responseTopic1, HelloReply.parser(), obs1);
        channel.subscribe(responseTopic1, HelloReply.parser(), obs2);
        channel.subscribe(responseTopic2, HelloReply.parser(), obs3);

        assertEquals(3, channel.getStats().getSubscribers());
        channel.unsubscribe(responseTopic1);
        assertEquals(1, channel.getStats().getSubscribers());
        channel.unsubscribe(responseTopic2);
        assertEquals(0, channel.getStats().getSubscribers());

    }





}
