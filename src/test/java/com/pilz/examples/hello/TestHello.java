package com.pilz.examples.hello;

import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import com.pilz.utils.ToList;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.RequestWithReplyTo;
import io.grpc.stub.StreamObserver;
import org.checkerframework.checker.units.qual.C;
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


public class TestHello {

    private static Logger log = LoggerFactory.getLogger(TestHello.class);

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
    public void testWithoutReplyTo() throws InterruptedException {

        //First verify that if we don't specify a replyTo then multipleSubscribers behaves like a normal service
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        RequestWithReplyTo request = RequestWithReplyTo.newBuilder()
                .setName("2").build();
        final Iterator<HelloReply> helloReplyIterator = blockingStub.multipleSubscribers(request);
        List<HelloReply> responseList = ToList.toList(helloReplyIterator);
        assertEquals(responseList.size(), 2);
        assertEquals("Hello 0", responseList.get(0).getMessage());
        assertEquals("Hello 1", responseList.get(1).getMessage());
        checkForLeaks(0);

    }


    @Test
    public void testSubscription() throws InterruptedException {

        //Set up multiple subscribers to listen for responses on a service via pub sub

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(channel);

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

        final String topic1 = Topics.out(DEVICE,"atesttopic");

        CountDownLatch latch = new CountDownLatch(3);
        HelloObserver obs1 = new HelloObserver(latch);
        channel.subscribeForStream(topic1, HelloReply.parser(), obs1);
        HelloObserver obs2 = new HelloObserver(latch);
        channel.subscribeForStream(topic1, HelloReply.parser(), obs2);
        HelloObserver obsTemp = new HelloObserver(latch);
        channel.subscribeForStream(topic1, HelloReply.parser(), obsTemp);

        assertEquals(channel.getStats().getSubscribers(), 3);
        channel.unsubscribeForStream(topic1, obsTemp);
        assertEquals(channel.getStats().getSubscribers(), 2);

        final String topic2 = Topics.out(DEVICE,"atesttopic2");
        HelloObserver obs3 = new HelloObserver(latch);
        channel.subscribeForStream(topic2, HelloReply.parser(), obs3);
        assertEquals(channel.getStats().getSubscribers(), 3);

        //Send two requests each with a different topic
        RequestWithReplyTo request = RequestWithReplyTo.newBuilder()
                .setReplyToTopic(topic1)
               .setName("2").build();

        RequestWithReplyTo request2 = RequestWithReplyTo.newBuilder()
                .setReplyToTopic(topic2)
                .setName("3").build();

        final Iterator<HelloReply> helloReplyIterator = blockingStub.multipleSubscribers(request);
        blockingStub.multipleSubscribers(request2);

        latch.await();

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

        checkForLeaks(0);

    }



}
