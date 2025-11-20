package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelConduit;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttStreamPubSub;
import io.mgrpc.mqtt.MqttUtils;
import io.mgrpc.utils.ToList;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestStreamsPubSub {

    private static Logger log = LoggerFactory.getLogger(TestStreamsPubSub.class);

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private MessageChannel baseChannel;

    Channel channel;
    private MessageServer server;

    private MqttStreamPubSub pubsub;

    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();


    @BeforeAll
    public static void startClients() throws Exception {
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

    @BeforeEach
    void setup() throws Exception {

        //Set up the serverb
        server = new MessageServer(new MqttServerConduit(serverMqtt, SERVER));
        server.start();

        pubsub = new MqttStreamPubSub(clientMqtt);

        baseChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));

        channel = TopicInterceptor.intercept(baseChannel, SERVER);

    }

    @AfterEach
    void tearDown() throws Exception {
        baseChannel.close();
        server.close();
    }


    public Channel getChannel() {
        return this.channel;
    }

    public MessageServer getServer() {
        return this.server;
    }

    public void checkForLeaks(int numActiveCalls) {
        try {
            //Give the channel and server time to process messages and release resources
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(numActiveCalls, baseChannel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, getServer().getStats().getActiveCalls());
    }




    @Test
    public void testPubSub() throws MessagingException, InterruptedException {
        class HelloObserver implements StreamObserver<HelloReply> {
            public final List<HelloReply> replies = new ArrayList<>();
            public final CountDownLatch latch = new CountDownLatch(2);

            @Override
            public void onNext(HelloReply helloReply) {
                log.debug("Received message: " + helloReply.getMessage());
                replies.add(helloReply);
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("", throwable);
            }

            @Override
            public void onCompleted() {
            }
        }
        ;

        HelloObserver observer = new HelloObserver();
        final String topic = "mgrpc/testpubsub";

        pubsub.subscribe(topic, HelloReply.parser(), observer);

        final StreamObserver<HelloReply> publisher = pubsub.getPublisher(topic);
        publisher.onNext(HelloReply.newBuilder().setMessage("Hello 1").build());
        publisher.onNext(HelloReply.newBuilder().setMessage("Hello 2").build());

        observer.latch.await();

        assertEquals("Hello 1", observer.replies.get(0).getMessage());
        assertEquals("Hello 2", observer.replies.get(1).getMessage());

        Thread.sleep(500);
    }

    @Test
    public void testServiceSubscription() throws InterruptedException, MessagingException {

        //Test the ability to have multiple subscribers listen for responses from
        //a single service via pub sub
        //See the java doc for MessagingSubscriber.subscribe()

        class HelloServiceSub extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                for (int i = 0; i < request.getNumResponses(); i++) {
                    HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName() + " " + i).build();
                    responseObserver.onNext(reply);
                }
                responseObserver.onCompleted();
            }
        }


        getServer().addService(new HelloServiceSub());


        class HelloObserver implements StreamObserver<HelloReply> {

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
                log.error("", throwable);
            }

            @Override
            public void onCompleted() {
                this.countDownLatch.countDown();
            }
        }

        final ServerTopics serverTopics = new ServerTopics(SERVER);
        final String responseTopic1 = serverTopics.out("atesttopic");
        final String responseTopic2 = serverTopics.out("atesttopic2");

        //Subscribe for responses
        CountDownLatch latch = new CountDownLatch(3);
        HelloObserver obs1 = new HelloObserver(latch);
        pubsub.subscribe(responseTopic1, HelloReply.parser(), obs1);
        HelloObserver obs2 = new HelloObserver(latch);
        pubsub.subscribe(responseTopic1, HelloReply.parser(), obs2);
        HelloObserver obsTemp = new HelloObserver(latch);
        pubsub.subscribe(responseTopic1, HelloReply.parser(), obsTemp);
        assertEquals(pubsub.getStats().getSubscribers(), 3);

        //Unsubscribe one of the observers of responseTopic1 and verify that it is removed
        pubsub.unsubscribe(responseTopic1, obsTemp);
        assertEquals(pubsub.getStats().getSubscribers(), 2);

        HelloObserver obs3 = new HelloObserver(latch);
        pubsub.subscribe(responseTopic2, HelloReply.parser(), obs3);
        assertEquals(pubsub.getStats().getSubscribers(), 3);

        //Send two requests each with a different responseTopic
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub1 =
                ExampleHelloServiceGrpc.newBlockingStub(getChannel())
                        .withOption(MessageChannel.OPT_OUT_TOPIC, responseTopic1);

        HelloRequest request1 = HelloRequest.newBuilder()
                .setNumResponses(2)
                .setName("sub1").build();
        final Iterator<HelloReply> helloReplyIterator = blockingStub1.lotsOfReplies(request1);
        //The request should have no responses because all the responses are sent to the response topic
        //instead of the replyTo
        assertEquals(0, ToList.toList(helloReplyIterator).size());

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub2 =
                ExampleHelloServiceGrpc.newBlockingStub(getChannel())
                        .withOption(MessageChannel.OPT_OUT_TOPIC, responseTopic2);
        HelloRequest request2 = HelloRequest.newBuilder()
                .setNumResponses(3)
                .setName("sub2").build();
        blockingStub2.lotsOfReplies(request2);

        latch.await();


        assertEquals(obs1.replies.size(), 2);
        assertEquals("Hello sub1 0", obs1.replies.get(0).getMessage());
        assertEquals("Hello sub1 1", obs1.replies.get(1).getMessage());

        assertEquals(obs2.replies.size(), 2);
        assertEquals("Hello sub1 0", obs2.replies.get(0).getMessage());
        assertEquals("Hello sub1 1", obs2.replies.get(1).getMessage());

        assertEquals(obs3.replies.size(), 3);
        assertEquals("Hello sub2 0", obs3.replies.get(0).getMessage());
        assertEquals("Hello sub2 1", obs3.replies.get(1).getMessage());
        assertEquals("Hello sub2 2", obs3.replies.get(2).getMessage());

        //obsTemp was unsubscribed and so should not receive any responses
        assertEquals(obsTemp.replies.size(), 0);

        checkForLeaks(0);

        //Test unsubscribe of all observers to a responseTopic
        pubsub.subscribe(responseTopic1, HelloReply.parser(), obs1);
        pubsub.subscribe(responseTopic1, HelloReply.parser(), obs2);
        pubsub.subscribe(responseTopic2, HelloReply.parser(), obs3);
        assertEquals(6, pubsub.getStats().getSubscribers());

        pubsub.unsubscribe(responseTopic1);
        //All 2 of the subscribers to responseTopic1 should be removed
        assertEquals(2, pubsub.getStats().getSubscribers());

        pubsub.unsubscribe(responseTopic2);
        assertEquals(0, pubsub.getStats().getSubscribers());

    }


}
