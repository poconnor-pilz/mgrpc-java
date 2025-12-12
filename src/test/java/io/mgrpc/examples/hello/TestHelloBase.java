package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.mgrpc.Id;
import io.mgrpc.MessageServer;
import io.mgrpc.StreamWaiter;
import io.mgrpc.TopicInterceptor;
import io.mgrpc.utils.ToList;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public abstract class TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private static final long REQUEST_TIMEOUT = 2000;




    public abstract Channel getChannel();



    public int getChannelActiveCalls() {
        return 0;
    }

    public abstract MessageServer makeMessageServer(String serverTopic) throws Exception;

    @Test
    public void testSayHello() throws Exception {

        String serverTopic = "mgrpc/" + Id.shortRandom();
        Channel channel = TopicInterceptor.intercept(getChannel(), serverTopic);
        MessageServer server = makeMessageServer(serverTopic);
        server.addService(new HelloServiceForTest());

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = blockingStub.sayHello(joe);
        assertEquals("Hello joe", helloReply.getMessage());

        //Check for leaks
        Thread.sleep(50); //Give close() threads a chance to complete
        assertEquals(0, server.getStats().getActiveCalls());
        assertEquals(0, getChannelActiveCalls());
        server.close();
    }



    @Test
    public void testLotsOfReplies() throws Throwable {

        String serverTopic = "mgrpc/" + Id.shortRandom();
        MessageServer server = runtLotsOfReplies(getChannel(), serverTopic, "somename");
        Thread.sleep(50); //Give close() threads a chance to complete
        assertEquals(0, server.getStats().getActiveCalls());
        assertEquals(0, getChannelActiveCalls());
        server.close();
    }


    @Test
    public void testLotsOfReplies2Servers() throws Throwable {

        //Verify that messages get dispatched and returned correctly to two
        //different servers on two different topics via one base channel
        Channel channel = getChannel();
        String serverTopic = "mgrpc/" + Id.shortRandom();
        MessageServer server = runtLotsOfReplies(channel, serverTopic, "somename1");
        String serverTopic2 = "mgrpc/" + Id.shortRandom();
        MessageServer server2 = runtLotsOfReplies(channel, serverTopic2, "somename2");
        Thread.sleep(50); //Give close() threads a chance to complete
        assertEquals(0, server.getStats().getActiveCalls());
        assertEquals(0, server2.getStats().getActiveCalls());
        assertEquals(0, getChannelActiveCalls());
        server.close();
    }

    public MessageServer runtLotsOfReplies(Channel baseChannel, String serverTopic, String name) throws Throwable {

        Channel channel = TopicInterceptor.intercept(baseChannel, serverTopic);
        MessageServer server = makeMessageServer(serverTopic);
        server.addService(new HelloServiceForTest());

        final int numReplies = 5;

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc
                .newBlockingStub(channel)
                .withDeadlineAfter(60*1000, TimeUnit.MILLISECONDS);

        //Run the service with a small delay between messages. This will test flow control
        //Without flow control a BlockingStub will fail as it puts incoming messages in a queue of only 3 elements
        //and fails if the queue is exceeded. (See BlockingResponseStream in ClientCalls.java in the gRPC codebase)
        HelloRequest twoReplies = HelloRequest.newBuilder()
                .setName(name)
                .setNumResponses(numReplies)
                .setMillisecondsDelay(20)
                .build();
        List<HelloReply> responseList = ToList.toList(stub.lotsOfReplies(twoReplies));
        assertEquals(numReplies, responseList.size());

        for(int i = 0; i < numReplies; i++) {
            assertEquals("Hello " + name + " " + i, responseList.get(i).getMessage());
        }

        return server;
    }



    @Test
    public void testParallelReplies() throws Throwable {

        //This test runs calls in parallel and checks that messages are received in order
        //Sometimes it may show up a concurrency bug by freezing or timing out.

        String serverTopic = "mgrpc/" + Id.shortRandom();
        Channel channel = TopicInterceptor.intercept(getChannel(), serverTopic);
        MessageServer server = makeMessageServer(serverTopic);
        server.addService(new HelloServiceForTest());
        String serverTopic2 = "mgrpc/" + Id.shortRandom();
        Channel channel2 = TopicInterceptor.intercept(getChannel(), serverTopic2);
        MessageServer server2 = makeMessageServer(serverTopic2);
        server2.addService(new HelloServiceForTest());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub2 = ExampleHelloServiceGrpc.newStub(channel2);
        //Tell lotsOfReplies to reply 10 times
        HelloRequest tenTimes = HelloRequest.newBuilder()
                .setNumResponses(10)
                .setName("Joe").build();
        int numRequests = 20;
        final CountDownLatch latch = new CountDownLatch(numRequests * 2);
        final boolean [] messagesInOrder =  {true};

        class ParObserver implements StreamObserver<HelloReply> {

            private int count = 0;
            @Override
            public void onNext(HelloReply value) {
                if(!value.getMessage().equals("Hello Joe " + count)){
                    messagesInOrder[0] = false;
                }
                count++;
                try {
                    sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public void onError(Throwable throwable) {}
            @Override
            public void onCompleted() {
                latch.countDown();
            }
        }


        Thread t1 = new Thread(() -> {
            for (int i = 0; i < numRequests; i++) {
                stub.lotsOfReplies(tenTimes, new ParObserver());
            }

        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < numRequests; i++) {
                stub2.lotsOfReplies(tenTimes, new ParObserver());
            }
        });

        t1.start();
        t2.start();

        assertTrue(latch.await(60, TimeUnit.SECONDS), "timed out");

        assertTrue(messagesInOrder[0], "Messages were not received in order");

        //Check for leaks
        Thread.sleep(50); //Give close() threads a chance to complete
        assertEquals(0, server.getStats().getActiveCalls());
        assertEquals(0, server2.getStats().getActiveCalls());
        assertEquals(0, getChannelActiveCalls());
        server.close();
        server2.close();
    }


    @Test
    public void testLotsOfGreetings() throws Exception {

        String serverTopic = "mgrpc/" + Id.shortRandom();
        Channel channel = TopicInterceptor.intercept(getChannel(), serverTopic);
        MessageServer server = makeMessageServer(serverTopic);
        server.addService(new HelloServiceForTest());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        HelloRequest john = HelloRequest.newBuilder().setName("john").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>(REQUEST_TIMEOUT);
        StreamObserver<HelloRequest> clientStreamObserver = stub.lotsOfGreetings(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onNext(jane);
        clientStreamObserver.onNext(john);
        clientStreamObserver.onCompleted();
        final HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe,jane,john,", reply.getMessage());
        //Check for leaks
        Thread.sleep(50); //Give close() threads a chance to complete
        assertEquals(0, server.getStats().getActiveCalls());
        assertEquals(0, getChannelActiveCalls());
        server.close();
    }


    @Test
    public void testBidiHello() throws Throwable {

        String serverTopic = "mgrpc/" + Id.shortRandom();
        Channel channel = TopicInterceptor.intercept(getChannel(), serverTopic);
        MessageServer server = makeMessageServer(serverTopic);
        server.addService(new HelloServiceForTest());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);

        class TestHelloReplyObserver implements StreamObserver<HelloReply> {
            public HelloReply lastReply;
            public CountDownLatch latch = new CountDownLatch(1);

            @Override
            public void onNext(HelloReply value) {
                lastReply = value;
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        }
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        TestHelloReplyObserver replyObserver = new TestHelloReplyObserver();
        StreamObserver<HelloRequest> clientStreamObserver = stub.bidiHello(replyObserver);
        clientStreamObserver.onNext(joe);
        assertTrue(replyObserver.latch.await(10, TimeUnit.SECONDS), "timed out");
        assertEquals("Hello joe", replyObserver.lastReply.getMessage());
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onNext(jane);
        assertTrue(replyObserver.latch.await(10, TimeUnit.SECONDS), "timed out");
        assertEquals("Hello jane", replyObserver.lastReply.getMessage());
        //close the call cleanly
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onCompleted();
        assertTrue(replyObserver.latch.await(10, TimeUnit.SECONDS), "timed out");
        //Check for leaks
        Thread.sleep(50); //Give close() threads a chance to complete
        assertEquals(0, server.getStats().getActiveCalls());
        assertEquals(0, getChannelActiveCalls());
        server.close();
    }




        @Test
    public void testInProcess() throws Exception {
        String uniqueName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(uniqueName)
                .directExecutor()
                .addService(new HelloServiceForTest() {
                })
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName)
                .directExecutor()
                .build();

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        final HelloRequest name = HelloRequest.newBuilder().setName("test name").build();
        HelloReply reply = blockingStub.sayHello(name);
        assertEquals("Hello test name", reply.getMessage());

        channel.shutdown();
        server.shutdown();
    }


}
