package io.mgrpc.examples.hello;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
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


public abstract class TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();

    private static final long REQUEST_TIMEOUT = 2000;


    public void checkForLeaks(int numActiveCalls){
        try {
            //Give the channel and server time to process messages and release resources
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(numActiveCalls, getChannel().getStats().getActiveCalls());
        assertEquals(numActiveCalls, getServer().getStats().getActiveCalls());
    }

    public abstract MessageChannel getChannel();

    public abstract MessageServer getServer();

    @Test
    public void testSayHello() {
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(getChannel())
                .withDeadlineAfter(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);;
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = blockingStub.sayHello(joe);
        assertEquals("Hello joe", helloReply.getMessage());
        checkForLeaks(0);
    }


    @Test
    public void testLotsOfReplies() throws Throwable{

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc
                .newBlockingStub(getChannel())
                .withDeadlineAfter(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        HelloRequest joe = HelloRequest.newBuilder().setName("2").build();
        List<HelloReply> responseList = ToList.toList(stub.lotsOfReplies(joe));
        assertEquals(responseList.size(), 2);
        assertEquals("Hello 0", responseList.get(0).getMessage());
        assertEquals("Hello 1", responseList.get(1).getMessage());
        checkForLeaks(0);
    }

    @Test
    public void testParallelReplies() throws Throwable{

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(getChannel());
        HelloRequest joe = HelloRequest.newBuilder().setName("10").build();
        int numRequests = 100;
        final CountDownLatch latch = new CountDownLatch(numRequests);
        for(int i = 0; i < numRequests; i++) {
            final int index = i;
            stub.lotsOfReplies(joe, new NoopStreamObserver<HelloReply>() {
                @Override
                public void onNext(HelloReply value) {
                    log.debug(index + " - " + value.getMessage());
                    try {
                        sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            });
        }

        latch.await();
        checkForLeaks(0);
    }


    @Test
    public void testLotsOfGreetings(){

        log.debug("testLotsOfGreetings");
        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(getChannel());
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>(REQUEST_TIMEOUT);
        StreamObserver<HelloRequest> clientStreamObserver = stub.lotsOfGreetings(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onNext(jane);
        clientStreamObserver.onCompleted();
        final HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe,jane,", reply.getMessage());
        checkForLeaks(0);
    }


    @Test
    public void testBidiHello() throws Throwable{

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(getChannel());

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
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello joe", replyObserver.lastReply.getMessage());
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onNext(jane);
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        assertEquals("Hello jane", replyObserver.lastReply.getMessage());
        //close the call cleanly
        replyObserver.latch = new CountDownLatch(1);
        clientStreamObserver.onCompleted();
        replyObserver.latch.await(10, TimeUnit.SECONDS);
        checkForLeaks(0);
    }




    @Test
    public void testInProcess() throws Exception{
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
        checkForLeaks(0);
    }




}
