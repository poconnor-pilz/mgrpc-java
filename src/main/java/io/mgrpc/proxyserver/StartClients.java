package io.mgrpc.proxyserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.mgrpc.TopicInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;

public class StartClients {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static void main(String[] args) {


        String target = "localhost:50051";
        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        int numServers = 5;

        HelloRequest tenTimes = HelloRequest.newBuilder()
                .setNumResponses(10)
                .setName("Joe").build();

//        String topic1 = "mgrpc/server-" + 0;
//        ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub1 = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(httpChannel, topic1));
//        final Iterator<HelloReply> helloReplyIterator1 = stub1.lotsOfReplies(tenTimes);
//        int count1 = 0;
//        for(; helloReplyIterator1.hasNext(); ) {
//            final HelloReply reply = helloReplyIterator1.next();
//            if(!reply.getMessage().equals("Hello Joe " + count1++)) {
//                log.error("Reply is out of order");
//                System.exit(1);
//            }
//        }
//
//        if(true) return;


        for(int i=0; i < numServers; i++) {
            String topic = "mgrpc/server-" + i;
            Thread t = new Thread(()->{
                ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(httpChannel, topic));
                while(true) {
                    final Iterator<HelloReply> helloReplyIterator = stub.lotsOfReplies(tenTimes);
                    int count = 0;
                    for(; helloReplyIterator.hasNext(); ) {
                        final HelloReply reply = helloReplyIterator.next();
                        if(!reply.getMessage().equals("Hello Joe " + count++)) {
                            log.error("Reply is out of order");
                            System.exit(1);
                        } else {
                            //log.debug("Received reply: {}", reply.getMessage());
                        }
                    }
                }

            });
            t.start();
        }



    }
}
