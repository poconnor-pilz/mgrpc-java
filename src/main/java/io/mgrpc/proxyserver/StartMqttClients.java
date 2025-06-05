package io.mgrpc.proxyserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.mgrpc.MessageChannel;
import io.mgrpc.TopicInterceptor;
import io.mgrpc.mqtt.MqttChannelConduit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;

public class StartMqttClients {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static void main(String[] args) throws Exception {


        if(args.length != 1) {
            System.out.println("Usage: StartProxy mqtt broker url (e.g. tcp://localhost:1883)");
            return;
        }
        final ClientFactory clientFactory = new ClientFactory(args[0]);
        final MqttChannelConduit mqttChannelConduit = new MqttChannelConduit(clientFactory);
        MessageChannel messageChannel = new MessageChannel(mqttChannelConduit);

        int numServers = 5;

        log.debug("Running clients");
        HelloRequest tenTimes = HelloRequest.newBuilder()
                .setNumResponses(10)
                .setName("Joe").build();

        for (int i = 0; i < numServers; i++) {
            String topic = "mgrpc/server-" + i;
            Thread t = new Thread(() -> {
                ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(messageChannel, topic));
                while (true) {
                    final Iterator<HelloReply> helloReplyIterator = stub.lotsOfReplies(tenTimes);
                    int count = 0;
                    while (helloReplyIterator.hasNext()) {
                        final HelloReply reply = helloReplyIterator.next();
                        if (!reply.getMessage().equals("Hello Joe " + count++)) {
                            log.error("Reply is out of order");
                            System.exit(1);
                        }
                    }
                }

            });
            t.start();
        }

    }

}
