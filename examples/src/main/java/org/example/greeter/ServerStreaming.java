package org.example.greeter;

import io.grpc.Channel;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.mgrpc.MessageChannel;
import io.mgrpc.TopicInterceptor;
import io.mgrpc.mqtt.MqttChannelConduit;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.example.mqttutils.MqttUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;

public class ServerStreaming {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Example that executes a server streaming RPC (single request, stream of responses)
     * Before running this example make sure that the GreeterService has been started.
     */
    public static void main(String[] args) throws Exception {


        String brokerUrl = "tcp://localhost:1887";
        MqttAsyncClient clientMqtt = MqttUtils.makeClient(brokerUrl);

        MessageChannel baseChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));
        //Make sure all messages on the channel are routed through GreeterService.SERVER_TOPIC
        Channel channel = TopicInterceptor.intercept(baseChannel, GreeterService.SERVER_TOPIC);


        final GreeterGrpc.GreeterBlockingStub blockingStub = GreeterGrpc.newBlockingStub(channel);

        //Server streaming
        //Send a single HelloRequest message and return a stream of HelloReply messages
        HelloRequest request = HelloRequest.newBuilder().setName("Joe").setNumResponses(3).build();
        final Iterator<HelloReply> helloReplyIterator = blockingStub.lotsOfReplies(request);
        while (helloReplyIterator.hasNext()) {
            HelloReply reply = helloReplyIterator.next();
            log.debug("Client received: " + reply.getMessage());
        }

        baseChannel.close();
        clientMqtt.disconnect().waitForCompletion();
        clientMqtt.close();

    }

}
