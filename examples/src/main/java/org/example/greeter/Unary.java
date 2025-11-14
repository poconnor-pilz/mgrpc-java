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

/**
 * Example that executes a unary RPC (single request, single response)
 * Before running this example make sure that the GreeterService has been started.
 */
public class Unary {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) throws Exception {

        String brokerUrl = "tcp://localhost:1887";
        MqttAsyncClient clientMqtt = MqttUtils.makeClient(brokerUrl);

        MessageChannel baseChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));
        //Make sure all messages on the channel are routed through GreeterService.SERVER_TOPIC
        Channel channel = TopicInterceptor.intercept(baseChannel, GreeterService.SERVER_TOPIC);


        //Unary request response
        //Send a single HelloRequest message and return a single HelloReply
        final GreeterGrpc.GreeterBlockingStub blockingStub = GreeterGrpc.newBlockingStub(channel);
        HelloRequest request = HelloRequest.newBuilder().setName("Joe").build();
        final HelloReply helloReply = blockingStub.sayHello(request);
        log.debug("Client received: " + helloReply.getMessage());


        baseChannel.close();
        clientMqtt.disconnect().waitForCompletion();
        clientMqtt.close();

    }


}