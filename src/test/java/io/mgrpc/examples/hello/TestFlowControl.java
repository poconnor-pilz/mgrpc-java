package io.mgrpc.examples.hello;


import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelConduit;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

public class TestFlowControl {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;


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

    @Test
    void testBasicFlow() throws Exception {


        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String SERVER = Id.shortRandom();
        MessageServer server = new MessageServer(new MqttServerConduit(serverMqtt, SERVER));

        server.start();

        server.addService(new HelloServiceForTest());

        MessageChannel messageChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));
        Channel channel = TopicInterceptor.intercept(messageChannel, SERVER);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub =
                ExampleHelloServiceGrpc.newBlockingStub(channel);
        final HelloRequest request = HelloRequest.newBuilder().
                setName("joe")
                .setNumResponses(88).build();

        final Iterator<HelloReply> helloReplyIterator = stub.lotsOfReplies(request);
        while(helloReplyIterator.hasNext()) {
            final HelloReply reply = helloReplyIterator.next();
            log.debug(reply.getMessage());
        }

        messageChannel.close();
        server.close();

    }
}
