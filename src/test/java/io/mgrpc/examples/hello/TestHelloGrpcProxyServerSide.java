package io.mgrpc.examples.hello;

import io.grpc.*;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelConduit;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 This tests the case where a java client that can connect to an mqtt broker can call
 a http server on the server side of the broker that is mapped to a particular topic.
 A jvm has to be on the server side of the broker that makes that connection.

 MqttChannel -> Broker -> MqttServer ->  GrpcProxy -> HttpChannel -> HttpServer
 */
public class TestHelloGrpcProxyServerSide extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();

    MessageChannel messageChannel;
    MessageServer messageServer;
    Server httpServer;

    Channel messageChannelWithTopic;


    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClients() throws Exception {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
    }


    @BeforeEach
    void setup() throws Exception{

        //We want to wire this:
        //MqttChannel -> Broker -> MqttServer ->  GrpcProxy -> HttpChannel -> HttpServer

        int port = 50051;
        String target = "localhost:" + port;

        messageChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));

        messageChannelWithTopic = ClientInterceptors.intercept(messageChannel, new TopicInterceptor(SERVER));


        httpServer = ServerBuilder.forPort(port)
                .addService(new HelloServiceForTest())
                .build().start();

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        messageServer = new MessageServer(new MqttServerConduit(serverMqtt, SERVER));

        //We want to wire this:
        //MqttServer ->  GrpcProxy -> HttpChannel

        //Connect the proxy to the http channel
        final GrpcProxy proxy2 = new GrpcProxy(httpChannel);
        //Connect the MessageServer to the proxy
        messageServer.setFallBackRegistry(new GrpcProxy.Registry(proxy2));

        messageServer.start();

    }



    @AfterEach
    void tearDown() throws Exception{
        messageChannel.close();
        messageServer.close();
        httpServer.shutdownNow();
        httpServer.awaitTermination();
    }


    @Override
    public Channel getChannel() {
        return messageChannelWithTopic;
    }

    @Override
    public void checkNumActiveCalls(int numActiveCalls) {
        assertEquals(messageServer.getStats().getActiveCalls(), numActiveCalls);
        assertEquals(messageChannel.getStats().getActiveCalls(), numActiveCalls);
    }
}
