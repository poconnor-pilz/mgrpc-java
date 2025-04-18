package io.mgrpc.examples.hello;

import io.grpc.*;
import io.mgrpc.EmbeddedBroker;
import io.mgrpc.GrpcProxy;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
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

/**
 * Make a http client and server where the http server proxies calls to the broker and test it
 * httpChannel -> httpServer -> GrpcProxy -> messageChannel -> broker-> messageServer
*/
public class TestHelloGrpcProxyClientSide extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;


    MessageChannel messageChannel;
    ManagedChannel httpChannel;
    Server httpServer;


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
        //httpChannel -> httpServer -> GrpcProxy -> messageChannel -> broker-> messageServer

        messageChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));

        GrpcProxy proxy = new GrpcProxy(messageChannel);

        int port1 = 8981;
        httpServer = Grpc.newServerBuilderForPort(port1, InsecureServerCredentials.create())
                .fallbackHandlerRegistry(new GrpcProxy.Registry(proxy))
                .build()
                .start();

        String target = "localhost:" + port1;
        httpChannel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();


    }


    @AfterEach
    void tearDown() throws Exception{
        httpChannel.shutdownNow();
        messageChannel.close();
        httpServer.shutdownNow();
        httpServer.awaitTermination();
    }


    @Override
    public Channel getChannel() {
        return httpChannel;
    }


    @Override
    public int getChannelActiveCalls() {
        return this.messageChannel.getStats().getActiveCalls();
    }

    public MessageServer makeMessageServer(String serverTopic) throws Exception {
        MessageServer server  = new MessageServer(new MqttServerConduit(serverMqtt, serverTopic));
        server.start();
        return server;
    }

}
