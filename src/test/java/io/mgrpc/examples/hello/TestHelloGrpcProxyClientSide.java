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
 * Make a http client and server where the http server proxies calls to the broker and test it
 * httpChannel -> httpServer -> GrpcProxy -> messageChannel -> broker-> messageServer
*/
public class TestHelloGrpcProxyClientSide extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();

    MessageChannel messageChannel;
    MessageServer messageServer;
    Server httpServer;

    Channel interceptedHttpChannel;

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

        messageServer = new MessageServer(new MqttServerConduit(serverMqtt, SERVER));
        messageServer.start();
        messageServer.addService(new HelloServiceForTest());

        messageChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));
        messageChannel.start();

        GrpcProxy proxy = new GrpcProxy(messageChannel);

        int port1 = 8981;
        httpServer = Grpc.newServerBuilderForPort(port1, InsecureServerCredentials.create())
                .fallbackHandlerRegistry(new GrpcProxy.Registry(proxy))
                .build()
                .start();

        String target = "localhost:" + port1;
        Channel httpChannel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();

        interceptedHttpChannel = ClientInterceptors.intercept(httpChannel, new TopicInterceptor(SERVER));

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
        return interceptedHttpChannel;
    }

    @Override
    public void checkNumActiveCalls(int numActiveCalls) {
        assertEquals(messageServer.getStats().getActiveCalls(), numActiveCalls);
        assertEquals(messageChannel.getStats().getActiveCalls(), numActiveCalls);
    }
}
