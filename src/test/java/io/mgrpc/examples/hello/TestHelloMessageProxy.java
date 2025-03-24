package io.mgrpc.examples.hello;

import io.grpc.*;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelFactory;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Make a http client and server where the http server proxies calls to the broker and test it
 * httpChannel -> httpServer -> GrpcProxy -> messageChannel -> broker-> messageServer
*/
public class TestHelloMessageProxy extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private final String SERVER = Id.shortRandom();

    MessageServer messageServer;
    Server httpServer;

    MessageProxy<byte[], byte[]> proxy;

    Channel httpChannel;
    ManagedChannel basicHttpChannel;

    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClients() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close(true);
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close(true);
        clientMqtt = null;
    }


    @BeforeEach
    void setup() throws Exception{

        //We want to wire this:
        //httpChannel -> httpServer -> GrpcProxy -> messageChannel -> broker-> messageServer

        messageServer = new MessageServer(new MqttServerConduit(serverMqtt, SERVER));
        messageServer.start();
        messageServer.addService(new HelloServiceForTest());

        final MqttChannelFactory mqttChannelFactory = new MqttChannelFactory(clientMqtt);
        proxy = new MessageProxy(mqttChannelFactory);

        int port1 = 8982;
        httpServer = Grpc.newServerBuilderForPort(port1, InsecureServerCredentials.create())
                .fallbackHandlerRegistry(new MessageProxy.Registry(proxy))
                .build()
                .start();

        String target = "localhost:" + port1;
        basicHttpChannel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();

        //Add the interceptor that will take the server topic and put it in the metadata header for the call
        httpChannel = ClientInterceptors.intercept(basicHttpChannel, new TopicInterceptor(SERVER));

    }


    @AfterEach
    void tearDown() throws Exception{
        proxy.close(); //will close all channels
        messageServer.close();
        httpServer.shutdownNow();
        httpServer.awaitTermination();
    }


    @Override
    public Channel getChannel() {
        return httpChannel;
    }



    @Override
    public void checkNumActiveCalls(int numActiveCalls) {
        assertEquals(messageServer.getStats().getActiveCalls(), numActiveCalls);
    }
}
