package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelConduitManager;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestHelloMqtt extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    Channel channel;
    MessageChannel baseChannel;

    MessageServer server;



    private static final long REQUEST_TIMEOUT = 2000;

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

    @BeforeEach
    void setup() throws Exception{

        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String serverTopic = "mgprc/" + Id.shortRandom();

        //Set up the serverb
        server = new MessageServer(new MqttServerConduit(serverMqtt, serverTopic));
        server.start();
        server.addService(new HelloServiceForTest());
        baseChannel = new MessageChannel(new MqttChannelConduitManager(clientMqtt));
        baseChannel.start();
        channel = ClientInterceptors.intercept(baseChannel, new TopicInterceptor(serverTopic));
    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
        baseChannel.close();
    }


    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void checkNumActiveCalls(int numActiveCalls) {
        assertEquals(numActiveCalls, this.baseChannel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, this.server.getStats().getActiveCalls());
    }

}
