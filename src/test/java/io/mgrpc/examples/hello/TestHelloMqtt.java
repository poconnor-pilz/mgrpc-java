package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.mgrpc.EmbeddedBroker;
import io.mgrpc.Id;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
import io.mgrpc.mqtt.MqttChannelConduit;
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

    MessageChannel channel;
    MessageServer server;


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();

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

        //Set up the serverb
        server = new MessageServer(new MqttServerConduit(serverMqtt, SERVER));
        server.start();
        server.addService(new HelloServiceForTest());
        channel = new MessageChannel(new MqttChannelConduit(clientMqtt, SERVER));
        channel.start();
    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
        channel.close();
    }


    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void checkNumActiveCalls(int numActiveCalls) {
        assertEquals(numActiveCalls, this.channel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, this.server.getStats().getActiveCalls());
    }

}
