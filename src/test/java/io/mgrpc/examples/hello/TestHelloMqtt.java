package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.mgrpc.EmbeddedBroker;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
import io.mgrpc.mqtt.MqttChannelBuilder;
import io.mgrpc.mqtt.MqttServerBuilder;
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


public class TestHelloMqtt extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    MessageChannel channel;

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
        channel = new MqttChannelBuilder().setClient(clientMqtt).build();
    }

    @AfterEach
    void tearDown() throws Exception{
        channel.close();
    }


    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public int getChannelActiveCalls() {
        return this.channel.getStats().getActiveCalls();
    }

    public MessageServer makeMessageServer(String serverTopic) throws Exception {
        MessageServer server = new MqttServerBuilder().setClient(serverMqtt).setTopic(serverTopic).build();
        server.start();
        return server;
    }


}
