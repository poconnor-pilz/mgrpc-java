package io.mgrpc.examples.hello;

import io.mgrpc.EmbeddedBroker;
import io.mgrpc.Id;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
import io.mgrpc.mqtt.MqttChannelTransport;
import io.mgrpc.mqtt.MqttServerTransport;
import io.mgrpc.utils.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;


public class TestHelloInProcess extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    MessageChannel channel;
    MessageServer server;


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shrt(Id.random());

    private static final long REQUEST_TIMEOUT = 2000;

    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient(null);
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
        server = new MessageServer(new MqttServerTransport(serverMqtt, SERVER));
        server.start();
        server.addService(new HelloServiceForTest());
        channel = new MessageChannel(new MqttChannelTransport(clientMqtt, SERVER));
        channel.start();

//        InProcessMessageTransport transport = new InProcessMessageTransport();
//        server = new MessageServer(transport.getServerTransport());
//        server.start();
//        server.addService(new HelloServiceForTest());
//        channel = new MessageChannel(transport.getChannelTransport());
//        channel.start();

    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
        channel.close();
    }


    @Override
    public MessageChannel getChannel() {
        return this.channel;
    }

    @Override
    public MessageServer getServer() {
        return this.server;
    }
}
