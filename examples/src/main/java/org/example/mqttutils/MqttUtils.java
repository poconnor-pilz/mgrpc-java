package org.example.mqttutils;

import io.mgrpc.ConnStatus;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.InputStream;
import java.util.Properties;

public class MqttUtils {

    private static Properties PROPS = null;
    public static Properties getProperties() throws Exception{
        if(PROPS == null){
            InputStream is = MqttUtils.class.getClassLoader().getResourceAsStream("broker.properties");
            PROPS = new Properties();
            PROPS.load(is);
        }
        return PROPS;
    }

    public  static String getBrokerUrl() throws Exception{
        String brokerUrl = (String)getProperties().get("brokerUrl");
        return brokerUrl;
    }

    public static MqttAsyncClient makeClient(String brokerUrl) throws Exception {
        return makeClient(brokerUrl, null);
    }

    public static MqttAsyncClient makeClient(String brokerUrl, String lwtTopic) throws Exception {
        final MqttAsyncClient client;
        client = new MqttAsyncClient(
                brokerUrl,
                MqttAsyncClient.generateClientId(),
                new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setMaxInflight(1000);

        if(lwtTopic != null){
            log("Setting LWT to: " + lwtTopic);
            final byte[] lwtMessage = ConnStatus.makeConnectionStatusProto(false);
            mqttConnectOptions.setWill(lwtTopic, lwtMessage, 1, true);
        }
        client.connect(mqttConnectOptions).waitForCompletion();
        return client;

    }

    public static void log(String msg){
        System.out.println(msg);
    }

    public static class EmbeddedBroker {

        private static boolean STARTED = false;

        private static EmbeddedActiveMQ embeddedActiveMQ;
        public static void start() throws Exception {

            if(STARTED){
                return;
            }
            synchronized (EmbeddedBroker.class) {
                embeddedActiveMQ = new EmbeddedActiveMQ();
                embeddedActiveMQ.start();
                STARTED = true;
            }
        }

        public static void stop() throws Exception {

            if(STARTED){
                embeddedActiveMQ.stop();
                STARTED = false;
            }
        }

        public static void main(String[] args) throws Exception{
            start();
        }

    }
}
