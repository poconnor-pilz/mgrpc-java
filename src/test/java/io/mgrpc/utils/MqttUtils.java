package io.mgrpc.utils;

import io.mgrpc.ConnectionStatus;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.InputStream;
import java.util.Properties;


public class MqttUtils {



    private static Properties PROPS = null;
    private static Properties getProperties() throws Exception{
        if(PROPS == null){
            InputStream is = MqttUtils.class.getClassLoader().getResourceAsStream("broker.properties");
            PROPS = new Properties();
            PROPS.load(is);
        }
        return PROPS;
    }


    public static MqttAsyncClient makeClient(String lwtTopic) throws Exception {
        String brokerUrl = (String)getProperties().get("brokerUrl");
        return makeClient(lwtTopic, brokerUrl);
    }

    public static MqttAsyncClient makeClient(String lwtTopic, String brokerUrl) throws Exception {


        final MqttAsyncClient client;
        client = new MqttAsyncClient(
                brokerUrl,
                MqttAsyncClient.generateClientId(),
                new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setMaxInflight(1000);

        if(lwtTopic != null){
            final byte[] lwtMessage = ConnectionStatus.newBuilder().setConnected(false).build().toByteArray();
            mqttConnectOptions.setWill(lwtTopic, lwtMessage, 1, false);
        }
        client.connect(mqttConnectOptions).waitForCompletion();
        return client;
    }
}
