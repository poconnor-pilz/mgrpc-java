package io.mgrpc.mqtt;


import io.mgrpc.ConnectionStatus;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Properties;


public class MqttUtils {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Properties PROPS = null;
    private static Properties getProperties() throws Exception{
        if(PROPS == null){
            InputStream is = MqttUtils.class.getClassLoader().getResourceAsStream("broker.properties");
            PROPS = new Properties();
            PROPS.load(is);
        }
        return PROPS;
    }


    public static MqttAsyncClient makeClient() throws Exception {
        String brokerUrl = (String)getProperties().get("brokerUrl");
        return makeClient(null, null, brokerUrl);
    }

    public static MqttAsyncClient makeClient(String lwtTopic, String channelId) throws Exception {
        String brokerUrl = (String)getProperties().get("brokerUrl");
        return makeClient(lwtTopic, channelId, brokerUrl);
    }

    public static MqttAsyncClient makeClient(String lwtTopic, String channelId, String brokerUrl) throws Exception {

        return makeClient(lwtTopic, channelId, brokerUrl, null);
    }

    public static MqttAsyncClient makeClient(String lwtTopic, String channelId, SocketFactory socketFactory) throws Exception {

        String brokerUrl = (String)getProperties().get("brokerUrl");
        return makeClient(lwtTopic, channelId, brokerUrl, socketFactory);
    }


    public static MqttAsyncClient makeClient(String lwtTopic, String channelId, String brokerUrl, SocketFactory socketFactory) throws Exception {
        final MqttAsyncClient client;
        client = new MqttAsyncClient(
                brokerUrl,
                MqttAsyncClient.generateClientId(),
                new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setMaxInflight(1000);

        if(socketFactory != null){
            mqttConnectOptions.setSocketFactory(socketFactory);
        }

        if(lwtTopic != null){
            log.debug("Setting LWT to: " + lwtTopic + " channelId " + channelId);
            final ConnectionStatus.Builder builder = ConnectionStatus.newBuilder();
            builder.setConnected(false);
            if(channelId != null){
                builder.setChannelId(channelId);
            }
            final byte[] lwtMessage = builder.build().toByteArray();
            mqttConnectOptions.setWill(lwtTopic, lwtMessage, 1, true);
        }
        client.connect(mqttConnectOptions).waitForCompletion();
        return client;
    }


}
