package io.mgrpc.utils;

import io.mgrpc.MessagingClient;
import io.mgrpc.MessagingException;
import io.mgrpc.MessagingListener;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttMessagingClient implements MessagingClient {

    private final MqttAsyncClient client;
    private final static String TOPIC_SEPARATOR = "/";
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    public MqttMessagingClient(MqttAsyncClient client) {
        this.client = client;
    }

    @Override
    public String topicSeparator(){
        return TOPIC_SEPARATOR;
    }

    public void disconnect() throws MqttException {
        this.client.disconnect();
    }

    public void close() throws MqttException {
        this.client.close();
    }
    @Override
    public void publish(String topic, byte[] buffer) throws MessagingException {
        try {
            client.publish(topic, new MqttMessage(buffer));
        } catch (MqttException e) {
            throw new MessagingException(e);
        }
    }

    @Override
    public void subscribe(String topic, MessagingListener listener) throws MessagingException {
        try {
            client.subscribe(topic, 1, new IMqttMessageListener() {
                @Override
                public void messageArrived(String tpic, MqttMessage mqttMessage) throws Exception {
                    listener.messageArrived(tpic, mqttMessage.getPayload());
                }
            }).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);
        } catch (MqttException e) {
            throw new MessagingException(e);
        }
    }

    @Override
    public void subscribeAll(String topicPrefix, MessagingListener listener) throws MessagingException {
        subscribe(topicPrefix + "/#", listener);
    }

    @Override
    public void unsubscribe(String topic) throws MessagingException {
        try {
            client.unsubscribe(topic).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);
        } catch (MqttException e) {
            throw new MessagingException(e);
        }
    }

    @Override
    public void unsubscribeAll(String topicPrefix) throws MessagingException {
        unsubscribe(topicPrefix + "/#");
    }
}
