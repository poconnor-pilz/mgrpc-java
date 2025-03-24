package io.mgrpc.mqtt;

import io.mgrpc.MessageChannel;
import io.mgrpc.MessageChannelFactory;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;

public class MqttChannelFactory implements MessageChannelFactory {

    private final MqttAsyncClient client;

    public MqttChannelFactory(MqttAsyncClient client) {
        this.client = client;
    }

    @Override
    public MessageChannel createMessageChannel(String serverTopic) {
        MessageChannel channel = new MessageChannel(new MqttChannelConduit(client, serverTopic));
        return channel;
    }
}
