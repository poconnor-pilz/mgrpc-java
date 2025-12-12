package io.mgrpc.mqtt;

import io.mgrpc.MessageChannel;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;

public class MqttChannelBuilder {

    private static final int DEFAULT_FLOW_CREDIT = 100;

    private MqttClientFactory clientFactory;

    private IMqttAsyncClient client;
    private String channelStatusTopic;
    private int flowCredit = DEFAULT_FLOW_CREDIT;


    private String channelId;

    private int queueSize;


    public MqttChannelBuilder setClientFactory(MqttClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    public MqttChannelBuilder setClient(IMqttAsyncClient client) {
        this.client = client;
        return this;
    }

    public MqttChannelBuilder setChannelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    public MqttChannelBuilder setFlowCredit(int flowCredit) {
        this.flowCredit = flowCredit;
        return this;
    }


    public MqttChannelBuilder setChannelId(String channelId) {
        this.channelId = channelId;
        return this;
    }

    public MqttChannelBuilder setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public MessageChannel build(){
        final MqttChannelConduit conduit = new MqttChannelConduit(clientFactory, client, channelStatusTopic);
        return new MessageChannel(conduit, channelId, queueSize, flowCredit);
    }
}
