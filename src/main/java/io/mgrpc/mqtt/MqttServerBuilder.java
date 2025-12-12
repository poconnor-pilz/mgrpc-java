package io.mgrpc.mqtt;


import io.mgrpc.MessageServer;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;

public class MqttServerBuilder {

    private static final int DEFAULT_FLOW_CREDIT = 100;


    private IMqttAsyncClient client;
    private String channelStatusTopic;
    private int flowCredit = DEFAULT_FLOW_CREDIT;


    private String topic;

    private int queueSize;



    public MqttServerBuilder setClient(IMqttAsyncClient client) {
        this.client = client;
        return this;
    }

    public MqttServerBuilder setTopic(String topic) {
        this.topic = topic;
        return this;
    }


    public MqttServerBuilder setChannelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    public MqttServerBuilder setFlowCredit(int flowCredit) {
        this.flowCredit = flowCredit;
        return this;
    }


    public MqttServerBuilder setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public MessageServer build(){
        final MqttServerConduit conduit = new MqttServerConduit(client, topic, channelStatusTopic, flowCredit);
        return new MessageServer(conduit, queueSize);
    }
}
