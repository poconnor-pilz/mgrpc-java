package io.mgrpc.mqtt;

import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;

public class MqttChannelBuilder {


    private MqttClientFactory clientFactory;

    private IMqttAsyncClient client;
    private String channelStatusTopic;
    private int creditSize = MessageServer.DEFAULT_CREDIT_SIZE;

    private String channelId;

    private int queueSize = MessageChannel.DEFAULT_QUEUE_SIZE;


    /**
     * @param clientFactory The Mqtt client factory used for creating mqtt clients
     *                      for the channel. If this is not specified then you must
     *                      use setClient to set a single client
     */
    public MqttChannelBuilder setClientFactory(MqttClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    /**
     * @param client The mqtt client. If this is not specified then you must set a client factory
     * @return
     */
    public MqttChannelBuilder setClient(IMqttAsyncClient client) {
        this.client = client;
        return this;
    }

    /**
     * @param channelStatusTopic The topic on which messages regarding the channel status will be published.
     *                           (For MQTT this topic will be the same topic as the MQTT LWT for the channel client)
     *                           If this value is null then the conduit will not attempt to publish
     *                           channel status messages.
     */
    public MqttChannelBuilder setChannelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    /**
     * @param creditSize The amount of credit that should be issued for flow control e.g. if creditSize is 20
     *      then the sender will only send 20 messages before waiting for the receiver to send more credit.
     */
    public MqttChannelBuilder setCreditSize(int creditSize) {
        this.creditSize = creditSize;
        return this;
    }

    /**
     * @param channelId The client id for the channel. Should be unique.
     */
    public MqttChannelBuilder setChannelId(String channelId) {
        this.channelId = channelId;
        return this;
    }

    /**
     * @param queueSize  The size of the internal in-memory message queue for each call's replies
     */
    public MqttChannelBuilder setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public MessageChannel build(){
        final MqttChannelConduit conduit = new MqttChannelConduit(clientFactory, client, channelStatusTopic);
        return new MessageChannel(conduit, channelId, queueSize, creditSize);
    }
}
