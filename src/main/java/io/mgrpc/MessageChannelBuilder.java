package io.mgrpc;

import io.mgrpc.messaging.ChannelConduit;

public class MessageChannelBuilder {
    private ChannelConduit conduit;
    private String channelId;
    private int queueSize;

    public MessageChannelBuilder conduit(ChannelConduit conduit) {
        this.conduit = conduit;
        return this;
    }

    public MessageChannelBuilder channelId(String channelId) {
        this.channelId = channelId;
        return this;
    }

    public MessageChannelBuilder queueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }


    public MessageChannel build() {
        return new MessageChannel(conduit, channelId, queueSize);
    }
}