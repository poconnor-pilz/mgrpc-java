package io.mgrpc;

public class MessageChannelBuilder {
    private ChannelConduitManager conduitManager;
    private String channelId;
    private int queueSize;
    private String channelStatusTopic;

    public MessageChannelBuilder conduitManager(ChannelConduitManager conduitManager) {
        this.conduitManager = conduitManager;
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

    public MessageChannelBuilder channelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    public MessageChannel build() {
        return new MessageChannel(conduitManager, channelId, queueSize, channelStatusTopic);
    }
}