package io.mgrpc;

public interface MessageChannelFactory {
    public MessageChannel createMessageChannel(String serverTopic);
}
