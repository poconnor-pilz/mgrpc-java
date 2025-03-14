package io.mgrpc;

import io.grpc.ManagedChannel;

public interface MessageChannelFactory {
    public MessageChannel createMessageChannel(String serverTopic);
}
