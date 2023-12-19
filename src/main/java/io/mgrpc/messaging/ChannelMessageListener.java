package io.mgrpc.messaging;

import io.mgrpc.RpcMessage;

/**
 * Interface implemented by the channel
 */
public interface ChannelMessageListener {

    /**
     * The ChannelMessageTransport should call this method on the channel when it receives
     * a reply from the server
     * @param message The reply from the server.
     */
    void onMessage(RpcMessage message);

    /**
     * The ChannelMessageTransport should call this message on the channel if it knows that the server
     * has disconnected.
     */
    void onServerDisconnected();

    /**
     * The ChannelMessageTransport can call this method to get the channel id.
     * @return
     */
    String getChannelId();
}
