package io.mgrpc.messaging;

import io.mgrpc.RpcMessageHandler;

/**
 * Interface implemented by the channel
 */
public interface ChannelMessageListener extends RpcMessageHandler {


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
