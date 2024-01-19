package io.mgrpc.messaging;


import io.mgrpc.RpcMessageHandler;

/**
 * Interface implemented by the server
 */
public interface ServerMessageListener extends RpcMessageHandler {

    /**
     * The ServerMessageTransport should call this message on the serer if it knows that a channel
     * has disconnected.
     */
    void onChannelDisconnected(String channelId);
}
