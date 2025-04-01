package io.mgrpc.messaging;

import io.mgrpc.RpcMessage;

/**
 * Interface implemented by the channel
 */
public interface ChannelListener{

    /**
     * The ChannelConduit should call this method on the channel when it receives
     * a reply from the server
     * @param message The reply from the server.
     */
    void onMessage(RpcMessage message);


    /**
     * The ChannelConduit can call this method to get the channel id.
     * @return
     */
    String getChannelId();

    /**
     * The ChannelConduit should call this message on the channel if it knows that the server
     * has disconnected.
     */
     void onServerDisconnected(String serverTopic);
}
