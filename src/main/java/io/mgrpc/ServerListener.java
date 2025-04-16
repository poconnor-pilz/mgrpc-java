package io.mgrpc;


/**
 * Interface implemented by the server
 */
public interface ServerListener{

    /**
     * The ServerConduit should call this method on the server when it receives
     * a request from a channel
     * @param message An rpc message containing the request from a channel.
     */
    void onMessage(RpcMessage message);

    /**
     * The ServerConduit should call this message on the server if it knows that a channel
     * has disconnected.
     */
    void onChannelDisconnected(String channelId);

}
