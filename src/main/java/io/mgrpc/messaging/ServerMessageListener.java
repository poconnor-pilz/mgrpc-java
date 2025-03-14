package io.mgrpc.messaging;


import io.mgrpc.RpcMessage;

/**
 * Interface implemented by the server
 */
public interface ServerMessageListener extends DisconnectListener{

    /**
     * The ServerMessageTransport should call this method on the server when it receives
     * a request from a channel
     * @param message An rpc message containing the request from a channel.
     */
    void onMessage(RpcMessage message);

}
