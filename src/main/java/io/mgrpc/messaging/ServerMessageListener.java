package io.mgrpc.messaging;


/**
 * Interface implemented by the server
 */
public interface ServerMessageListener {

    /**
     * The ServerMessageTransport should call this method on the server when it receives
     * a request from a channel
     * @param buffer A protocol buffer containing the reply from the server.
     */
    void onMessage(byte[] buffer);

    /**
     * The ServerMessageTransport should call this message on the serer if it knows that a channel
     * has disconnected.
     */
    void onChannelDisconnected(String channelId);
}
