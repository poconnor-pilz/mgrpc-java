package io.mgrpc.messaging;

/**
 * Interface implemented by the channel
 */
public interface ChannelMessageListener {

    /**
     * The ChannelMessageTransport should call this method on the channel when it receives
     * a reply from the server
     * @param buffer A protocol buffer containing the reply from the server.
     */
    void onMessage(byte[] buffer);

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
