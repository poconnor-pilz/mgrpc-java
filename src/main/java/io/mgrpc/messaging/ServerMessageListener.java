package io.mgrpc.messaging;


public interface ServerMessageListener {

    void onMessage(byte[] buffer);

    void onChannelDisconnected(String channelId);
}
