package io.mgrpc.messaging;


public interface ChannelMessageListener {

    void onMessage(byte[] buffer);

    void onServerDisconnected();

    String getChannelId();
}
