package io.mgrpc.messaging;

public interface DisconnectListener {

    /**
     * Notify that a channel or server has disconnected.
     * If it is a server that has disconnected then the channelId will be null
     */
    void onDisconnect(String channelId);

}
