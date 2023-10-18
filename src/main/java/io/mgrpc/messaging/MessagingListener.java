package io.mgrpc.messaging;


public interface MessagingListener {

    void onMessage(byte[] buffer);

    void onCounterpartDisconnected(String clientId);
}
