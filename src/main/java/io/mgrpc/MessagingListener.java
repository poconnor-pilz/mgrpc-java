package io.mgrpc;


public interface MessagingListener {

    void onMessage(String topic, byte[] buffer) throws Exception;

    void onCounterpartDisconnected(String clientId);
}
