package io.mgrpc;


public interface MessagingListener {

    void messageArrived(String topic, byte[] buffer) throws Exception;
}
