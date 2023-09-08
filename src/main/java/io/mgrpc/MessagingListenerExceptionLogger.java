package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;


public class MessagingListenerExceptionLogger implements MessagingListener {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final MessagingListener inner;

    public MessagingListenerExceptionLogger(MessagingListener inner) {
        this.inner = inner;
    }

    @Override
    public void onMessage(String topic, byte[] buffer) {

        //If an exception is thrown in messageArrived of IMqttMessageListener
        //the calling code will close the mqtt connection.
        //Instead just trap the exception here and log it
        try {
            inner.onMessage(topic, buffer);
        } catch (Throwable t) {
            log.error("Exception occurred in MessageListener", t);
        }
    }


    @Override
    public void onCounterpartDisconnected(String clientId) {
        log.error("This shouldn't be called");
    }
}
