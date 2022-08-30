package com.pilz.mqttgrpc;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MqttExceptionLogger implements IMqttMessageListener {

    private static Logger log = LoggerFactory.getLogger(MqttExceptionLogger.class);
    private final IMqttMessageListener inner;

    public MqttExceptionLogger(IMqttMessageListener inner) {
        this.inner = inner;
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {

        //If an exception is thrown in messageArrived of IMqttMessageListener
        //the calling code will close the mqtt connection.
        //Instead just trap the exception here and log it
        try {
            inner.messageArrived(topic, message);
        } catch (Throwable t) {
            log.error("Exception occurred in IMqttMessageListener", t);
        }
    }
}
