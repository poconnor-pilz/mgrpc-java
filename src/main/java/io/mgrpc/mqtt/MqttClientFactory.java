package io.mgrpc.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;

public interface MqttClientFactory {

    IMqttAsyncClient createMqttClient();
}
