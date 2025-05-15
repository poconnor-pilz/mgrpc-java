package io.mgrpc.proxyserver;

import io.mgrpc.mqtt.MqttClientFactory;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

class ClientFactory implements MqttClientFactory {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String brokerUrl;

    ClientFactory(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    @Override
    public IMqttAsyncClient createMqttClient() {
        try {
            final MqttAsyncClient client;
            final String clientId = MqttAsyncClient.generateClientId();
            client = new MqttAsyncClient(
                    brokerUrl,
                    clientId,
                    new MemoryPersistence());
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions.setMaxInflight(1000);
            client.connect(mqttConnectOptions).waitForCompletion();
            log.debug("Created MQTT client {}", clientId);
            return client;
        } catch (Exception e) {
            log.error("Failed to create client", e);
            return null;
        }
    }
}
