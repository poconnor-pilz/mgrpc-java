package com.pilz.utils;

import com.pilz.mqttgrpc.ConnectionStatus;
import io.moquette.broker.Server;
import io.moquette.broker.config.ClasspathResourceLoader;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.IResourceLoader;
import io.moquette.broker.config.ResourceLoaderConfig;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MqttUtils {

    static class PublisherListener extends AbstractInterceptHandler {

        @Override
        public String getID() {
            return "EmbeddedLauncherPublishListener";
        }

        @Override
        public void onPublish(InterceptPublishMessage msg) {
            final String decodedPayload = msg.getPayload().toString(UTF_8);
            System.out.println("Received on topic: " + msg.getTopicName() + " content: " + decodedPayload);
        }
    }

    private static Server mqttBroker;

    public static void startEmbeddedBroker() throws IOException {
        IResourceLoader classpathLoader = new ClasspathResourceLoader();
        final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);

        mqttBroker = new Server();
        List<? extends InterceptHandler> userHandlers = null;
        //Include this line to see all publishes to the broker
        //userHandlers = Collections.singletonList(new PublisherListener());
        mqttBroker.startServer(classPathConfig, userHandlers);
    }

    public static void stopEmbeddedBroker(){
        mqttBroker.stopServer();
    }

    public static MqttAsyncClient makeClient(String lwtTopic) throws MqttException {
        final MqttAsyncClient client;
        client = new MqttAsyncClient(
                "tcp://localhost:1884",
                MqttAsyncClient.generateClientId(),
                new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();

        if(lwtTopic != null){
            final byte[] lwtMessage = ConnectionStatus.newBuilder().setConnected(false).build().toByteArray();
            mqttConnectOptions.setWill(lwtTopic, lwtMessage, 1, false);
        }
        client.connect(mqttConnectOptions).waitForCompletion();
        return client;
    }

}
