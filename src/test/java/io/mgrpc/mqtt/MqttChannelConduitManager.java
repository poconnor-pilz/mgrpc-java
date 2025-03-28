package io.mgrpc.mqtt;

import io.mgrpc.ChannelConduitManager;
import io.mgrpc.ConnectionStatus;
import io.mgrpc.MessageServer;
import io.mgrpc.messaging.ChannelConduit;
import io.mgrpc.messaging.ChannelListener;
import io.mgrpc.messaging.MessagingException;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MqttChannelConduitManager implements ChannelConduitManager {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MqttAsyncClient client;


    private final Map<String, ChannelConduit> conduitsByServerTopic = new ConcurrentHashMap<>();


    private static volatile Executor executorSingleton;

    @Override
    public Executor getExecutor() {
        return getExecutorInstance();
    }

    private static Executor getExecutorInstance() {
        if (executorSingleton == null) {
            synchronized (MessageServer.class) {
                if (executorSingleton == null) {
                    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum or maybe it should be passed as a constructor parameter?
                    executorSingleton = Executors.newCachedThreadPool();
                }
            }
        }
        return executorSingleton;
    }


    public MqttChannelConduitManager(MqttAsyncClient client) {
        this.client = client;
    }

    @Override
    public ChannelConduit getChannelConduitForServer(String serverTopic, ChannelListener channelListener) {

        ChannelConduit conduit;
        synchronized (conduitsByServerTopic) {
            conduit = conduitsByServerTopic.get(serverTopic);
            if (conduit == null) {
                conduit = new MqttChannelConduit(client, serverTopic);
                conduitsByServerTopic.put(serverTopic, conduit);
            }
        }
        try {
            //start should be idempotent and synchronized
            conduit.start(channelListener);
            return conduit;
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close(String channelId, String channelStatusTopic) {
        for (ChannelConduit conduit : conduitsByServerTopic.values()) {
            conduit.close();
        }
        if (channelStatusTopic != null) {
            try {
                //Notify that this client has been closed so that any server with ongoing calls can cancel them and
                //release resources.
                log.debug("Closing channel. Sending notification on " + channelStatusTopic);
                final byte[] connectedMsg = ConnectionStatus.newBuilder().
                        setConnected(false).setChannelId(channelId).build().toByteArray();
                client.publish(channelStatusTopic, new MqttMessage(connectedMsg));
            } catch (MqttException exception) {
                log.error("Exception closing " + exception);
            }

        }
    }

}
