package io.mgrpc.mqtt;

import io.mgrpc.*;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttChannelConduit implements ChannelConduit {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private final String channelStatusTopic;


    private final Map<String, TopicConduit> conduitsByServerTopic = new ConcurrentHashMap<>();

    private final MqttTopicConduitManager topicConduitManager;


    private final IMqttAsyncClient client;


    private static volatile Executor executorSingleton;

    public static class Stats {
        private final int numClients;

        public Stats(int numClients) {
            this.numClients = numClients;
        }

        public int getNumClients() {
            return numClients;
        }

    }


    @Override
    public Executor getExecutor() {
        return getExecutorInstance();
    }

    private static Executor getExecutorInstance() {
        if (executorSingleton == null) {
            synchronized (MessageServer.class) {
                if (executorSingleton == null) {
                    //Note that the default exector for grpc classic is a cached thread pool.
                    //The cached thread pool will retire threads that are not used for 60 seconds but otherwise
                    //create, cache and re-use threads as needed.
                    executorSingleton = Executors.newCachedThreadPool(new ThreadFactory() {
                        private final AtomicInteger threadNumber = new AtomicInteger(1);
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "mgrpc-channel-" + threadNumber.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });
                }
            }
        }
        return executorSingleton;
    }


    /**
     * @param clientFactory The Mqtt client factory
     * @param channelStatusTopic The topic on which messages regarding the channel status will be published.
     *                           (For MQTT this topic will be the same topic as the MQTT LWT for the channel client)
     *                           If this value is null then the conduit will not attempt to publish
     *                           channel status messages.
     **/
    public MqttChannelConduit(MqttClientFactory clientFactory, String channelStatusTopic) {
        this.topicConduitManager = new MqttTopicConduitManager(clientFactory);
        this.client = topicConduitManager.makeMainClient();
        this.channelStatusTopic = channelStatusTopic;
    }

    /**
     * @param clientFactory The Mqtt client factory
     **/
    public MqttChannelConduit(MqttClientFactory clientFactory) {
        this(clientFactory, null);
    }

    public MqttChannelConduit(IMqttAsyncClient client, String channelStatusTopic) {

        this.client = client;
        this.topicConduitManager = new MqttTopicConduitManager(new MqttClientFactory() {
            @Override
            public IMqttAsyncClient createMqttClient() {
                return client;
            }
        });
        this.channelStatusTopic = channelStatusTopic;
    }

    public MqttChannelConduit(IMqttAsyncClient client) {
        this(client, null);
    }

    @Override
    public TopicConduit getTopicConduit(String serverTopic, ChannelListener channelListener) {

        final TopicConduit topicConduit = this.topicConduitManager.getTopicConduit(serverTopic, channelListener);

        try {
            //start should be idempotent and synchronized
            topicConduit.start(channelListener);
            return topicConduit;
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close(String channelId) {
        for (TopicConduit conduit : conduitsByServerTopic.values()) {
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




    public Stats getStats() {

        return new Stats(topicConduitManager.limitedClients.size());
    }

}
