package io.mgrpc.mqtt;

import com.google.protobuf.Parser;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.messaging.ChannelConduit;
import io.mgrpc.messaging.ChannelTopicConduit;
import io.mgrpc.messaging.ChannelListener;
import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.pubsub.BufferToStreamObserver;
import io.mgrpc.messaging.pubsub.MessageSubscriber;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttChannelConduit implements ChannelConduit, MessageSubscriber {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MqttAsyncClient client;

    private final String channelStatusTopic;


    private final Map<String, ChannelTopicConduit> conduitsByServerTopic = new ConcurrentHashMap<>();

    private final Map<String, List<StreamObserver>> subscribersByTopic = new ConcurrentHashMap<>();


    private static volatile Executor executorSingleton;

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
     * @param client The Mqtt client
     * @param channelStatusTopic The topic on which messages regarding the channel status will be published.
     *                           (For MQTT this topic will be the same topic as the MQTT LWT for the channel client)
     *                           If this value is null then the conduit will not attempt to publish
     *                           channel status messages.
     **/
    public MqttChannelConduit(MqttAsyncClient client, String channelStatusTopic) {
        this.client = client;
        this.channelStatusTopic = channelStatusTopic;
    }

    /**
     * @param client The Mqtt client
     **/
    public MqttChannelConduit(MqttAsyncClient client) {
        this(client, null);
    }

    @Override
    public ChannelTopicConduit getChannelTopicConduit(String serverTopic, ChannelListener channelListener) {

        ChannelTopicConduit conduit;
        synchronized (conduitsByServerTopic) {
            conduit = conduitsByServerTopic.get(serverTopic);
            if (conduit == null) {
                conduit = new MqttChannelTopicConduit(client, serverTopic);
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
    public void close(String channelId) {
        for (ChannelTopicConduit conduit : conduitsByServerTopic.values()) {
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



    @Override
    public <T> void subscribe(String responseTopic, Parser<T> parser, StreamObserver<T> streamObserver) throws MessagingException {
        List<StreamObserver> subscribers = subscribersByTopic.get(responseTopic);
        if (subscribers != null) {
            subscribers.add(streamObserver);
            return;
        }

        final MqttExceptionLogger messageListener = new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
            final List<StreamObserver> observers = subscribersByTopic.get(topic);
            if (observers == null) {
                //We should not receive any messages if there are no subscribers
                log.warn("No subscribers for " + topic);
                return;
            }
            boolean remove = false;
            for (StreamObserver observer : observers) {
                final RpcSet rpcSet = RpcSet.parseFrom(mqttMessage.getPayload());
                for(RpcMessage message: rpcSet.getMessagesList()) {
                    remove = BufferToStreamObserver.convert(parser, message, observer);
                }
            }
            if (remove) {
                subscribersByTopic.remove(topic);
                client.unsubscribe(topic);
            }
        });

        try {
            client.subscribe(responseTopic, 1, messageListener);
            if (subscribers == null) {
                subscribers = new ArrayList<>();
                subscribersByTopic.put(responseTopic, subscribers);
            }
            subscribers.add(streamObserver);
        } catch (MqttException e) {
            throw new MessagingException("Subscription failed", e);
        }

    }

    @Override
    public void unsubscribe(String responseTopic) throws MessagingException {
        final List<StreamObserver> observers = subscribersByTopic.get(responseTopic);
        if (observers != null) {
            subscribersByTopic.remove(responseTopic);
            try {
                client.unsubscribe(responseTopic);
            } catch (MqttException e) {
                log.error("Failed to unsubscribe for " + responseTopic, e);
                throw new MessagingException(e);
            }
        } else {
            log.warn("No subscription found for responseTopic: " + responseTopic);
        }
    }

    @Override
    public void unsubscribe(String responseTopic, StreamObserver observer) throws MessagingException {
        final List<StreamObserver> observers = subscribersByTopic.get(responseTopic);
        if (observers != null) {
            if (!observers.remove(observer)) {
                log.warn("Observer not found");
            }
            if (observers.isEmpty()) {
                try {
                    client.unsubscribe(responseTopic);
                } catch (MqttException e) {
                    log.error("Failed to unsubscribe for " + responseTopic, e);
                    throw new MessagingException(e);
                }
            }
        }
    }


    public MqttChannelTopicConduit.Stats getStats() {

        int subscribers = 0;
        final Set<String> topics = subscribersByTopic.keySet();
        for (String topic : topics) {
            subscribers += subscribersByTopic.get(topic).size();
        }

        return new MqttChannelTopicConduit.Stats(subscribers);
    }

}
