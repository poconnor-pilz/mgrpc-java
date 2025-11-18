package io.mgrpc.mqtt;

import com.google.protobuf.Parser;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
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

public class MqttSubscriber implements MessageSubscriber {


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Map<String, List<StreamObserver>> subscribersByTopic = new ConcurrentHashMap<>();

    private final IMqttAsyncClient client;

    public MqttSubscriber(IMqttAsyncClient client) {
        this.client = client;
    }


    public static class Stats {
        private final int subscribers;

        public Stats(int subscribers) {
            this.subscribers = subscribers;
        }

        public int getSubscribers() {
            return subscribers;
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


    public Stats getStats() {
        int subscribers = 0;
        final Set<String> topics = subscribersByTopic.keySet();
        for (String topic : topics) {
            subscribers += subscribersByTopic.get(topic).size();
        }
        return new Stats(subscribers);
    }

}
