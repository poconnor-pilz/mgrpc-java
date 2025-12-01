package io.mgrpc.mqtt;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttStreamPubSub implements StreamPubSub {
    private static volatile Executor executorSingleton;

    public static final int QUEUE_SIZE = 100;

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


    private static class ObserverProcessor{
        public final StreamObserver observer;
        public final MessageProcessor processor;


        public ObserverProcessor(StreamObserver observer, Parser parser) {
            this.observer = observer;
            this.processor = new MessageProcessor(getExecutorInstance(), QUEUE_SIZE, new MessageProcessor.MessageHandler() {
                @Override
                public void onProviderMessage(RpcMessage message) {
                    BufferToStreamObserver.convert(parser, message, observer );
                }

                @Override
                public void onQueueCapacityExceeded() {
                    observer.onError(new Throwable("MessageProcessor queue capacity exceeded."));
                }
            }, 0,"pubsub");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Map<String, List<ObserverProcessor>> subscribersByTopic = new ConcurrentHashMap<>();

    private final IMqttAsyncClient client;

    public MqttStreamPubSub(IMqttAsyncClient client) {
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
    public <T> StreamObserver<T> getPublisher(String topic) {
        return new StreamObserver<T>() {

            private int sequence = 1;
            @Override
            public void onNext(T value) {
                if(!(value instanceof MessageLite)) {
                    throw new RuntimeException("Invalid message type: " + value.getClass());
                }
                RpcMessage rpcMessage = RpcMessage.newBuilder()
                        .setValue(Value.newBuilder().setContents(((MessageLite)value).toByteString()))
                        .setCallId("publish")
                        .setSequence(sequence++).build();
                final RpcSet rpcSet = RpcSet.newBuilder().addMessages(rpcMessage).build();
                try {
                    client.publish(topic, new MqttMessage(rpcSet.toByteArray()));
                } catch (MqttException e) {
                    log.error("Failed to send mqtt message", e);
                    throw new RuntimeException(e); //TODO: should this be a checked exception e.g. MessagingException?
                }
            }

            @Override
            public void onError(Throwable t) {
                sendStatus(Status.INTERNAL.withDescription(t.getMessage()));
            }

            @Override
            public void onCompleted() {
                sendStatus(Status.OK);
            }

            private void sendStatus(Status status){
                final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
                RpcMessage rpcMessage = RpcMessage.newBuilder()
                        .setStatus(grpcStatus)
                        .setCallId("publish")
                        .setSequence(sequence++)
                        .build();
                final RpcSet rpcSet = RpcSet.newBuilder().addMessages(rpcMessage).build();
                try {
                    client.publish(topic, new MqttMessage(rpcSet.toByteArray()));
                } catch (MqttException e) {
                    log.error("Failed to send mqtt message", e);
                    throw new RuntimeException(e); //TODO: should this be a checked exception e.g. MessagingException?
                }
            }
        };
    }

    @Override
    public <T> void subscribe(String topic, Parser<T> parser, StreamObserver<T> streamObserver) throws MessagingException {
        List<ObserverProcessor> subscribers = subscribersByTopic.get(topic);
        if (subscribers != null) {
            subscribers.add(new ObserverProcessor(streamObserver, parser));
            return;
        }

        final MqttExceptionLogger messageListener = new MqttExceptionLogger((String mqtttopic, MqttMessage mqttMessage) -> {
            final List<ObserverProcessor> observers = subscribersByTopic.get(topic);
            if (observers == null) {
                //We should not receive any messages if there are no subscribers
                log.error("No subscribers for " + topic);
                return;
            }
            boolean remove = false;
            final RpcSet rpcSet = RpcSet.parseFrom(mqttMessage.getPayload());
            for (ObserverProcessor observer : observers) {
                for(RpcMessage message: rpcSet.getMessagesList()) {
                    observer.processor.request(1);
                    observer.processor.queueMessage(message);
                }
            }
        });

        try {
            client.subscribe(topic, 1, messageListener);
            if (subscribers == null) {
                subscribers = new ArrayList<>();
                subscribersByTopic.put(topic, subscribers);
            }
            subscribers.add(new ObserverProcessor(streamObserver, parser));
        } catch (MqttException e) {
            throw new MessagingException("Subscription failed", e);
        }

    }

    @Override
    public void unsubscribe(String topic) throws MessagingException {
        final List<ObserverProcessor> observers = subscribersByTopic.get(topic);
        if (observers != null) {
            subscribersByTopic.remove(topic);
            try {
                client.unsubscribe(topic);
            } catch (MqttException e) {
                log.error("Failed to unsubscribe for " + topic, e);
                throw new MessagingException(e);
            }
        } else {
            log.warn("No subscription found for responseTopic: " + topic);
        }
    }

    @Override
    public void unsubscribe(String topic, StreamObserver observer) throws MessagingException {
        final List<ObserverProcessor> observers = subscribersByTopic.get(topic);
        if (observers != null) {
            observers.removeIf(o -> o.observer == observer);
            if (observers.isEmpty()) {
                try {
                    client.unsubscribe(topic);
                } catch (MqttException e) {
                    log.error("Failed to unsubscribe for " + topic, e);
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
