package io.mgrpc.mqtt;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.messaging.*;
import io.mgrpc.messaging.pubsub.BufferToStreamObserver;
import io.mgrpc.messaging.pubsub.MessagingSubscriber;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MqttChannelMessageProvider implements MessagingProvider, MessagingSubscriber {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MqttAsyncClient client;
    private final static String TOPIC_SEPARATOR = "/";
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    private final String clientId;

    private final ServerTopics serverTopics;

    private CountDownLatch serverConnectedLatch;
    private boolean serverConnected = false;

    private MessagingListener messagingListener;

    private final Map<String, List<StreamObserver>> subscribersByTopic = new ConcurrentHashMap<>();

    public static class Stats {
        private final int subscribers;

        public Stats(int subscribers) {
            this.subscribers = subscribers;
        }

        public int getSubscribers() {
            return subscribers;
        }

    }


    /**
     * @param client
     * @param serverTopic The root topic of the server to connect to e.g. "tenant1/device1"
     *                    Requests will be sent to {serverTopic}/i/svc/{service}/{method}
     *                    The channel will subscribe for replies on {serverTopic}/o/svc/{clientId}/#
     *                    The channel will receive replies to a specific call on
     *                    {serverTopic}/o/svc/{clientId}/{service}/{method}/{callId}
     * @param clientId
     */
    public MqttChannelMessageProvider(MqttAsyncClient client, String serverTopic, String clientId) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic, TOPIC_SEPARATOR);
        this.clientId = clientId;
    }

    public void close() throws MqttException {
        this.client.close();
    }


    @Override
    public void connectListener(MessagingListener listener) throws MessagingException {

        if (this.messagingListener != null) {
            throw new MessagingException("Listener already connected");
        }
        this.messagingListener = listener;
        try {
            final String replyTopicPrefix = serverTopics.servicesOutForClient(clientId) + "/#";
            log.debug("Subscribing for responses on: " + replyTopicPrefix);
            client.subscribe(replyTopicPrefix, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                listener.onMessage(mqttMessage.getPayload());
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);

            client.subscribe(serverTopics.status, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                try {
                    this.serverConnected = ConnectionStatus.parseFrom(mqttMessage.getPayload()).getConnected();
                    log.debug("Server connected status = " + serverConnected);
                    this.serverConnectedLatch.countDown();
                    if (!serverConnected) {
                        listener.onCounterpartDisconnected(null);
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Failed to parse connection status", e);
                    return;
                }
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);

            pingServer();

        } catch (MqttException ex) {
            throw new MessagingException(ex);
        }

    }

    @Override
    public void disconnectListener() {
        try {
            //Notify that this client has been closed so that any server with ongoing calls can cancel them and
            //release resources.
            String statusTopic = ServerTopics.make(TOPIC_SEPARATOR, serverTopics.statusClients, this.clientId);
            log.debug("Closing channel. Sending notification on " + statusTopic);
            final byte[] connectedMsg = ConnectionStatus.newBuilder().setConnected(false).build().toByteArray();
            client.publish(statusTopic, new MqttMessage(connectedMsg));
            final String replyTopicPrefix = serverTopics.servicesOutForClient(clientId) + "/#";
            client.unsubscribe(replyTopicPrefix);
            client.unsubscribe(serverTopics.status);
        } catch (MqttException exception) {
            log.error("Exception closing " + exception);
        }
    }

    @Override
    public void send(String clientId, String methodName, byte[] buffer) throws MessagingException {
        if (!serverConnected) {
            //The server should have an mqtt LWT that reliably sends a message when it is disconnected.
            //Nevertheless send it a ping to make double sure that it is definitely not connected.
            pingServer();
            try {
                serverConnectedLatch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("", e);
            }
            if (!serverConnected) {
                log.warn("Tried to send message but server is not connected");
                throw new MessagingException("Server is not connected");
            }
        }
        final String topic = serverTopics.methodIn(methodName);
        try {
            client.publish(topic, new MqttMessage(buffer));
        } catch (MqttException e) {
            log.error("Failed to send mqtt message", e);
            throw new MessagingException(e);
        }
    }

    /**
     * Send the server an empty message to the prompt topic to prompt it so send back its status
     * This will be handled in the client.subscribe(Topics.statusOut(serverTopic) set up in this.connectListener
     */
    private void pingServer() {
        this.serverConnectedLatch = new CountDownLatch(1);
        //In case the server is already started, prompt it to send its connection status
        //If it is not started it will send connection status when it does start.
        try {
            log.debug("Pinging server for status at: " + serverTopics.statusPrompt);
            client.publish(serverTopics.statusPrompt, new MqttMessage(new byte[0]));
        } catch (MqttException e) {
            log.error("Failed to ping server", e);
        }
    }

    /**
     * Some tests will not use a real server that responds to pings.
     * The channel will fail to send messages if it thinks that a server is not connected.
     * This method will fool the channel into thinking that a real server is connected.
     */
    public void fakeServerConnectedForTests() {
        this.serverConnected = true;
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
                remove = BufferToStreamObserver.convert(parser, mqttMessage.getPayload(), observer);
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
            throw new MessagingException("Subscription failed",e);
        }

    }

    @Override
    public void unsubscribe(String responseTopic) throws MessagingException{
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
    public void unsubscribe(String responseTopic, StreamObserver observer) throws MessagingException{
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
