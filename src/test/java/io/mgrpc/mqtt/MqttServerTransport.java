package io.mgrpc.mqtt;

import io.mgrpc.ConnectionStatus;
import io.mgrpc.MessageServer;
import io.mgrpc.ServerTopics;
import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.ServerMessageListener;
import io.mgrpc.messaging.ServerMessageTransport;
import io.mgrpc.messaging.pubsub.MessagePublisher;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MqttServerTransport implements ServerMessageTransport, MessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MqttAsyncClient client;
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;


    private final ServerTopics serverTopics;

    private static volatile Executor executorSingleton;

    private ServerMessageListener server;

    /**
     * @param client
     * @param serverTopic       The root topic of the server e.g. "tenant1/device1"
     *                          This topic should be unique to the broker.
     *                          The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                          A request for a method should be sent to sent to {serverTopic}/i/svc/{slashedFullMethod}
     *                          Replies will be sent to {serverTopic}/o/svc/{channelId}/{slashedFullMethod}
     *                          Where if the gRPC fullMethodName is "helloworld.HelloService/SayHello"
     *                          then {slashedFullMethod} is "helloworld/HelloService/SayHello"
     *
     */
    public MqttServerTransport(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic);
    }



    @Override
    public void start(MessageServer server) throws MessagingException {
        if (this.server != null) {
            throw new MessagingException("Listener already connected");
        }
        this.server = server;
        try {
            //Subscribe for gRPC messages
            String inTopicFilter = serverTopics.servicesIn + "/#";
            log.debug("Subscribing for requests on " + inTopicFilter);
            client.subscribe(inTopicFilter, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                server.onMessage(mqttMessage.getPayload());
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);


            //Subscribe for channel status messages
            log.debug("Subscribing for client status on " + serverTopics.statusClients);
            client.subscribe(serverTopics.statusClients, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                ConnectionStatus connectionStatus = ConnectionStatus.parseFrom(mqttMessage.getPayload());
                log.debug("Received client connected status = " + connectionStatus.getConnected() + " on " + topic + " for channel " + connectionStatus.getChannelId());
                if(!connectionStatus.getConnected()) {
                    server.onChannelDisconnected(connectionStatus.getChannelId());
                }
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);

            //If this receives a ping from a client then send a notification that we are connected
            log.debug("Subscribing for client pings on " + serverTopics.statusPrompt);
            client.subscribe(serverTopics.statusPrompt, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
               notifyConnected(true);
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);

            notifyConnected(true);
        } catch (MqttException ex) {
            throw new MessagingException(ex);
        }
    }

    @Override
    public void close(){
        try {
            notifyConnected(false);
            client.unsubscribe(serverTopics.servicesIn + "/#");
            client.unsubscribe(serverTopics.statusClients + "/#");
            client.unsubscribe(serverTopics.statusPrompt);
        } catch (MqttException exception) {
            log.error("Exception closing " + exception);
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
                    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum or maybe it should be passed as a constructor parameter?
                    executorSingleton = Executors.newCachedThreadPool();
                }
            }
        }
        return executorSingleton;
    }


    @Override
    public void send(String channelId, String methodName, byte[] buffer) throws MessagingException {

        try {
            client.publish(serverTopics.replyTopic(channelId, methodName), new MqttMessage(buffer));
        } catch (MqttException e) {
            log.error("Failed to send mqtt message", e);
            throw new MessagingException(e);
        }
    }

    private void notifyConnected(boolean connected) {
        //Notify any clients that the server has been connected
        final byte[] connectedMsg = ConnectionStatus.newBuilder().setConnected(connected).build().toByteArray();
        try {
            client.publish(serverTopics.status, new MqttMessage(connectedMsg));
        } catch (MqttException e) {
            log.error("Failed to notify connected", e);
        }
    }


    @Override
    public void publish(String topic, byte[] buffer) throws MessagingException {
        try {
            client.publish(topic, new MqttMessage(buffer));
        } catch (MqttException e) {
            log.error("Failed to send mqtt message", e);
            throw new MessagingException(e);
        }
    }
}
