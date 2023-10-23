package io.mgrpc.mqtt;

import io.mgrpc.ConnectionStatus;
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

public class MqttServerTransport implements ServerMessageTransport, MessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MqttAsyncClient client;
    private final static String TOPIC_SEPARATOR = "/";
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;


    private final ServerTopics serverTopics;


    private ServerMessageListener server;

    /**
     * @param client
     * @param serverTopic       The root topic of the server e.g. "tenant1/device1"
     *                          The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                          A request for a method should be sent to sent to {serverTopic}/i/svc/{service}/{method}
     *                          Replies will be sent to whatever the client specifies in the message header's replyTo
     *                          This will normally be:
     *                          {serverTopic}/o/svc/{channelId}/{service}/{method}/{callId}
     */
    public MqttServerTransport(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic, TOPIC_SEPARATOR);
    }



    @Override
    public void start(ServerMessageListener server) throws MessagingException {
        if (this.server != null) {
            throw new MessagingException("Listener already connected");
        }
        this.server = server;
        try {
            client.subscribe(serverTopics.servicesIn + "/#", 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                server.onMessage(mqttMessage.getPayload());
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);

            client.subscribe(serverTopics.statusClients + "/#", 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                //If the client sends any message to this topic it means that it has disconnected
                //The client will send the message to {serverTopic}/in/sys/status/client/{channelId}
                //So we need to parse the channelId from the topic
                log.debug("Received client connected status on " + topic);
                boolean connected = ConnectionStatus.parseFrom(mqttMessage.getPayload()).getConnected();
                String channelId = topic.substring(topic.lastIndexOf(TOPIC_SEPARATOR) + TOPIC_SEPARATOR.length());
                log.debug("Received client connected status = " + connected + " on " + topic + " for client " + channelId);
                if(!connected) {
                    server.onChannelDisconnected(channelId);
                }
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);

            //If this receives a ping from a client then send a notification that we are connected
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

    /**
     * @return The MQTT last will and testament topic. This topic should be used to configure the MQTT connection
     */
    public static String getLWTTopic(String serverTopic){
        return new ServerTopics(serverTopic, TOPIC_SEPARATOR).status;
    }

    @Override
    public void send(String channelId, String methodName, byte[] buffer) throws MessagingException {

        final String replyTopicPrefix = serverTopics.servicesOutForChannel(channelId);
        final String topic = ServerTopics.replyTopic(replyTopicPrefix, TOPIC_SEPARATOR, methodName);
        try {
            client.publish(topic, new MqttMessage(buffer));
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
