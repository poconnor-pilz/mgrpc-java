package io.mgrpc.jms;

import io.mgrpc.ConnectionStatus;
import io.mgrpc.MessageServer;
import io.mgrpc.ServerTopics;
import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.ServerMessageListener;
import io.mgrpc.messaging.ServerMessageTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class JmsServerTransport implements ServerMessageTransport {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Connection client;

    private Session session;

    private final static String TOPIC_SEPARATOR = "/";
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    private final Map<String, MessageProducer> channelProducers = new ConcurrentHashMap<>();

    private final ServerTopics serverTopics;

    private static volatile Executor executorSingleton;

    private ServerMessageListener server;

    /**
     * @param client
     * @param serverTopic       The root topic of the server e.g. "tenant1/device1"
     *                          The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                          A request for a method should be sent to sent to {serverTopic}/i/svc
     *                          Replies will be sent to {serverTopic}/o/svc/{channelId}}
     *
     */
    public JmsServerTransport(Connection client, String serverTopic) {
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

            session = client.createSession();

            Topic inTopic = session.createTopic(serverTopics.servicesIn);
            log.debug("Subscribing for requests on : " + inTopic.getTopicName());

            MessageConsumer consumer = session.createConsumer(inTopic);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        server.onMessage(JmsUtils.byteArrayFromMessage(session, message));
                    } catch (Exception ex){
                        log.error("Failed to process request", ex);
                    }
                }
            });

            Topic statusTopic = session.createTopic(serverTopics.statusClients);
            log.debug("Subscribing for channel status on: " + statusTopic.getTopicName());
            MessageConsumer statusConsumer = session.createConsumer(statusTopic);
            statusConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        ConnectionStatus connectionStatus = ConnectionStatus.parseFrom(JmsUtils.byteArrayFromMessage(session, message));
                        log.debug("Received client connected status = " + connectionStatus.getConnected() + " for channel " + connectionStatus.getChannelId());
                        if(!connectionStatus.getConnected()) {
                            server.onChannelDisconnected(connectionStatus.getChannelId());
                            channelProducers.remove(connectionStatus.getChannelId());
                        }
                    } catch (Exception ex){
                        log.error("Failed to process status reply", ex);
                    }
                }
            });

            Topic pingTopic = session.createTopic(serverTopics.statusPrompt);
            log.debug("Subscribing for pings on : " + pingTopic.getTopicName());
            MessageConsumer pingConsumer = session.createConsumer(pingTopic);
            pingConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        log.debug("Received ping");
                        notifyConnected(true);
                    } catch (Exception ex){
                        log.error("Failed to process status reply", ex);
                    }
                }
            });


            notifyConnected(true);
        } catch (JMSException ex) {
            throw new MessagingException(ex);
        }
    }

    @Override
    public void close(){
        try {
            notifyConnected(false);
            session.close();
        } catch (JMSException exception) {
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
            MessageProducer channelProducer = channelProducers.get(channelId);
            if(channelProducer == null){
                final Topic replyTopic = session.createTopic(serverTopics.servicesOutForChannel(channelId));
                channelProducer = session.createProducer(replyTopic);
                channelProducers.put(channelId, channelProducer);
            }
            channelProducer.send(JmsUtils.messageFromByteArray(session, buffer));
        } catch (JMSException e) {
            log.error("Failed to send mqtt message", e);
            throw new MessagingException(e);
        }
    }

    private void notifyConnected(boolean connected) {
        //Notify any clients that the server has been connected
        final byte[] connectedMsg = ConnectionStatus.newBuilder().setConnected(connected).build().toByteArray();
        try {
            log.debug("Sending connected status " + connected);
            Topic topic = session.createTopic(serverTopics.status);
            MessageProducer producer = session.createProducer(topic);
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(connectedMsg);
            producer.send(bytesMessage);
        } catch (JMSException e) {
            log.error("Failed to notify connected", e);
        }
    }

}
