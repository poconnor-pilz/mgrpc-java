package io.mgrpc.jms;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
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

    private Map<String, JmsCallQueues> callQueuesMap = new ConcurrentHashMap<>();

    private final ServerTopics serverTopics;

    private static volatile Executor executorSingleton;

    private MessageServer server;

    /**
     * @param client
     * @param serverTopic The root topic of the server e.g. "tenant1/device1"
     *                    The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                    A request for a method should be sent to sent to {serverTopic}/i/svc
     *                    Replies will be sent to {serverTopic}/o/svc/{channelId}}
     */
    public JmsServerTransport(Connection client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic, TOPIC_SEPARATOR);
    }


    @Override
    public void start(MessageServer server) throws MessagingException {
        if (this.server != null) {
            throw new MessagingException("Listener already connected");
        }
        this.server = server;
        try {

            session = client.createSession();

            Queue inQueue = session.createQueue(serverTopics.servicesIn);
            log.debug("Subscribing for requests on : " + inQueue.getQueueName());

            MessageConsumer consumer = session.createConsumer(inQueue);
            consumer.setMessageListener(message -> {
                try {
                    server.onMessage(JmsUtils.byteArrayFromMessage(session, message));
                } catch (Exception ex) {
                    log.error("Failed to process request", ex);
                }
            });

            Queue statusQueue = session.createQueue(serverTopics.statusClients);
            log.debug("Subscribing for channel status on: " + statusQueue.getQueueName());
            MessageConsumer statusConsumer = session.createConsumer(statusQueue);
            statusConsumer.setMessageListener(message -> {
                try {
                    ConnectionStatus connectionStatus = ConnectionStatus.parseFrom(JmsUtils.byteArrayFromMessage(session, message));
                    log.debug("Received client connected status = " + connectionStatus.getConnected() + " for channel " + connectionStatus.getChannelId());
                    if (!connectionStatus.getConnected()) {
                        server.onChannelDisconnected(connectionStatus.getChannelId());
                        channelProducers.remove(connectionStatus.getChannelId());
                    }
                } catch (Exception ex) {
                    log.error("Failed to process status reply", ex);
                }
            });

            Queue pingQueue = session.createQueue(serverTopics.statusPrompt);
            log.debug("Subscribing for pings on : " + pingQueue.getQueueName());
            MessageConsumer pingConsumer = session.createConsumer(pingQueue);
            pingConsumer.setMessageListener(message -> {
                try {
                    log.debug("Received ping");
                    notifyConnected(true);
                } catch (Exception ex) {
                    log.error("Failed to process status reply", ex);
                }
            });

            server.addService(new CallTopicsServiceGrpc.CallTopicsServiceImplBase() {
                @Override
                public void setCallTopics(CallTopics request, StreamObserver<Empty> responseObserver) {
                    JmsCallQueues callQueues = new JmsCallQueues();
                    callQueuesMap.put(request.getChannelId() + request.getCallId(), callQueues);
                    try {
                        if (request.getTopicIn() != null && !request.getTopicIn().isEmpty()) {
                            log.debug("Subscribing for input stream for call " + request.getCallId() + " on topic " + request.getTopicIn());
                            String inQ = serverTopics.make(request.getTopicIn());
                            callQueues.consumerQueue = session.createQueue(inQ);
                            callQueues.consumer = session.createConsumer(callQueues.consumerQueue);
                            callQueues.consumer.setMessageListener(message -> {
                                try {
                                    server.onMessage(JmsUtils.byteArrayFromMessage(session, message));
                                } catch (Exception ex) {
                                    log.error("Failed to process reply", ex);
                                }
                            });
                            responseObserver.onNext(Empty.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                        if (request.getTopicOut() != null && !request.getTopicOut().isEmpty()) {
                            log.debug("Will send output stream for call " + request.getCallId() + " to topic " + request.getTopicOut());
                            String outQ = serverTopics.make(request.getTopicOut());
                            callQueues.producerQueue = session.createQueue(outQ);
                            callQueues.producer = session.createProducer(callQueues.producerQueue);
                        }
                    } catch (JMSException ex) {
                        responseObserver.onError(ex);
                        return;
                    }
                    responseObserver.onNext(Empty.newBuilder().build());
                    responseObserver.onCompleted();
                }
            });


            notifyConnected(true);


        } catch (JMSException ex) {
            throw new MessagingException(ex);
        }
    }

    @Override
    public void close() {
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
    public void send(String channelId, String callId,  String methodName, byte[] buffer) throws MessagingException {

        try {
            MessageProducer producer = null;
            final JmsCallQueues callQueues = callQueuesMap.get(channelId + callId);
            if(callQueues!=null && callQueues.producer!=null){
                //There is a specific queue for this call
                producer = callQueues.producer;
            } else {
                //Use the general producer for the channel
                producer = channelProducers.get(channelId);
                if (producer == null) {
                    final Queue replyQueue = session.createQueue(serverTopics.servicesOutForChannel(channelId));
                    producer = session.createProducer(replyQueue);
                    channelProducers.put(channelId, producer);
                }
            }
            log.debug("Sending response for call " + callId + " on " + producer.getDestination().toString());
            producer.send(JmsUtils.messageFromByteArray(session, buffer));
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
            Queue queue = session.createQueue(serverTopics.status);
            MessageProducer producer = session.createProducer(queue);
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(connectedMsg);
            producer.send(bytesMessage);
        } catch (JMSException e) {
            log.error("Failed to notify connected", e);
        }
    }

}
