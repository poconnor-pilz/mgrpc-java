package io.mgrpc.jms;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.mgrpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class JmsServerConduit implements ServerConduit {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private final Connection client;

    private Session session;

    private final static String TOPIC_SEPARATOR = "/";
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    private final Map<String, MessageProducer> channelProducers = new ConcurrentHashMap<>();

    private Map<String, RpcMessage> startMessages = new ConcurrentHashMap<>();

    private Map<String, JmsCallQueues> callQueuesMap = new ConcurrentHashMap<>();

    private final ServerTopics serverTopics;

    private final String channelStatusTopic;

    private static final int TWO_BILLION = 2*1000*1000*1000;


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
                            Thread t = new Thread(r, "mgrpc-jms-server-" + threadNumber.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });
                }
            }
        }
        return executorSingleton;
    }


    private ServerListener server;

    /**
     * @param client
     * @param serverTopic The root topic of the server e.g. "tenant1/device1"
     *                    The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                    A request for a method should be sent to sent to {serverTopic}/i/svc
     *                    Replies will be sent to {serverTopic}/o/svc/{channelId}}
     * @param channelStatusTopic The topic on which messages regarding channel status will be reported.
     *                           If this value is null then the conduit will not attempt to subscribe for
     *                           channel status messages.
     */
    public JmsServerConduit(Connection client, String serverTopic, String channelStatusTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic, TOPIC_SEPARATOR);
        this.channelStatusTopic = channelStatusTopic;
    }

    /**
     * @param client
     * @param serverTopic The root topic of the server e.g. "tenant1/device1"
     *                    The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                    A request for a method should be sent to sent to {serverTopic}/i/svc
     *                    Replies will be sent to {serverTopic}/o/svc/{channelId}}
     */
    public JmsServerConduit(Connection client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic, TOPIC_SEPARATOR);
        this.channelStatusTopic = null;
    }

    @Override
    /**
     * For JMS there is no flow control unless the client specifies to use broker flow control
     */
    public int getFlowCredit() {
        //Note: It would not be hard to have flow control without broker flow control here but we are just
        //not implementing it for the moment
        //2 billion. Safely under max integer
        return TWO_BILLION;
    }


    @Override
    public void start(ServerListener server) throws MessagingException {
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
                    final byte[] bytes = JmsUtils.byteArrayFromMessage(session, message);
                    try {
                        final RpcSet rpcSet = RpcSet.parseFrom(bytes);
                        for (RpcMessage rpcMessage : rpcSet.getMessagesList()) {
                            if (rpcMessage.hasStart()) {
                                startMessages.put(rpcMessage.getCallId(), rpcMessage);
                                JmsCallQueues callQueues = new JmsCallQueues();
                                try {
                                    final Start start = rpcMessage.getStart();
                                    if (start.getClientStreamTopic() != null && !start.getClientStreamTopic().isEmpty()) {
                                        //This call has a specific queue for the client stream so we need to subscribe to it
                                        log.debug("Subscribing for client stream for call " + rpcMessage.getCallId() + " on topic " + start.getClientStreamTopic());
                                        String inQ = serverTopics.make(start.getClientStreamTopic());
                                        callQueues.consumerQueue = session.createQueue(inQ);
                                        callQueues.consumer = session.createConsumer(callQueues.consumerQueue);
                                        callQueuesMap.put(rpcMessage.getCallId(), callQueues);
                                    }
                                    if (start.getServerStreamTopic() != null && !start.getServerStreamTopic().isEmpty()) {
                                        log.debug("Will send server stream for call " + rpcMessage.getCallId() + " to topic " + start.getServerStreamTopic());
                                        String outQ = serverTopics.make(start.getServerStreamTopic());
                                        callQueues.producerQueue = session.createQueue(outQ);
                                        callQueues.producer = session.createProducer(callQueues.producerQueue);
                                        callQueuesMap.put(rpcMessage.getCallId(), callQueues);
                                    }
                                } catch (JMSException ex) {
                                    log.error("Failed to set up dedicated queue(s) for call " + rpcMessage.getCallId());
                                    return;
                                }

                            }
                            server.onMessage(rpcMessage);
                        }
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Failed to parse RpcMessage", e);
                        return;
                    }
                } catch (Exception ex) {
                    log.error("Failed to process request", ex);
                }
            });

            if(this.channelStatusTopic != null) {
                Queue statusQueue = session.createQueue(this.channelStatusTopic);
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
            }

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

            notifyConnected(true);

        } catch (JMSException ex) {
            throw new MessagingException(ex);
        }
    }

    @Override
    public void close() {
        try {
            log.debug("Closing conduit");
            notifyConnected(false);
            session.close();
        } catch (JMSException exception) {
            log.error("Exception closing " + exception);
        }
    }


    @Override
    public void onCallClosed(String callId) {
        startMessages.remove(callId);
        final JmsCallQueues callQueues = callQueuesMap.get(callId);
        try {
            if (callQueues != null) {
                log.debug("Conduit releasing resources for call " + callId);
                if (callQueues.producer != null) {
                    callQueues.producer.close();
                }
                if (callQueues.consumer != null) {
                    callQueues.consumer.close();
                }
                callQueuesMap.remove(callId);
            }
        } catch (Exception ex) {
            log.error("Exception closing call queues", ex);
        }
    }

    @Override
    public void request(String callId, int numMessages) {
        final JmsCallQueues callQueues = callQueuesMap.get(callId);
        if (callQueues != null) {
            if (callQueues.consumer != null) {
                getExecutor().execute(() -> {
                    //This will block until a message is available so we need to run it on a thread
                    //to prevent the channel from blocking
                    //We ignore numMessages and just get the next message from the queue
                    //After it is processed the service will call request() again.
                    try {
                        boolean more = true;
                        while(more) {
                            final Message message = callQueues.consumer.receive();
                            more = false;
                            if (message != null) {
                                final byte[] bytes = JmsUtils.byteArrayFromMessage(session, message);
                                try {
                                    final RpcSet rpcSet = RpcSet.parseFrom(bytes);
                                    for (RpcMessage rpcMessage : rpcSet.getMessagesList()) {
                                        if(rpcMessage.hasFlow()){
                                            //If the message is a flow message then we will need to pull again
                                            //to get the gRPC message that request is calling for.
                                            more = true;
                                        }
                                        server.onMessage(rpcMessage);
                                    }
                                } catch (InvalidProtocolBufferException e) {
                                    log.error("Failed to parse RpcMessage", e);
                                    return;
                                }
                            }
                        }
                    } catch (Exception ex) {
                        log.error("Exception processing or waiting for message for call " + callId, ex);
                    }
                });
            }
        }
    }


    @Override
    public void send(RpcMessage message) throws MessagingException {

        final RpcMessage startMessage = startMessages.get(message.getCallId());
        if (startMessage == null) {
            String err = "No cached start message for call " + message.getCallId();
            log.error(err);
            throw new MessagingException(err);
        }

        try {
            MessageProducer producer = null;
            final JmsCallQueues callQueues = callQueuesMap.get(message.getCallId());
            if (callQueues != null && callQueues.producer != null) {
                //There is a specific queue for this call
                producer = callQueues.producer;
            } else {
                final String channelId = startMessage.getStart().getChannelId();
                //Use the general producer for the channel
                producer = channelProducers.get(channelId);
                if (producer == null) {
                    final Queue replyQueue = session.createQueue(serverTopics.servicesOutForChannel(channelId));
                    producer = session.createProducer(replyQueue);
                    channelProducers.put(channelId, producer);
                }
            }
            final RpcSet.Builder setBuilder = RpcSet.newBuilder();
            setBuilder.addMessages(message);
            if (MethodTypeConverter.fromStart(startMessage).serverSendsOneMessage()) {
                if (message.hasValue()) {
                    //Send the value and the status as on as a set in a single message to the broker
                    setBuilder.addMessages(message);
                    final RpcMessage.Builder statusBuilder = RpcMessage.newBuilder()
                            .setCallId(message.getCallId())
                            .setSequence(message.getSequence() + 1)
                            .setStatus(GOOGLE_RPC_OK_STATUS);
                    setBuilder.addMessages(statusBuilder);
                } else {
                    if(message.hasStatus()) {
                        if (message.getStatus().getCode() == Status.OK.getCode().value()) {
                            //Ignore non error status values (non cancel values) as the status will already have been sent automatically above
                            return;
                        }
                    }
                }
            }

            producer.send(JmsUtils.messageFromByteArray(session, setBuilder.build().toByteArray()));

        } catch (JMSException e) {
            log.error("Failed to send jms message", e);
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
