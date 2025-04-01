package io.mgrpc.jms;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.mgrpc.*;
import io.mgrpc.messaging.ChannelTopicConduit;
import io.mgrpc.messaging.ChannelListener;
import io.mgrpc.messaging.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


//  Topics and connection status:

//  The general topic structure is:
//  {server}/i|o/svc/{clientId}

//  The client sends RpcMessage requests to:
//
//  device1/i/svc
//
//  The server sends RpcMessage replies to:
//
//  device1/o/svc/l2zb6li45zy7ilso
//


//Status topics
//The server status topic will be
//  server/o/sys/status
//The server will send a connected=true message to this when it starts up or when a client sends it a message on
//  server/i/sys/status/prompt
//The server will send a connected=false message to server/o/sys/status when it shuts down normally or when it shuts down
//abnormally via its LWT
//The client status topic will be
//  server/i/sys/status/client
//The client will send a connected=false message to server/i/sys/status/client when it shuts down normally
//(or when it shuts down abnormally LWT or some kind of watchdog). The server will then release any resources it
// has for {channelId}.
// The Channel will have a waitForServer method which a client can use to determine if a sever is up.
// This will method will subscribe to server/o/sys/status and send a prompt to server/i/sys/status/prompt

public class JmsChannelTopicConduit implements ChannelTopicConduit {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private final Session session;

    private MessageProducer requestProducer;
    private MessageProducer pingProducer;
    private final static String TOPIC_SEPARATOR = "/";

    private final ServerTopics serverTopics;

    private CountDownLatch serverConnectedLatch;
    private volatile boolean serverConnected = false;

    private final boolean useBrokerFlowControl;

    private ChannelListener channel;

    private Map<String, JmsCallQueues> callQueuesMap = new ConcurrentHashMap<>();

    private Map<String, RpcMessage.Builder> startMessages = new ConcurrentHashMap<>();

    private final String channelStatusTopic;


    private final Executor executor;

    private volatile boolean isStarted = false;



    /**
     * @param session
     * @param serverTopic The root topic of the server to connect to e.g. "tenant1/device1"
     *                    This topic should be unique to the broker.
     *                    Requests will be sent to {serverTopic}/i/svc
     *                    The channel will subscribe for replies on {serverTopic}/o/svc/{channelId}
     * @param useBrokerFlowControl If this is true then for non-unary calls the conduit will create
     *                             an individual broker queue for the server or client streams and only
     *                             pull from this queue when the next message is required meaning that the
     *                             channel and the server do not need to use their internal buffers.
     * @param channelStatusTopic The topic on which messages regarding channel status will be reported.
     *                           If this value is null then the conduit will not attempt to send
     *                           channel status messages.
     */
    public JmsChannelTopicConduit(Session session, String serverTopic, boolean useBrokerFlowControl, Executor executor, String channelStatusTopic) {
        this.session = session;
        this.useBrokerFlowControl = useBrokerFlowControl;
        this.serverTopics = new ServerTopics(serverTopic, TOPIC_SEPARATOR);
        this.executor = executor;
        this.channelStatusTopic = channelStatusTopic;
    }

    /**
     * @param session
     * @param serverTopic The root topic of the server to connect to e.g. "tenant1/device1"
     *                    This topic should be unique to the broker.
     *                    Requests will be sent to {serverTopic}/i/svc
     *                    The channel will subscribe for replies on {serverTopic}/o/svc/{channelId}
     * @param useBrokerFlowControl If this is true then for non-unary calls the conduit will create
     *                             an individual broker queue for the server or client streams and only
     *                             pull from this queue when the next message is required meaning that the
     *                             channel and the server do not need to use their internal buffers.
     */
    public JmsChannelTopicConduit(Session session, String serverTopic, boolean useBrokerFlowControl, Executor executor) {
        this(session, serverTopic, useBrokerFlowControl, executor, null);
    }


    @Override
    public synchronized void start(ChannelListener channel) throws MessagingException {

        if(isStarted) {
            return;
        }

        isStarted = true;

        if (this.channel != null) {
            throw new MessagingException("Listener already connected");
        }
        this.channel = channel;
        try {
            Queue sendQueue = session.createQueue(serverTopics.servicesIn);
            log.debug("Will send requests to : " + sendQueue.getQueueName());
            requestProducer = session.createProducer(sendQueue);

            Queue pingQueue = session.createQueue(serverTopics.statusPrompt);
            log.debug("Will send pings to : " + pingQueue.getQueueName());
            pingProducer = session.createProducer(pingQueue);

            Queue replyQueue = session.createQueue(serverTopics.servicesOutForChannel(this.channel.getChannelId()));
            log.debug("Subscribing for responses on: " + replyQueue.getQueueName());
            MessageConsumer consumer = session.createConsumer(replyQueue);
            consumer.setMessageListener(message -> {
                try {
                    final RpcSet rpcSet = RpcSet.parseFrom(JmsUtils.byteArrayFromMessage(session, message));
                    for (RpcMessage rpcMessage : rpcSet.getMessagesList()) {
                        channel.onMessage(rpcMessage);
                    }
                } catch (Exception e) {
                    log.error("Failed to parse RpcMessage", e);
                    return;
                }
            });
            Queue statusQueue = session.createQueue(serverTopics.status);
            log.debug("Subscribing for server status on: " + statusQueue.getQueueName());
            MessageConsumer statusConsumer = session.createConsumer(statusQueue);
            statusConsumer.setMessageListener(message -> {
                try {
                    serverConnected = ConnectionStatus.parseFrom(JmsUtils.byteArrayFromMessage(session, message)).getConnected();
                    log.debug("Server " + serverTopics.root + " connected status = " + serverConnected);
                    serverConnectedLatch.countDown();
                    if (!serverConnected) {
                        channel.onDisconnect(serverTopics.root);
                    }
                } catch (Exception ex) {
                    log.error("Failed to process status reply", ex);
                }
            });

            pingServer();

        } catch (JMSException ex) {
            throw new MessagingException(ex);
        }


    }

    @Override
    public void close() {
        //Nothing to do. The JMSChannelConduit will close the session which
        //will release all resources like publishers etc.
    }

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
    public void send(RpcMessage.Builder messageBuilder) throws MessagingException {

        RpcMessage.Builder start = startMessages.get(messageBuilder.getCallId());
        if (start == null) {
            if (messageBuilder.hasStart()) {
                start = messageBuilder;
                startMessages.put(messageBuilder.getCallId(), messageBuilder);
                if(useBrokerFlowControl) {
                    //If this method has client or server streams (it's not just request response)
                    //Create specific queues for these so that the broker can buffer messages using broker queues
                    JmsCallQueues callQueues = new JmsCallQueues();
                    final MethodDescriptor.MethodType methodType = MethodTypeConverter.fromStart(start.getStart().getMethodType());
                    try {
                        if (!methodType.clientSendsOneMessage()) {
                            String inQ = serverTopics.make(serverTopics.servicesIn, channel.getChannelId(), messageBuilder.getCallId());
                            log.debug("Will publish client stream for call " + messageBuilder.getCallId() + " to " + inQ);
                            callQueues.producerQueue = session.createQueue(inQ);
                            callQueues.producer = session.createProducer(callQueues.producerQueue);
                            //The server will subscribe on clientStreamTopic for the client stream
                            start.getStartBuilder().setClientStreamTopic(inQ);
                            callQueuesMap.put(messageBuilder.getCallId(), callQueues);
                        }
                        if (!methodType.serverSendsOneMessage()) {
                            String outQ;
                            if (start.getStart().getServerStreamTopic() != null && !start.getStart().getServerStreamTopic().isEmpty()) {
                                //The client has used withOption MessageChannel.OUT_TOPIC
                                outQ = start.getStart().getServerStreamTopic();
                            } else {
                                outQ = serverTopics.make(serverTopics.servicesOutForChannel(channel.getChannelId()), messageBuilder.getCallId());
                            }
                            log.debug("Will subscribe for server stream for call " + messageBuilder.getCallId() + " on " + outQ);
                            callQueues.consumerQueue = session.createQueue(outQ);
                            callQueues.consumer = session.createConsumer(callQueues.consumerQueue);
                            //The server will publish server stream messages to serverStreamTopic
                            start.getStartBuilder().setServerStreamTopic(outQ);
                            callQueuesMap.put(messageBuilder.getCallId(), callQueues);
                        }
                    } catch (JMSException ex) {
                        throw new MessagingException("Failed to create queues for call " + messageBuilder.getCallId());
                    }
                }
            } else {
                if (messageBuilder.hasStatus() && (messageBuilder.getStatus().getCode() == Status.CANCELLED.getCode().value())) {
                    log.warn("Call cancelled before start. An exception may have occurred");
                    return;
                } else {
                    throw new RuntimeException("First message sent to conduit must be a start message. Call " + messageBuilder.getCallId());
                }
            }
        }

        final RpcSet.Builder setBuilder = RpcSet.newBuilder();
        if (MethodTypeConverter.fromStart(start).clientSendsOneMessage()) {
            //If clientSendsOneMessage we only want to send one broker message containing
            //start, request, status.
            if (messageBuilder.hasStart()) {
                //Wait for the request
                return;
            }
            if (messageBuilder.hasStatus()) {
                if (messageBuilder.getStatus().getCode() != Status.OK.getCode().value()) {
                    setBuilder.addMessages(messageBuilder);
                } else {
                    //Ignore non error status values (non cancel values) as the status will already have been sent automatically below
                    return;
                }
            } else {
                setBuilder.addMessages(start);
                setBuilder.addMessages(messageBuilder);
                final RpcMessage.Builder statusBuilder = RpcMessage.newBuilder()
                        .setCallId(messageBuilder.getCallId())
                        .setSequence(messageBuilder.getSequence() + 1)
                        .setStatus(GOOGLE_RPC_OK_STATUS);
                setBuilder.addMessages(statusBuilder);
            }
        } else {
            setBuilder.addMessages(messageBuilder);
        }

        final byte[] buffer = setBuilder.build().toByteArray();
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
                log.warn("Tried to send message but server is not connected at topic: " + serverTopics.root);
                throw new MessagingException("Server is not connected at topic: " + serverTopics.root);
            }
        }

        JmsCallQueues callQueues = callQueuesMap.get(messageBuilder.getCallId());
        try {
            if (!messageBuilder.hasStart() && callQueues != null && callQueues.producer != null) {
                //This message must be sent to a specific input queue
                callQueues.producer.send(JmsUtils.messageFromByteArray(session, buffer));
            } else {
                requestProducer.send(JmsUtils.messageFromByteArray(session, buffer));
            }
        } catch (JMSException ex) {
            throw new MessagingException(ex);
        }

    }

    @Override
    public void request(String callId, int numMessages) {
        final JmsCallQueues callQueues = callQueuesMap.get(callId);
        if (callQueues != null) {
            if (callQueues.consumer != null) {
                executor.execute(() -> {
                    //This will block until a message is available so we need to run it on a thread
                    //to prevent the channel from blocking. This will effectively block the thread for the duration
                    //of the whole call.
                    //We ignore numMessages and just get the next message from the queue
                    //After it is processed the service will call request() again.
                    try {
                        final Message message = callQueues.consumer.receive();
                        if (message != null) {
                            final byte[] bytes = JmsUtils.byteArrayFromMessage(session, message);
                            try {
                                final RpcSet rpcSet = RpcSet.parseFrom(bytes);
                                for (RpcMessage rpcMessage : rpcSet.getMessagesList()) {
                                    channel.onMessage(rpcMessage);
                                }
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Failed to parse RpcMessage", e);
                                return;
                            }
                        }
                    } catch (Exception ex) {
                        log.error("Exception processing or waiting for message", ex);
                    }
                });
            }
        }
    }

    /**
     * Send the server an empty message to the prompt topic to prompt it so send back its status
     * This will be handled in the client.subscribe(Topics.statusOut(serverTopic) set up in this.start
     */
    private void pingServer() {
        this.serverConnectedLatch = new CountDownLatch(1);
        //In case the server is already started, prompt it to send its connection status
        //If it is not started it will send connection status when it does start.
        try {
            log.debug("Pinging server for status");
            pingProducer.send(JmsUtils.messageFromByteArray(session, new byte[1]));
        } catch (JMSException ex) {
            log.error("Failed to ping server", ex);
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


}
