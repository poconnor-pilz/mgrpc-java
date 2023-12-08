package io.mgrpc.jms;

import io.grpc.MethodDescriptor;
import io.mgrpc.*;
import io.mgrpc.messaging.ChannelMessageListener;
import io.mgrpc.messaging.ChannelMessageTransport;
import io.mgrpc.messaging.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.*;


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

public class JmsChannelTransport implements ChannelMessageTransport {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Connection client;
    private Session session;

    private MessageProducer requestProducer;
    private MessageProducer pingProducer;
    private final static String TOPIC_SEPARATOR = "/";

    private final ServerTopics serverTopics;

    private CountDownLatch serverConnectedLatch;
    private boolean serverConnected = false;

    private MessageChannel channel;

    private Map<String, JmsCallQueues> callQueuesMap = new ConcurrentHashMap<>();

    private static volatile Executor executorSingleton;


    /**
     * @param client
     * @param serverTopic The root topic of the server to connect to e.g. "tenant1/device1"
     *                    This topic should be unique to the broker.
     *                    Requests will be sent to {serverTopic}/i/svc
     *                    The channel will subscribe for replies on {serverTopic}/o/svc/{channelId}
     */
    public JmsChannelTransport(Connection client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic, TOPIC_SEPARATOR);
    }


    @Override
    public void start(MessageChannel channel) throws MessagingException {

        if (this.channel != null) {
            throw new MessagingException("Listener already connected");
        }
        this.channel = channel;
        try {

            session = client.createSession();

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
                    channel.onMessage(JmsUtils.byteArrayFromMessage(session, message));
                } catch (Exception ex){
                    log.error("Failed to process reply", ex);
                }
            });

            Queue statusQueue = session.createQueue(serverTopics.status);
            log.debug("Subscribing for server status on: " + statusQueue.getQueueName());
            MessageConsumer statusConsumer = session.createConsumer(statusQueue);
            statusConsumer.setMessageListener(message -> {
                try {
                    serverConnected = ConnectionStatus.parseFrom(JmsUtils.byteArrayFromMessage(session, message)).getConnected();
                    log.debug("Server connected status = " + serverConnected);
                    serverConnectedLatch.countDown();
                    if (!serverConnected) {
                        channel.onServerDisconnected();
                    }
                } catch (Exception ex){
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
        try {
            //Notify that this client has been closed so that any server with ongoing calls can cancel them and
            //release resources.
            String statusQueue = serverTopics.statusClients;
            log.debug("Closing channel. Sending notification on " + statusQueue);
            final byte[] connectedMsg = ConnectionStatus.newBuilder()
                    .setConnected(false)
                    .setChannelId(channel.getChannelId())
                    .build().toByteArray();
            Queue queue = session.createQueue(statusQueue);
            MessageProducer producer = session.createProducer(queue);
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(connectedMsg);
            producer.send(bytesMessage);
            session.close();
        } catch (JMSException e) {
            log.error("Exception closing " + e);
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
    public void send(boolean isStart, String callId, MethodDescriptor methodDescriptor, byte[] buffer) throws MessagingException {
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
        try {
            JmsCallQueues callQueues = null;
            if(isStart) {
                if (!methodDescriptor.getType().clientSendsOneMessage()
                ||!methodDescriptor.getType().serverSendsOneMessage()) {
                    //This method has input or output streams (it's not just request response)
                    //Create specific queues for these so that the broker can buffer messages.
                    callQueues = new JmsCallQueues();
                    callQueuesMap.put(callId, callQueues);
                    final CallTopics.Builder builder = CallTopics.newBuilder();
                    builder.setChannelId(channel.getChannelId());
                    builder.setCallId(callId);

                    if(!methodDescriptor.getType().clientSendsOneMessage()) {
                        String inQ = serverTopics.make(serverTopics.servicesIn, channel.getChannelId(), callId);
                        callQueues.producerQueue = session.createQueue(inQ);
                        callQueues.producer = session.createProducer(callQueues.producerQueue);
                        builder.setTopicIn(inQ);
                    }
                    if(!methodDescriptor.getType().serverSendsOneMessage()) {
                        String outQ = serverTopics.make(serverTopics.servicesOutForChannel(channel.getChannelId()), callId);
                        callQueues.consumerQueue = session.createQueue(outQ);
                        callQueues.consumer = session.createConsumer(callQueues.consumerQueue);
                        builder.setTopicOut(outQ);
                        //Listen for messages on the specific queue
                        callQueues.consumer.setMessageListener(message -> {
                            try {
                                channel.onMessage(JmsUtils.byteArrayFromMessage(session, message));
                            } catch (Exception ex){
                                log.error("Failed to process reply", ex);
                            }
                        });
                    }
                    final CallTopicsServiceGrpc.CallTopicsServiceBlockingStub callTopicsService
                            = CallTopicsServiceGrpc.newBlockingStub(channel);
                    //Inform the server to create topics. This call will block until the server has returned a response.
                    callTopicsService.setCallTopics(builder.build());
                }
            } else {
                callQueues = callQueuesMap.get(callId);
            }

            if(callQueues != null && callQueues.producer != null){
                //This message must be sent to a specific input queue
                callQueues.producer.send(JmsUtils.messageFromByteArray(session, buffer));
            } else {
                requestProducer.send(JmsUtils.messageFromByteArray(session, buffer));
            }

            //TODO: The CallQueues need to be closed when the call closes so need notification of that
            //Then do all the server side stuff. It needs to implement the CallTopicsServiceGrpc and create
            //the queues on its side for starters.
            //This should be done in the the transport there is just one transport per server
            // Then it needs to listen for request() and pump the queues
            //Also the client above has a listener for the output queue but it is just pumping it whenever
            //a message comes in. It also needs to listen for request()
            //But before testing with request could run all tests without it to make sure things are working.


        } catch (JMSException e) {
            log.error("Failed to send request", e);
            throw new MessagingException(e);
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
