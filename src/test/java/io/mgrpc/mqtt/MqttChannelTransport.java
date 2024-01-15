package io.mgrpc.mqtt;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.messaging.ChannelMessageListener;
import io.mgrpc.messaging.ChannelMessageTransport;
import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.pubsub.BufferToStreamObserver;
import io.mgrpc.messaging.pubsub.MessageSubscriber;
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
import java.util.concurrent.*;

import static io.mgrpc.MethodTypeConverter.methodType;

//  Topics and connection status:

//  The general topic structure is:
//  {server}/i|o/svc/{clientId}/{slashedFullMethodName}

//  The client sends RpcMessage requests to:
//
//  device1/i/svc/helloworld/ExampleHelloService/SayHello
//
//  The server sends RpcMessage replies to:
//
//  device1/o/svc/l2zb6li45zy7ilso/helloworld/ExampleHelloService/SayHello
//
//    where,
//      clientId = l2zb6li45zy7ilso
//      callId   = 2wded6fbtekgll6b
//      gRPC full method name = helloworld.ExampleHelloService/SayHello


//Status topics
//The server status topic will be
//  server/o/sys/status
//The server will send a connected=true message to this when it starts up or when a client sends it a message on
//  server/i/sys/status/prompt
//The server will send a connected=false message to server/o/sys/status when it shuts down normally or when it shuts down
//abnormally via its LWT
//The client status topic will be
//  server/i/sys/status/client/{channelId}
//so that clients can have restrictive policies. But the server will subscribe to
//  server/i/sys/status/#
//The client will send a connected=false message to server/i/sys/status/client when it shuts down normally
//(or when it shuts down abnormally LWT or some kind of watchdog). The server will then release any resources it
// has for {channelId}.
// The Channel will have a waitForServer method which a client can use to determine if a sever is up.
// This will method will subscribe to server/o/sys/status and send a prompt to server/i/sys/status/prompt

public class MqttChannelTransport implements ChannelMessageTransport, MessageSubscriber {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MqttAsyncClient client;
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private Map<String, RpcMessage.Builder> startMessages = new ConcurrentHashMap<>();

    private final ServerTopics serverTopics;

    private CountDownLatch serverConnectedLatch;
    private boolean serverConnected = false;

    private ChannelMessageListener channel;

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

    private static volatile Executor executorSingleton;
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

    /**
     * @param client
     * @param serverTopic The root topic of the server to connect to e.g. "tenant1/device1"
     *                    This topic should be unique to the broker.
     *                    Requests will be sent to {serverTopic}/i/svc/{slashedFullMethod}
     *                    The channel will subscribe for replies on {serverTopic}/o/svc/{channelId}/#
     *                    The channel will receive replies to on
     *                    {serverTopic}/o/svc/{channelId}/{slashedFullMethod}
     *                    Where if the gRPC fullMethodName is "helloworld.HelloService/SayHello"
     *                    then {slashedFullMethod} is "helloworld/HelloService/SayHello"
     */
    public MqttChannelTransport(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic);
    }


    @Override
    public void start(ChannelMessageListener channel) throws MessagingException {

        if (this.channel != null) {
            throw new MessagingException("Listener already connected");
        }
        this.channel = channel;
        try {
            final String replyTopicPrefix = serverTopics.servicesOutForChannel(this.channel.getChannelId()) + "/#";
            log.debug("Subscribing for responses on: " + replyTopicPrefix);
            client.subscribe(replyTopicPrefix, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                try {
                    final RpcSet rpcSet = RpcSet.parseFrom(mqttMessage.getPayload());
                    for (RpcMessage rpcMessage : rpcSet.getMessagesList()) {
                        channel.onMessage(rpcMessage);
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Failed to parse RpcMessage", e);
                    return;
                }
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);

            client.subscribe(serverTopics.status, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                try {
                    this.serverConnected = ConnectionStatus.parseFrom(mqttMessage.getPayload()).getConnected();
                    log.debug("Server connected status = " + serverConnected);
                    this.serverConnectedLatch.countDown();
                    if (!serverConnected) {
                        channel.onServerDisconnected();
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
    public void onCallClosed(String callId) {
        startMessages.remove(callId);
    }

    @Override
    public void request(String callId, int numMessages){}

    @Override
    public void close() {
        try {
            //Notify that this client has been closed so that any server with ongoing calls can cancel them and
            //release resources.
            log.debug("Closing channel. Sending notification on " + serverTopics.statusClients);
            final byte[] connectedMsg = ConnectionStatus.newBuilder().
                    setConnected(false).setChannelId(channel.getChannelId()).build().toByteArray();
            client.publish(serverTopics.statusClients, new MqttMessage(connectedMsg));
            final String replyTopicPrefix = serverTopics.servicesOutForChannel(channel.getChannelId()) + "/#";
            client.unsubscribe(replyTopicPrefix);
            client.unsubscribe(serverTopics.status);
        } catch (MqttException exception) {
            log.error("Exception closing " + exception);
        }
    }

    @Override
    public void send(RpcMessage.Builder messageBuilder) throws MessagingException {

        RpcMessage.Builder start = startMessages.get(messageBuilder.getCallId());
        if(start == null){
            if(messageBuilder.hasStart()){
                start = messageBuilder;
                startMessages.put(messageBuilder.getCallId(), messageBuilder);
                log.debug("Will send input messages for call " + messageBuilder.getCallId()
                        + " to " + serverTopics.methodIn(start.getStart().getMethodName()));
            } else {
                if(messageBuilder.hasStatus()){
                    log.warn("Call cancelled or half closed  before start. An exception may have occurred");
                return;
                } else {
                    throw new RuntimeException("First message sent to transport must be a start message. Call " + messageBuilder.getCallId() + " type " + messageBuilder.getMessageCase());
                }
            }
        }

        final RpcSet.Builder rpcSet = RpcSet.newBuilder();

        if (methodType(start).clientSendsOneMessage()) {
            //If clientSendsOneMessage we only want to send one broker message containing
            //start, request, status.
            if (messageBuilder.hasStart()) {
                //Wait for the request
                return;
            }
            if (messageBuilder.hasStatus()) {
                if (messageBuilder.getStatus().getCode() != Status.OK.getCode().value()) {
                    rpcSet.addMessages(messageBuilder);
                } else {
                    //Ignore non error status values (non cancel values) as the status will already have been sent automatically below
                    return;
                }
            } else {
                rpcSet.addMessages(start);
                rpcSet.addMessages(messageBuilder);
                final RpcMessage.Builder statusBuilder = RpcMessage.newBuilder()
                        .setCallId(messageBuilder.getCallId())
                        .setSequence(messageBuilder.getSequence() + 1)
                        .setStatus(GOOGLE_RPC_OK_STATUS);
                rpcSet.addMessages(statusBuilder);
            }
        } else {
            rpcSet.addMessages(messageBuilder);
        }


        final byte[] buffer = rpcSet.build().toByteArray();

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
        final String topic = serverTopics.methodIn(start.getStart().getMethodName());
        try {
            client.publish(topic, new MqttMessage(buffer));
        } catch (MqttException e) {
            log.error("Failed to send mqtt message", e);
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
