package io.mgrpc.mqtt;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.mgrpc.*;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

public class MqttTopicConduit implements TopicConduit {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IMqttAsyncClient client;
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private Map<String, RpcMessage> startMessages = new ConcurrentHashMap<>();

    private final ServerTopics serverTopics;

    private CountDownLatch serverConnectedLatch;
    private boolean serverConnected = false;

    private ChannelListener channel;


    private volatile boolean isStarted = false;

    private long timeLastUsed = System.currentTimeMillis();


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
     *                    send more flow credit.
     */
    public MqttTopicConduit(IMqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic);
    }


    @Override
    public synchronized void start(ChannelListener channel) throws MessagingException {

        if(isStarted) {
            return;
        }

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

            log.debug("Subscribing for server status at " + serverTopics.status);
            client.subscribe(serverTopics.status, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                try {
                    this.serverConnected = ConnectionStatus.parseFrom(mqttMessage.getPayload()).getConnected();
                    log.debug("Server connected status = " + serverConnected);
                    this.serverConnectedLatch.countDown();
                    if (!serverConnected) {
                        channel.onServerDisconnected(serverTopics.root);
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

        this.isStarted = true;

    }

    @Override
    public void onCallClosed(String callId) {
        startMessages.remove(callId);
    }

    public int getNumOpenCalls(){
        return startMessages.size();
    }

    public void unsubscribe(){
        try {
            final String replyTopicPrefix = serverTopics.servicesOutForChannel(channel.getChannelId()) + "/#";
            client.unsubscribe(replyTopicPrefix);
            client.unsubscribe(serverTopics.status);
        } catch (MqttException exception) {
            log.error("Exception unsubscribing " + exception);
        }
        this.isStarted = false;
    }

    public long getTimeLastUsed(){
        return timeLastUsed;
    }


    @Override
    public void request(String callId, int numMessages){}

    @Override
    public void close() {

    }


    @Override
    public void send(RpcMessage.Builder messageBuilder) throws MessagingException {

        timeLastUsed = System.currentTimeMillis();

        RpcMessage start = startMessages.get(messageBuilder.getCallId());
        if(start == null){
            if(messageBuilder.hasStart()){
                final Start.Builder startBuilder = messageBuilder.getStartBuilder();
                //If the client has not specified a server stream topic then set it to the default
                if(startBuilder.getServerStreamTopic().isEmpty()) {
                    final String replyTopic = serverTopics.replyTopic(this.channel.getChannelId(), messageBuilder.getStart().getMethodName());
                    startBuilder.setServerStreamTopic(replyTopic);
                }
                start = messageBuilder.build();
                startMessages.put(messageBuilder.getCallId(), start);
                log.debug("Will send input messages for call " + messageBuilder.getCallId()
                        + " to " + serverTopics.methodIn(start.getStart().getMethodName()));
            } else {
                if(messageBuilder.hasStatus()){
                    log.warn("Call cancelled or half closed  before start. An exception may have occurred");
                    return;
                } else {
                    throw new RuntimeException("First message sent to conduit must be a start message. Call " + messageBuilder.getCallId() + " type " + messageBuilder.getMessageCase());
                }
            }
        }

        final RpcSet.Builder rpcSet = RpcSet.newBuilder();

        if (MethodTypeConverter.fromStart(start).clientSendsOneMessage()) {
            //If clientSendsOneMessage we only want to send one broker message containing
            //start, request, status. This means that we don't send a Start message now but wait for the
            //client to send request message. When we receive the request message we immediately send
            //start, request, status in one single broker message. Finally when then client actually sends the status
            //message we just ignore it.

            switch(messageBuilder.getMessageCase()){
                case START:
                    //Wait for the client to send the request message
                    return;

                case STATUS:
                    if (messageBuilder.getStatus().getCode() != Status.OK.getCode().value()) {
                        rpcSet.addMessages(messageBuilder);
                    } else {
                        //Ignore non error status values (non cancel values) as the status will already have been sent automatically below
                        return;
                    }
                    break;

                case VALUE:
                    //Send start, request, status as a single broker message
                    rpcSet.addMessages(start);
                    rpcSet.addMessages(messageBuilder);
                    final RpcMessage.Builder statusBuilder = RpcMessage.newBuilder()
                            .setCallId(messageBuilder.getCallId())
                            .setSequence(messageBuilder.getSequence() + 1)
                            .setStatus(GOOGLE_RPC_OK_STATUS);
                    rpcSet.addMessages(statusBuilder);
                    break;

                case FLOW:
                    rpcSet.addMessages(messageBuilder);
                    break;
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
                log.warn("Tried to send message but server is not connected at topic: " + serverTopics.root);
                throw new MessagingException("Server is not connected at topic: " + serverTopics.root);
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



}
