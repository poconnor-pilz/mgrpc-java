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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttServerConduit implements ServerConduit {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private final IMqttAsyncClient client;
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    private Map<String, RpcMessage> startMessages = new ConcurrentHashMap<>();

    private final ServerTopics serverTopics;

    private final String channelStatusTopic;

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
                            Thread t = new Thread(r, "mgprc-mqtt-server-" + threadNumber.getAndIncrement());
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
     *                    This topic should be unique to the broker.
     *                    The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                    A request for a method should be sent to sent to {serverTopic}/i/svc/{slashedFullMethod}
     *                    Replies will be sent to {serverTopic}/o/svc/{channelId}/{slashedFullMethod}
     *                    Where if the gRPC fullMethodName is "helloworld.HelloService/SayHello"
     *                    then {slashedFullMethod} is "helloworld/HelloService/SayHello"
     * @param channelStatusTopic The topic on which messages regarding channel status will be reported.
     *                           This topic will usually be the same topic as the MQTT LWT for the channel client.
     *                           If this value is null then the conduit will not attempt to subscribe for
     *                           channel status messages.
     */
    public MqttServerConduit(IMqttAsyncClient client, String serverTopic, String channelStatusTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic);
        this.channelStatusTopic = channelStatusTopic;
    }

    /**
     * @param client
     * @param serverTopic The root topic of the server e.g. "tenant1/device1"
     *                    This topic should be unique to the broker.
     *                    The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *                    A request for a method should be sent to sent to {serverTopic}/i/svc/{slashedFullMethod}
     *                    Replies will be sent to {serverTopic}/o/svc/{channelId}/{slashedFullMethod}
     *                    Where if the gRPC fullMethodName is "helloworld.HelloService/SayHello"
     *                    then {slashedFullMethod} is "helloworld/HelloService/SayHello"
     */
    public MqttServerConduit(IMqttAsyncClient client, String serverTopic) {
        this(client, serverTopic, null);
    }


    @Override
    public void start(ServerListener server) throws MessagingException {
        if (this.server != null) {
            throw new MessagingException("Listener already connected");
        }
        this.server = server;
        try {
            //Subscribe for gRPC messages
            String inTopicFilter = serverTopics.servicesIn + "/#";
            log.debug("Subscribing for requests on " + inTopicFilter);
            client.subscribe(inTopicFilter, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                try {
                    final RpcSet rpcSet = RpcSet.parseFrom(mqttMessage.getPayload());
                    for (RpcMessage message : rpcSet.getMessagesList()) {
                        if (message.hasStart()) {
                            //Cache the start message so that we can use it to get information about the call later.
                            startMessages.put(message.getCallId(), message);
                            log.debug("Will send output messages for call " + message.getCallId() + " to "
                            + message.getStart().getServerStreamTopic());
                        }
                        server.onMessage(message);
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Failed to parse RpcMessage", e);
                    return;
                }
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);


            //Subscribe for channel status messages
            if(this.channelStatusTopic != null) {
                log.debug("Subscribing for client status on " + channelStatusTopic);
                client.subscribe(channelStatusTopic, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                    ConnectionStatus connectionStatus = ConnectionStatus.parseFrom(mqttMessage.getPayload());
                    log.debug("Received client connected status = " + connectionStatus.getConnected() + " on " + topic + " for channel " + connectionStatus.getChannelId());
                    if (!connectionStatus.getConnected()) {
                        server.onChannelDisconnected(connectionStatus.getChannelId());
                    }
                })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);
            }

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
    public void close() {
        try {
            notifyConnected(false);
            client.unsubscribe(serverTopics.servicesIn + "/#");
            if(this.channelStatusTopic != null) {
                client.unsubscribe(this.channelStatusTopic + "/#");
            }
            client.unsubscribe(serverTopics.statusPrompt);
        } catch (MqttException exception) {
            log.error("Exception closing " + exception);
        }
    }

    @Override
    public void onCallClosed(String callId) {
        startMessages.remove(callId);
    }

    @Override
    public void request(String callId, int numMessages) {
    }

    @Override
    public void send(RpcMessage message) throws MessagingException {

        final RpcMessage startMessage = startMessages.get(message.getCallId());
        if (startMessage == null) {
            String err = "No cached start message for call " + message.getCallId();
            log.error(err);
            throw new MessagingException(err);
        }


        final RpcSet.Builder setBuilder = RpcSet.newBuilder();
        if (MethodTypeConverter.fromStart(startMessage).serverSendsOneMessage()) {
            if (message.hasValue()) {
                //Send the value and the status as a set in one message to the broker
                setBuilder.addMessages(message);
                final RpcMessage.Builder statusBuilder = RpcMessage.newBuilder()
                        .setCallId(message.getCallId())
                        .setSequence(message.getSequence() + 1)
                        .setStatus(GOOGLE_RPC_OK_STATUS);
                setBuilder.addMessages(statusBuilder);
            } else {
                if (message.getStatus().getCode() != Status.OK.getCode().value()) {
                    setBuilder.addMessages(message);
                } else {
                    //Ignore non error status values (non cancel values) as the status will already have been sent automatically above
                    return;
                }
            }
        } else {
            setBuilder.addMessages(message);
        }
        try {
            final String topic =  startMessage.getStart().getServerStreamTopic();
            if(topic.isEmpty()){
                log.error("Cannot send a reply because the start message did not specify a topic on which to send it. serverStreamTopic is empty");
            } else {
                client.publish(topic, new MqttMessage(setBuilder.build().toByteArray()));
            }
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



}
