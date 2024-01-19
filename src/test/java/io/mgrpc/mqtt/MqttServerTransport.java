package io.mgrpc.mqtt;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.mgrpc.*;
import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.ServerMessageListener;
import io.mgrpc.messaging.ServerMessageTransport;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.mgrpc.MethodTypeConverter.methodType;

public class MqttServerTransport implements ServerMessageTransport {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private static final int DEFAULT_QUEUE_SIZE = 100;

    private final MqttAsyncClient client;
    private final static long SUBSCRIBE_TIMEOUT_MILLIS = 5000;

    private Map<String, RpcMessage> startMessages = new ConcurrentHashMap<>();

    private final CallSequencer callSequencer = new CallSequencer();


    private final ServerTopics serverTopics;

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

    private ServerMessageListener server;

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
    public MqttServerTransport(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopics = new ServerTopics(serverTopic);
    }


    @Override
    public void start(ServerMessageListener server) throws MessagingException {
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
                                    + getTopicForSend(message));
                        }
                        callSequencer.makeQueue(message.getCallId());
                        callSequencer.queueMessage(message);
                        if (message.hasStart()) {
                            //pump the queue. After the start message is processed grpc will do the rest of the pumping
                            request(message.getCallId(), 1);
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Failed to parse RpcMessage", e);
                    return;
                }
            })).waitForCompletion(SUBSCRIBE_TIMEOUT_MILLIS);


            //Subscribe for channel status messages
            log.debug("Subscribing for client status on " + serverTopics.statusClients);
            client.subscribe(serverTopics.statusClients, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
                ConnectionStatus connectionStatus = ConnectionStatus.parseFrom(mqttMessage.getPayload());
                log.debug("Received client connected status = " + connectionStatus.getConnected() + " on " + topic + " for channel " + connectionStatus.getChannelId());
                if (!connectionStatus.getConnected()) {
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
    public void close() {
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
    public void onCallClosed(String callId) {
        callSequencer.onCallClosed(callId);
        startMessages.remove(callId);
    }

    @Override
    public void request(String callId, int numMessages) {
        log.debug("Request(" + numMessages + ") for call " + callId);
        getExecutor().execute(()->{
            for(int i = 0; i < numMessages; i++){
                final RpcMessage message = callSequencer.getNextMessage(callId);
                log.debug("got message from sequencer");
                if(message == null){
                    //This could happen if call has been terminated
                    log.warn("Failed to get next message for call " + callId);
                    return;
                }
                server.onRpcMessage(message);
            }
        });
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
        if (methodType(startMessage).serverSendsOneMessage()) {
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
            client.publish(getTopicForSend(startMessage), new MqttMessage(setBuilder.build().toByteArray()));
        } catch (MqttException e) {
            log.error("Failed to send mqtt message", e);
            throw new MessagingException(e);
        }
    }

    private String getTopicForSend(RpcMessage startMessage){
        String topic = startMessage.getStart().getServerStreamTopic();
        if(topic == null || topic.isEmpty()){
            return serverTopics.replyTopic(startMessage.getStart().getChannelId(),
                    startMessage.getStart().getMethodName());
        } else {
            return topic;
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
