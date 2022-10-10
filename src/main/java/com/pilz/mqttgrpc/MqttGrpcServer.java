package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class MqttGrpcServer implements Closeable {
    private static Logger log = LoggerFactory.getLogger(MqttGrpcServer.class);

    private final MqttAsyncClient client;

    private final String serverTopic;

    private boolean initCalled = false;
    private final Map<String, BufferObserver> streamIdToClientBufferObserver = new HashMap<>();

    public MqttGrpcServer(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopic = serverTopic;
    }


    /**
     * This should always be called after construction. It will set the lwt status to connected
     * so that clients can detect if the server is connected
     *
     * @throws StatusException
     */
    public void init() throws StatusException {
        //TODO: If the server is set to auto re-connect then we need to call this init method again when there is a sucessful connection.
        //This can be done with MqttCallbackExtended
        final byte[] statusConnected = ConnectionStatus.newBuilder().setConnected(true).build().toByteArray();
        try {
            //Subscribe for statusprompt. A client will send a message to statusprompt when it wants to know if the server is running
            //If we receive a message at statusprompt then send back a connected status message.
            client.subscribe(Topics.systemStatusPrompt(this.serverTopic), 1, new MqttExceptionLogger((String topic, MqttMessage message) -> {
                try {
                    this.client.publish(Topics.systemStatus(serverTopic), statusConnected, 1, true).waitForCompletion();
                } catch (MqttException e) {
                    throw new StatusException(Status.UNAVAILABLE.fromThrowable(e));
                }
            })).waitForCompletion();
        } catch (MqttException e) {
            throw new StatusException(Status.INTERNAL.withCause(e));
        }

        initCalled = true;
    }

    @Override
    /**
     * close() should be called on the server when finished with it so that clients are notified
     * and can release any resources that they have.
     */
    public void close()  {
        final byte[] statusDisconnected = ConnectionStatus.newBuilder().setConnected(false).build().toByteArray();
        try {
            this.client.publish(Topics.systemStatus(serverTopic), statusDisconnected, 1, false).waitForCompletion();
            this.client.unsubscribe(Topics.systemStatusPrompt(this.serverTopic));
        } catch (MqttException e) {
            log.error("Failed to publish disconnected status on close", e);
        }
    }


    public void subscribeService(String serviceName, Skeleton skeleton) throws MqttException {

        if (!initCalled) {
            throw new MqttException(new Throwable("init() method not called"));
        }
        String allMethodsIn = Topics.allMethodsIn(serverTopic, serviceName);
        log.debug("subscribeService: " + allMethodsIn);
        client.subscribe(allMethodsIn, 1, new MqttExceptionLogger((String topic, MqttMessage message) -> {
            //We use an MqttExceptionLogger here because if a we throw an exception in the subscribe handler
            //it will disconnect the mqtt client
            log.debug("Received message on : " + topic);
            //TODO: this should be in thread pool
            //TODO: Error handling needed everywhere
            final MqttGrpcRequest request = MqttGrpcRequest.parseFrom(message.getPayload());
            //TODO: When we unsubscribe will this protoService be garbage collected?
            //Maybe it should be in a map. instead and just have a single listener on the MqttProto class
            //In fact the MqttProto class could just do one subscribe for rootOfAllServices/#
            String method = topic.substring(topic.lastIndexOf('/') + 1);
            ByteString params = request.getMessage();
            String replyTo = request.getReplyTo();
            String requestId = request.getRequestId();
            if(requestId.isEmpty()){
                log.error("Every message sent from the client must have a requestId");
                return;
            }

            if (request.getType() == MqttGrpcType.REQUEST) {
                BufferObserver clientBufferObserver = skeleton.onRequest(method, params,
                        new ServerBufferPublisher(requestId, replyTo));

                if (clientBufferObserver != null) {
                    //The service expects a client side stream
                    //Store the stream so that we can send later messages to it if they have the same requestId
                    log.debug("Setting up MappedInputStream for " + requestId);
                    streamIdToClientBufferObserver.put(requestId, clientBufferObserver);
                }

            } else {
                //This message is not a REQUEST so it is part of a client side stream
                //Get the corresponding stream and send on the message
                final BufferObserver clientBufferObserver = streamIdToClientBufferObserver.get(requestId);
                if (clientBufferObserver == null) {
                    //TODO: log error, this should not occur
                    log.error("Can't find stream for: " + requestId);
                    return;
                }
                switch (request.getType()) {
                    case NEXT:
                        clientBufferObserver.onNext(request.getMessage());
                        break;
                    case SINGLE:
                        clientBufferObserver.onSingle(request.getMessage());
                        streamIdToClientBufferObserver.remove(requestId);
                        break;
                    case COMPLETED:
                        //TODO: This also needs to be removed if the client gets disconnected
                        log.debug("Completed received. Removing MappedClientStream for " + requestId);
                        streamIdToClientBufferObserver.remove(requestId);
                        clientBufferObserver.onCompleted();
                        break;
                    case ERROR:
                        log.error("Received error in client stream");
                        log.debug("Removing MappedClientStream for " + requestId);
                        streamIdToClientBufferObserver.remove(requestId);
                        clientBufferObserver.onError(request.getMessage());
                        break;
                    default:
                        log.error("Unhandled message type: " + request.getType().name());
                }
                return;
            }

        }));
    }


    /**
     * BufferObserver that just takes Buffer values and publishes them to replyTo with the correct type
     * (NEXT, COMPLETED ...)
     */
    private class ServerBufferPublisher implements BufferObserver {

        private final String requestId;
        private final String replyTo;

        private ServerBufferPublisher(String requestId, String replyTo) {
            this.requestId = requestId;
            this.replyTo = replyTo;
        }

        @Override
        public void onNext(ByteString value) {
            log.debug("Sending NEXT");
            MqttGrpcResponse reply = MqttGrpcResponse.newBuilder()
                    .setRequestId(requestId)
                    .setMessage(value)
                    .setType(MqttGrpcType.NEXT).build();
            try {
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (MqttException e) {
                //We can only log the exception here as the broker is broken
                log.error("onNext failed to send mqtt message", e);
            }
        }

        @Override
        public void onSingle(ByteString value) {
            log.debug("Sending SINGLE");
            MqttGrpcResponse reply = MqttGrpcResponse.newBuilder()
                    .setRequestId(requestId)
                    .setMessage(value)
                    .setType(MqttGrpcType.SINGLE).build();
            try {
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (MqttException e) {
                //We can only log the exception here as the broker is broken
                log.error("Publish failed", e);
            }
        }

        @Override
        public void onCompleted() {
            log.debug("Sending COMPLETED");
            MqttGrpcResponse reply = MqttGrpcResponse.newBuilder()
                    .setRequestId(requestId)
                    .setType(MqttGrpcType.COMPLETED).build();
            try {
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (MqttException e) {
                //We can only log the exception here as the broker is broken
                log.error("Publish failed", e);
            }
        }


        @Override
        public void onError(ByteString error) {
            log.debug("Sending ERROR");
            MqttGrpcResponse reply = MqttGrpcResponse.newBuilder()
                    .setRequestId(requestId)
                    .setMessage(error)
                    .setType(MqttGrpcType.ERROR).build();
            try {
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (MqttException e) {
                //We can only log the exception here as the broker is broken
                log.error("Publish failed");
            }
        }


    }

}
