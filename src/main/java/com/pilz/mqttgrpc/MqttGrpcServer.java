package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.pilz.mqttgrpc.Consts.IN;
import static com.pilz.mqttgrpc.Consts.SVC;

public class MqttGrpcServer {
    private static Logger log = LoggerFactory.getLogger(MqttGrpcServer.class);

    /**
     * BufferObserver that just takes Buffer values and publishes them to replyTo with the correct type
     * (NEXT, COMPLETED ...)
     */
    private class ServerBufferPublisher implements BufferObserver {

        private final String replyTo;

        private ServerBufferPublisher(String replyTo) {
            this.replyTo = replyTo;
        }

        @Override
        public void onNext(ByteString value) {
            log.debug("Sending NEXT");
            MqttGrpcResponse reply = MqttGrpcResponse.newBuilder()
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


    private final MqttAsyncClient client;

    private final String serverTopic;

    private final Map<String, BufferObserver> streamIdToClientBufferObserver = new HashMap<>();

    public MqttGrpcServer(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopic = serverTopic;
    }


    public void subscribeService(String serviceName, Skeleton skeleton) throws MqttException {
        String wildTopic = TopicMaker.make(serverTopic, IN, SVC,  serviceName, "#");
        log.debug("subscribeService: " + wildTopic);
        client.subscribe(wildTopic, 1, new MqttExceptionLogger((String topic, MqttMessage message) -> {
            //TODO: must handle all exceptions here (user mqttexceptionhandler)
            //or else the exception will disconnect the mqtt client
            log.debug("ProtoServiceManager received message on : " + topic);
            //TODO: this should be in thread pool
            //TODO: Error handling needed everywhere
            final MqttGrpcRequest request = MqttGrpcRequest.parseFrom(message.getPayload());
            //TODO: When we unsubscribe will this protoService be garbage collected?
            //Maybe it should be in a map. instead and just have a single listener on the MqttProto class
            //In fact the MqttProto class could just do one subscribe for rootOfAllServices/#
            String method = topic.substring(topic.lastIndexOf('/') + 1);
            ByteString params = request.getMessage();
            String replyTo = request.getReplyTo();
            String streamId = request.getStreamId();

            if (request.getType() == MqttGrpcType.REQUEST) {
                BufferObserver clientBufferObserver = skeleton.onRequest(method, params,
                        new ServerBufferPublisher(replyTo));

                if (clientBufferObserver != null) {
                    if (streamId.isEmpty()) {
                        //TODO: Send error to client. Client must supply streamId if the service has an input stream
                        log.error("No streamId supplied for client stream request");
                        return;
                    }
                    //Store the input stream so that we can send later messages to it if they have the same streamId
                    log.debug("Setting up MappedInputStream for " + streamId);
                    streamIdToClientBufferObserver.put(streamId, clientBufferObserver);
                }

            } else {
                //This message is not a REQUEST so it is part of a client side stream
                if (streamId.isEmpty()) {
                    //TODO: Send error to client. Client must supply streamId if there is no replyTo
                    log.error("If replyTo is empty then streamId must be empty");
                    return;
                }
                //Get the corresponding stream and send on the message
                final BufferObserver clientBufferObserver = streamIdToClientBufferObserver.get(streamId);
                if (clientBufferObserver == null) {
                    //TODO: log error, this should not occur
                    log.error("Can't find stream for: " + streamId);
                    return;
                }
                switch (request.getType()) {
                    case NEXT:
                        clientBufferObserver.onNext(request.getMessage());
                        break;
                    case SINGLE:
                        clientBufferObserver.onSingle(request.getMessage());
                        streamIdToClientBufferObserver.remove(streamId);
                        break;
                    case COMPLETED:
                        //TODO: This also needs to be removed if the client gets disconnected
                        log.debug("Completed received. Removing MappedClientStream for " + streamId);
                        streamIdToClientBufferObserver.remove(streamId);
                        clientBufferObserver.onCompleted();
                        break;
                    case ERROR:
                        log.error("Received error in client stream");
                        log.debug("Removing MappedClientStream for " + streamId);
                        streamIdToClientBufferObserver.remove(streamId);
                        clientBufferObserver.onError(request.getMessage());
                        break;
                    default:
                        log.error("Unhandled message type: " + request.getType().name());
                }
                return;
            }

        }));
    }
}
