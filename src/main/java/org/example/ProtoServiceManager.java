package org.example;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pilz.mqttwrap.MqttPType;
import com.pilz.mqttwrap.MqttProtoReply;
import com.pilz.mqttwrap.MqttProtoRequest;
import com.pilz.mqttwrap.Status;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.HashMap;
import java.util.Map;

public class ProtoServiceManager {

    /**
     * BufferObserver that just takes Buffer values and publishes them to replyTo with the correct type
     * (NEXT, COMPLETED ...)
     */
    private class ServerBufferPublisher implements BufferObserver{

        private final String replyTo;

        private ServerBufferPublisher(String replyTo) {
            this.replyTo = replyTo;
        }

        @Override
        public void onNext(ByteString value) {
            MqttProtoReply reply = MqttProtoReply.newBuilder()
                    .setMessage(value)
                    .setType(MqttPType.NEXT).build();
            try {
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onSingle(ByteString value) {
            MqttProtoReply reply = MqttProtoReply.newBuilder()
                    .setMessage(value)
                    .setType(MqttPType.SINGLE).build();
            try {
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onCompleted() {
            MqttProtoReply reply = MqttProtoReply.newBuilder()
                    .setType(MqttPType.COMPLETED).build();
            try {
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public void onError(ByteString error) {
            //TODO: Handle error - send it on to the broker
            try {
                Logit.error(Status.parseFrom(error).toString());
            } catch (InvalidProtocolBufferException e) {
                Logit.error("Failed to parse error");
            }
        }


    }


    private final MqttAsyncClient client;

    private final Map<String, BufferObserver> streamIdToClientBufferObserver = new HashMap<>();

    public ProtoServiceManager(MqttAsyncClient client) {
        this.client = client;
    }


   public void subscribeService(String serviceBaseTopic, MqttProtoService mqttProtoService) throws Exception{
        String topic = serviceBaseTopic + "/" + MqttProtoConsts.IN + "/#";
        Logit.log("subscribeService: " + topic);
        client.subscribe(topic, 1, new IMqttMessageListener() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                //TODO: must handle all exceptions here (user mqttexceptionhandler)
                //or else the exception will disconnect the mqtt client
                Logit.log("ProtoServiceManager received message on : " + topic);
                //TODO: this should be in thread pool
                //TODO: Error handling needed everywhere
                final MqttProtoRequest request = MqttProtoRequest.parseFrom(message.getPayload());
                //TODO: When we unsubscribe will this protoService be garbage collected?
                //Maybe it should be in a map. instead and just have a single listener on the MqttProto class
                //In fact the MqttProto class could just do one subscribe for rootOfAllServices/#
                String method = topic.substring(serviceBaseTopic.length() + MqttProtoConsts.IN.length() + 2);
                ByteString params = request.getMessage();
                String replyTo = request.getReplyTo();
                String streamId = request.getStreamId();

                if(request.getType() == MqttPType.REQUEST) {
                    BufferObserver clientBufferObserver = mqttProtoService.onProtoRequest(method, params,
                            new ServerBufferPublisher(replyTo));

                    if (clientBufferObserver != null) {
                        if (streamId.isEmpty()) {
                            //TODO: Send error to client. Client must supply streamId if the service has an input stream
                            Logit.error("No streamId supplied for client stream request");
                            return;
                        }
                        //Store the input stream so that we can send later messages to it if they have the same streamId
                        Logit.log("Setting up MappedInputStream for " + streamId);
                        streamIdToClientBufferObserver.put(streamId, clientBufferObserver);
                    }

                } else {
                    //This message is not a REQUEST so it is part of a client side stream
                    if (streamId.isEmpty()) {
                        //TODO: Send error to client. Client must supply streamId if there is no replyTo
                        Logit.error("If replyTo is empty then streamId must be empty");
                        return;
                    }
                    //Get the corresponding stream and send on the message
                    final BufferObserver clientBufferObserver = streamIdToClientBufferObserver.get(streamId);
                    if (clientBufferObserver == null) {
                        //TODO: log error, this should not occur
                        Logit.error("Can't find stream for: " + streamId);
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
                            Logit.log("Completed received. Removing MappedClientStream for " + streamId);
                            streamIdToClientBufferObserver.remove(streamId);
                            clientBufferObserver.onCompleted();
                            break;
                        case ERROR:
                            Logit.error("Received error in client stream");
                            Logit.log("Removing MappedClientStream for " + streamId);
                            streamIdToClientBufferObserver.remove(streamId);
                            clientBufferObserver.onError(request.getMessage());
                            break;
                        default:
                            Logit.error("Unhandled message type");
                    }
                    return;
                }

            }
        });
   }
}
