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

    private class MappedClientStream implements MPBufferObserver {

        private final MPBufferObserver serviceClientStream;
        private final String streamId;

        private MappedClientStream(MPBufferObserver serviceClientStream, String streamId) {
            this.serviceClientStream = serviceClientStream;
            this.streamId = streamId;
        }

        @Override
        public void onNext(ByteString value) {
            serviceClientStream.onNext(value);
        }


        @Override
        public void onCompleted() {
            serviceClientStream.onCompleted();
            //TODO: What if the client dies and never calls onLast. Then this object and the serviceInputStream
            //will leak. Put in something that can pick up the LWT of clients (like in IoT) and then remove
            //all MappedInputStreams associated with that client.
            //Maybe this can be propagated to here via the onError which will remove it anyway
            ProtoServiceManager.this.streamIdToClientStream.remove(this.streamId);
        }

        @Override
        public void onError(ByteString error) {
            serviceClientStream.onError(error);
            ProtoServiceManager.this.streamIdToClientStream.remove(this.streamId);
        }
    }

    private final MqttAsyncClient client;

    private final Map<String, MappedClientStream> streamIdToClientStream = new HashMap<>();

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
                    MPBufferObserver clientStream = mqttProtoService.onProtoRequest(method, params, new MPBufferObserver() {
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
                            //TODO: Handle error
                            try {
                                Logit.error(Status.parseFrom(error).toString());
                            } catch (InvalidProtocolBufferException e) {
                                Logit.error("Failed to parse error");
                            }
                        }

                    });

                    if (clientStream != null) {
                        if (streamId.isEmpty()) {
                            //TODO: Send error to client. Client must supply streamId if the service has an input stream
                            Logit.error("No streamId supplied for client stream request");
                            return;
                        }
                        //Store the input stream so that we can send later messages to it.
                        Logit.log("Setting up MappedInputStream for " + streamId);
                        streamIdToClientStream.put(streamId, new MappedClientStream(clientStream, streamId));
                    }

                } else {
                    //This message is not a REQUEST so it is part of a client side stream
                    if (streamId.isEmpty()) {
                        //TODO: Send error to client. Client must supply streamId if there is no replyTo
                        Logit.error("If replyTo is empty then streamId must be empty");
                        return;
                    }
                    //Get the corresponding stream and send on the message
                    final MappedClientStream mappedClientStream = streamIdToClientStream.get(streamId);
                    if (mappedClientStream == null) {
                        //TODO: log error, this should not occur
                        Logit.error("Can't find stream for: " + streamId);
                        return;
                    }
                    switch (request.getType()) {
                        case NEXT:
                            mappedClientStream.onNext(request.getMessage());
                            break;
                        case COMPLETED:
                            //TODO: This also needs to be removed if the client gets disconnected
                            Logit.log("Completed received. Removing MappedClientStream for " + streamId);
                            streamIdToClientStream.remove(streamId);
                            mappedClientStream.onCompleted();
                            break;
                        case ERROR:
                            Logit.error("Received error in client stream");
                            Logit.log("Removing MappedClientStream for " + streamId);
                            streamIdToClientStream.remove(streamId);
                            mappedClientStream.onError(request.getMessage());
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
