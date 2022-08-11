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

    private class MappedInputStream implements MPBufferObserver {

        private final MPBufferObserver serviceInputStream;
        private final String streamId;

        private MappedInputStream(MPBufferObserver serviceInputStream, String streamId) {
            this.serviceInputStream = serviceInputStream;
            this.streamId = streamId;
        }

        @Override
        public void onNext(ByteString value) {
            serviceInputStream.onNext(value);
        }

        @Override
        public void onLast(ByteString value) {
            serviceInputStream.onLast(value);
            //TODO: What if the client dies and never calls onLast. Then this object and the serviceInputStream
            //will leak. Put in something that can pick up the LWT of clients (like in IoT) and then remove
            //all MappedInputStreams associated with that client.
            //Maybe this can be propagated to here via the onError which will remove it anyway
            ProtoServiceManager.this.streamIdToInputStream.remove(this.streamId);
        }

        @Override
        public void onError(ByteString error) {
            serviceInputStream.onError(error);
            ProtoServiceManager.this.streamIdToInputStream.remove(this.streamId);
        }
    }

    private final MqttAsyncClient client;

    private final Map<String, MappedInputStream> streamIdToInputStream = new HashMap<>();

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

                if(replyTo.isEmpty()){
                    //This message is part of a client side stream
                    if(streamId.isEmpty()){
                        //TODO: Send error to client. Client must supply streamId if there is no replyTo
                        Logit.error("If replyTo is empty then streamId must be empty");
                        return;
                    }
                    //Get the corresponding stream and send on the message
                    final MappedInputStream mappedInputStream = streamIdToInputStream.get(streamId);
                    if(mappedInputStream == null){
                        //TODO: log error, this should not occur
                        Logit.error("Can't find stream for: " + streamId);
                        return;
                    }
                    switch(request.getType()){
                        case NEXT:
                            mappedInputStream.onNext(request.getMessage());
                            break;
                        case LAST:
                            //TODO: This also needs to be removed if the client gets disconnected
                            Logit.log("Removing MappedInputStream for " + streamId);
                            streamIdToInputStream.remove(streamId);
                            mappedInputStream.onLast(request.getMessage());
                            break;
                        case ERROR:
                            Logit.log("Removing MappedInputStream for " + streamId);
                            streamIdToInputStream.remove(streamId);
                            mappedInputStream.onError(request.getMessage());
                            break;
                        default:
                            Logit.error("Unhandled message type");
                    }
                    return;
                }

                MPBufferObserver inputStream = mqttProtoService.onProtoRequest(method, params, new MPBufferObserver() {
                    @Override
                    public void onNext(ByteString value) {
                        MqttProtoReply reply = MqttProtoReply.newBuilder()
                                .setMessage(value)
                                .setType(MqttPType.LAST).build();
                        try {
                            client.publish(replyTo, reply.toByteArray(), 1, false);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void onLast(ByteString value) {
                        MqttProtoReply reply = MqttProtoReply.newBuilder()
                                .setMessage(value)
                                .setType(MqttPType.LAST).build();
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

                if(inputStream != null){
                    if(streamId.isEmpty()){
                        //TODO: Send error to client. Client must supply streamId if the service has an input stream
                        return;
                    }
                    //Store the input stream so that we can send later messages to it.
                    Logit.log("Setting up MappedInputStream for " + streamId);
                    streamIdToInputStream.put(streamId, new MappedInputStream(inputStream, streamId));
                }
            }
        });
   }
}
