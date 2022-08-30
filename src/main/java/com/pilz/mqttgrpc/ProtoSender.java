package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.pilz.mqttgrpc.MqttGrpcRequest.Builder;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProtoSender {

    private static Logger log = LoggerFactory.getLogger(ProtoSender.class);

    //TODO: Decrease this timeout
    public static final long REPLY_TIMEOUT_MILLIS = 100 * 1000;
    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 3 * 1000;

    private final MqttAsyncClient client;
    private final String serviceBaseTopic;

    public ProtoSender(MqttAsyncClient client, String serviceBaseTopic) {
        this.client = client;
        this.serviceBaseTopic = serviceBaseTopic;
    }



    /**
     * Send a request to a service method
     * @param method The name of the method to call
     * @param message The payload of the message
     * @param responseObserver Listener for responses if this send is a request. Null if this send is part of an input
     *                      stream
     * @throws Exception
     */
    public void sendRequest(String method, MessageLite message, BufferObserver responseObserver){
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.REQUEST)
                .setMessage(message.toByteString());
        send(method, request, responseObserver);
    }



    /**
     * Send a request to a service method that takes an input stream
     * @param method The name of the method to call
     * @param responseObserver Listener for responses if this send is a request. Null if this send is part of an input
     *                      stream
     * @throws Exception
     */
    public ClientStreamObserverToSender sendClientStreamingRequest(String method, BufferObserver responseObserver) {
        final String streamId = Base64Utils.randomId();
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.REQUEST)
                .setStreamId(streamId);
        send(method, request, responseObserver);
        return new ClientStreamObserverToSender<>(this, method, streamId);
    }



    /**
     * Send an error to a method that takes a input stream (client side stream)
     * @param method The name of the method to call
     * @param error The error
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendClientStreamError(String method, String streamId, ByteString error) throws MqttException{
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.ERROR)
                .setMessage(error)
                .setStreamId(streamId);
        send(method, request);
    }



    /**
     * Send the completed token to method that takes a input stream (client side stream)
     * @param method The name of the method to call
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendClientStreamCompleted(String method, String streamId) throws MqttException{
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.COMPLETED)
                .setStreamId(streamId);
        send(method, request);
    }

    /**
     * Send a stream value to a method that takes a input stream (client side stream)
     * @param method The name of the method to call
     * @param message The payload of the message
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendClientStreamNext(String method, String streamId, MessageLite message) throws MqttException{
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.NEXT)
                .setMessage(message.toByteString())
                .setStreamId(streamId);
        send(method, request);
    }

    private void send(String method, Builder request, BufferObserver responseObserver){
        try {
            String replyTo = subscribeForReplies(method, responseObserver);
            request.setReplyTo(replyTo);
            send(method, request);
        } catch (MqttException e) {
            //TODO: should this be UNKNOWN or should we encode our own?
            Status status = io.grpc.Status.UNKNOWN.withDescription("Mqtt publish failed: " + e.getMessage());
            final com.google.rpc.Status status1 = StatusProto.fromStatusAndTrailers(status, null);
            responseObserver.onError(status1.toByteString());
        }
    }

    private void send(String method, Builder request) throws MqttException{
        final String topic = serviceBaseTopic + '/' + Consts.IN + '/' + method;
        client.publish(topic, new MqttMessage(request.build().toByteArray()));
    }


    private String subscribeForReplies(String method, BufferObserver protoListener) throws MqttException{
        final String replyTo = serviceBaseTopic + '/' + Consts.OUT + '/' + method + '/' + Base64Utils.randomId();
       log.debug("ProtoSender subscribing for responses on: " + replyTo);
        final IMqttMessageListener messageListener = new MqttExceptionLogger((String topic, MqttMessage message)->{
           log.debug("ProtoSender received response on: " + topic);
            //TODO: catch parsing exception here
            MqttGrpcResponse response = MqttGrpcResponse.parseFrom(message.getPayload());
            switch(response.getType()){
                case NEXT:
                    protoListener.onNext(response.getMessage());
                    break;
                case SINGLE:
                    protoListener.onSingle(response.getMessage());
                    //This is the only message in the stream so unsubscribe
                    client.unsubscribe(replyTo);
                    break;
                case ERROR:
                   log.debug("ProtoSender ERROR received Unsubscribing to: " + replyTo);
                    client.unsubscribe(replyTo);
                    protoListener.onError(response.getMessage());
                    break;
                case COMPLETED:
                   log.debug("ProtoSender COMPLETED received Unsubscribing to: " + replyTo);
                    client.unsubscribe(replyTo);
                    protoListener.onCompleted();
                    break;
                default:
                    log.error("Unhandled message type");
            }
        });
        client.subscribe(replyTo, 1, messageListener).waitForCompletion(SUBSCRIPTION_TIMEOUT_MILLIS); //TODO: check for timeout
        return replyTo;
    }

}
