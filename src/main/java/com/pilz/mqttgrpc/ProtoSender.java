package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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

import static com.pilz.mqttgrpc.Consts.IN;
import static com.pilz.mqttgrpc.Consts.SVC;


public class ProtoSender {

    private static Logger log = LoggerFactory.getLogger(ProtoSender.class);

    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 10 * 1000;

    private final MqttAsyncClient client;

    /**
     * The topic prefix of the server e.g. devices/device1
     * The LWT of the server will be expected to be at "server/o/sys/status/getStatus
     */
    private final String serverTopic;


    public ProtoSender(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopic = serverTopic;
    }


    /**
     * Send a request to a service method
     *
     * @param method           The name of the method to call
     * @param message          The payload of the message
     * @param responseObserver Listener for responses if this send is a request. Null if this send is part of an input
     *                         stream
     * @throws Exception
     */
    public void sendRequest(String serviceName, String method, MessageLite message, BufferObserver responseObserver) {
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.REQUEST)
                .setMessage(message.toByteString());
        send(serviceName, method, request, responseObserver);
    }


    /**
     * Send a request to a service method that takes an input stream
     *
     * @param method           The name of the method to call
     * @param responseObserver Listener for responses if this send is a request. Null if this send is part of an input
     *                         stream
     * @throws Exception
     */
    public ClientStreamObserverToSender sendClientStreamingRequest(String serviceName, String method, BufferObserver responseObserver) {
        final String streamId = Base64Utils.randomId();
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.REQUEST)
                .setStreamId(streamId);
        send(serviceName, method, request, responseObserver);
        return new ClientStreamObserverToSender<>(this, serviceName, method, streamId);
    }


    /**
     * Send an error to a method that takes a input stream (client side stream)
     *
     * @param method   The name of the method to call
     * @param error    The error
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendClientStreamError(String serviceName, String method, String streamId, ByteString error) throws MqttException {
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.ERROR)
                .setMessage(error)
                .setStreamId(streamId);
        send(serviceName, method, request);
    }


    /**
     * Send the completed token to method that takes a input stream (client side stream)
     *
     * @param method   The name of the method to call
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendClientStreamCompleted(String serviceName, String method, String streamId) throws MqttException {
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.COMPLETED)
                .setStreamId(streamId);
        send(serviceName, method, request);
    }

    /**
     * Send a stream value to a method that takes a input stream (client side stream)
     *
     * @param method   The name of the method to call
     * @param message  The payload of the message
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendClientStreamNext(String serviceName, String method, String streamId, MessageLite message) throws MqttException {
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.NEXT)
                .setMessage(message.toByteString())
                .setStreamId(streamId);
        send(serviceName, method, request);
    }

    private void send(String serviceName, String method, Builder request, BufferObserver responseObserver) {
        try {
            String replyTo = subscribeForReplies(serviceName, method, responseObserver);
            request.setReplyTo(replyTo);
            send(serviceName, method, request);
        } catch (MqttException e) {
            Status status = Status.UNAVAILABLE.withDescription("Mqtt publish failed: " + e.getMessage());
            final com.google.rpc.Status status1 = StatusProto.fromStatusAndTrailers(status, null);
            responseObserver.onError(status1.toByteString());
        }
    }

    private void send(String serviceName, String method, Builder request) throws MqttException {
        final String topic = TopicMaker.make(this.serverTopic, IN, SVC, serviceName, method);
        client.publish(topic, new MqttMessage(request.build().toByteArray()));
    }


    private String subscribeForReplies(String serviceName, String method, BufferObserver protoListener) throws MqttException {
        final String replyTo = TopicMaker.make(this.serverTopic, Consts.OUT, SVC, serviceName, method, Base64Utils.randomId());
        log.debug("Subscribing for responses on: " + replyTo);
        final IMqttMessageListener messageListener = new MqttExceptionLogger((String topic, MqttMessage message) -> {
            log.debug("Received response on: " + topic);
            MqttGrpcResponse response = null;
            try {
                response = MqttGrpcResponse.parseFrom(message.getPayload());
            } catch (InvalidProtocolBufferException ex) {
                client.unsubscribe(replyTo);
                protoListener.onError(StatusProto.fromThrowable(ex).toByteString());
                return;
            }

            switch (response.getType()) {
                case NEXT:
                    protoListener.onNext(response.getMessage());
                    break;
                case SINGLE:
                    protoListener.onSingle(response.getMessage());
                    //This is the only message in the stream so unsubscribe
                    client.unsubscribe(replyTo);
                    break;
                case ERROR:
                    log.debug("ERROR received Unsubscribing to: " + replyTo);
                    client.unsubscribe(replyTo);
                    protoListener.onError(response.getMessage());
                    break;
                case COMPLETED:
                    log.debug("COMPLETED received Unsubscribing to: " + replyTo);
                    client.unsubscribe(replyTo);
                    protoListener.onCompleted();
                    break;
                default:
                    log.error("Unhandled message type");
            }
        });
        //This will throw an exception if it times out.
        client.subscribe(replyTo, 1, messageListener).waitForCompletion(SUBSCRIPTION_TIMEOUT_MILLIS);
        return replyTo;
    }

}
