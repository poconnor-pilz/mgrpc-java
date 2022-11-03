package com.pilz.mqttgrpc;

import com.google.protobuf.MessageLite;
import io.grpc.*;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;

public class MqttChannel extends Channel {


    private static Logger log = LoggerFactory.getLogger(MqttChannel.class);
    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 10 * 1000;
    private final static Metadata EMPTY_METADATA = new Metadata();

    private final MqttAsyncClient mqttAsyncClient;
    /**
     * The topic prefix of the server e.g. devices/device1
     */
    private final String serverTopic;

    private boolean serverConnected;

    private final Collection<ClientCall.Listener<?>> listeners = Collections.synchronizedCollection(new ArrayList<>());

    public MqttChannel(MqttAsyncClient mqttAsyncClient, String serverTopic) {
        this.mqttAsyncClient = mqttAsyncClient;
        this.serverTopic = serverTopic;
    }

    /**
     * This will cause the MqttGrpcClient to listen for server lwt messages so that it can then
     * send an error to any response observers when the server is disconnected.
     * A client must call init() before using any other methods on the MqttGrpcClient.
     * @throws StatusException
     */
    public void init() throws StatusRuntimeException {
        //TODO: take the code from MqttGrpcClient and refactor it to work here.
        serverConnected = true;
    }


    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return new MqttCall<>(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
        return serverTopic;
    }


    private class MqttCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        final MethodDescriptor<ReqT, RespT> methodDescriptor;
        final CallOptions callOptions;

        final Executor callerExecutor;

        final String requestId;

        String replyTo;

        Listener<RespT> responseListener;

        private MqttCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
            this.methodDescriptor = methodDescriptor;
            this.callOptions = callOptions;
            this.requestId = Base64Uuid.id();
            //TODO: What do we default to if this is null?
            this.callerExecutor = callOptions.getExecutor();
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            this.responseListener = responseListener;
            this.replyTo = subscribeForReplies(methodDescriptor.getServiceName(),
                    methodDescriptor.getBareMethodName(),
                    requestId,
                    responseListener);
            //TODO should we call responseListener.onReady() here?
            exec(()->{responseListener.onReady();});
        }

        @Override
        public void request(int numMessages) {
            //Do nothing here as we don't implement backpressure.
            //This would be used to send a message to the service to tell it to sent on numMessages
            //But our services will send on messages when they have them (for the moment anyway)
            log.debug("request({})", numMessages);
        }

        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {

        }

        @Override
        public void halfClose() {
            //This will be sent by the client (e.g a stub) when the client stream is complete (or after one unary request)
            if(methodDescriptor.getType().clientSendsOneMessage()){
                //Don't send a completed as the server doesn't care and it's an extra unnecessary mqtt message
                return;
            }
            MqttGrpcRequest.Builder request = MqttGrpcRequest.newBuilder()
                    .setType(MqttGrpcType.COMPLETED)
                    .setRequestId(requestId);
            //TODO: If the send fails then should this send an error back to the listener?
            //or will the exception suffice?
            send(methodDescriptor.getServiceName(), methodDescriptor.getBareMethodName(), request);
        }

        @Override
        public void sendMessage(ReqT message) {
            MqttGrpcRequest.Builder request = MqttGrpcRequest.newBuilder()
                    .setType(MqttGrpcType.REQUEST)
                    .setMessage(((MessageLite)message).toByteString())
                    .setRequestId(requestId)
                    .setReplyTo(replyTo);
            //TODO: If the send fails then should this send an error back to the listener?
            //or will the exception suffice?
            send(methodDescriptor.getServiceName(), methodDescriptor.getBareMethodName(), request);
            responseListener.onReady();
        }

        private void send(String serviceName, String method, MqttGrpcRequest.Builder request) throws StatusRuntimeException {
            if (!serverConnected) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Server unavailable or init() was not called"));
            }
            try {
                final String topic = Topics.methodIn(serverTopic, serviceName, method);
                log.debug("Sending message on: " + topic);
                mqttAsyncClient.publish(topic, new MqttMessage(request.build().toByteArray()));
            } catch (MqttException e) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.fromThrowable(e));
            }
        }


        private String subscribeForReplies(String serviceName, String method, String requestId, Listener<RespT> responseListener) throws StatusRuntimeException {
            if (!serverConnected) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Server unavailable or MqttGrpcClient.init() was not called"));
            }
            final String replyTo = Topics.replyTo(serverTopic, serviceName, method, requestId);
            log.debug("Subscribing for responses on: " + replyTo);
            final IMqttMessageListener messageListener = new MqttExceptionLogger((String topic, MqttMessage message) -> {
                try {
                    final MqttGrpcResponse response = MqttGrpcResponse.parseFrom(message.getPayload());
                    switch (response.getType()) {
                        case NEXT:
                            log.debug("Received NEXT response on: " + topic);
                            responseListener.onMessage(methodDescriptor.parseResponse(response.getMessage().newInput()));
                            break;
                        case SINGLE:
                            log.debug("Received SINGLE response on: " + topic);
                            //This is the only message in the stream so unsubscribe
                            listeners.remove(responseListener);
                            exec(()->{
                                responseListener.onHeaders(EMPTY_METADATA);
                                responseListener.onMessage(methodDescriptor.parseResponse(response.getMessage().newInput()));
                                responseListener.onClose(Status.OK, EMPTY_METADATA);
                            });
                            mqttAsyncClient.unsubscribe(replyTo);
                            break;
                        case ERROR:
                            log.debug("Received ERROR response on: " + topic);
                            listeners.remove(responseListener);
                            final com.google.rpc.Status grpcStatus = com.google.rpc.Status.parseFrom(response.getMessage());
                            exec(()->{responseListener.onClose(toStatus(grpcStatus), EMPTY_METADATA);});
                            mqttAsyncClient.unsubscribe(replyTo);
                            break;
                        case COMPLETED:
                            log.debug("Received COMPLETED response on: " + topic);
                            listeners.remove(responseListener);
                            exec(()->{responseListener.onClose(Status.OK, EMPTY_METADATA);});
                            mqttAsyncClient.unsubscribe(replyTo);
                            break;
                        default:
                            log.error("Unhandled message type on: " + topic);
                    }
                } catch (Throwable t) {
                    mqttAsyncClient.unsubscribe(replyTo);
                    listeners.remove(responseListener);
                    exec(()->{responseListener.onClose(Status.fromThrowable(t), EMPTY_METADATA);});
                }
            });
            //This will throw an exception if it times out.
            try {
                mqttAsyncClient.subscribe(replyTo, 1, messageListener).waitForCompletion(SUBSCRIPTION_TIMEOUT_MILLIS);
            } catch (MqttException e) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.fromThrowable(e));
            }
            listeners.add(responseListener);
            return replyTo;
        }

        private void exec(Runnable runnable){
            if(this.callerExecutor != null){
                this.callerExecutor.execute(runnable);
            } else {
                runnable.run();
            }
        }

        //Copied more or less from StatusProto.toStatus() as that is private
        private Status toStatus(com.google.rpc.Status statusProto) {
            Status status = Status.fromCodeValue(statusProto.getCode());
            return status.withDescription(statusProto.getMessage());
        }
    }


}
