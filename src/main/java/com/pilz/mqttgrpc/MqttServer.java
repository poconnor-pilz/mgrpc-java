package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MqttServer {

    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private final MqttInternalHandlerRegistry registry = new MqttInternalHandlerRegistry();

    private final MqttAsyncClient client;

    private final Map<String, MqttServerCall> requestIdToServerCall = new ConcurrentHashMap<>();

    private final String serverTopic;

    public MqttServer(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopic = serverTopic;
    }

    private class MqttServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {

        final MethodDescriptor<ReqT, RespT> methodDescriptor;
        final MqttAsyncClient client;
        final String replyTo;
        final String requestId;
        private Listener listener;


        MqttServerCall(MqttAsyncClient client, MethodDescriptor<ReqT, RespT> methodDescriptor, String replyTo, String requestId) {
            this.methodDescriptor = methodDescriptor;
            this.client = client;
            this.replyTo = replyTo;
            this.requestId = requestId;
        }

        public void setListener(Listener listener){
            this.listener = listener;
        }

        public Listener getListener(){
            return this.listener;
        }

        @Override
        public void request(int numMessages) {

        }

        @Override
        public void sendHeaders(Metadata headers) {

        }

        @Override
        public void sendMessage(RespT message) {
            //Send the response up to the client
            try {
                final ByteString msgBytes = ((MessageLite) message).toByteString();
                MqttGrpcResponse reply = MqttGrpcResponse.newBuilder()
                        .setRequestId(requestId)
                        .setMessage(msgBytes)
                        .setType(MqttGrpcType.NEXT).build();
                client.publish(replyTo, reply.toByteArray(), 1, false);
            } catch (Exception e) {
                //We can only log the exception here as the broker is broken
                log.error("Publish response to client failed", e);
            }

        }

        @Override
        public void close(Status status, Metadata trailers) {
            sendStatus(replyTo, requestId, status);
            requestIdToServerCall.remove(requestId);
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return methodDescriptor;
        }
    }


    public void addService(BindableService service) {
        //TODO: Make a removeService
        registry.addService(service);
    }

    public void init() throws MqttException {
        String allServicesIn = Topics.allServicesIn(serverTopic);
        log.debug("subscribe server at: " + allServicesIn);

        client.subscribe(allServicesIn, 1, new MqttExceptionLogger((String topic, MqttMessage message) -> {
            //We use an MqttExceptionLogger here because if a we throw an exception in the subscribe handler
            //it will disconnect the mqtt client
            log.debug("Received message on : " + topic);
            //TODO: this should be in thread pool. Currently each request will be serialized through this
            final MqttGrpcRequest request = MqttGrpcRequest.parseFrom(message.getPayload());
            if (request.getRequestId().isEmpty()) {
                log.error("Every message sent from the client must have a requestId");
                return;
            }

            final MqttServerCall serverCall = requestIdToServerCall.get(request.getRequestId());
            if (serverCall != null) {
                if (request.getType() == MqttGrpcType.COMPLETED) {
                    serverCall.getListener().onHalfClose();
                    requestIdToServerCall.remove(request.getRequestId());
                } else if (request.getType() == MqttGrpcType.NEXT) {
                    //TODO: What if this does not match the request type, the parse will not fail
                    Object requestParam = serverCall.getMethodDescriptor().parseRequest(request.getMessage().newInput());
                    serverCall.getListener().onMessage(requestParam);
                } else {
                    requestIdToServerCall.remove(request.getRequestId());
                    sendStatus(request.getReplyTo(), request.getRequestId(),
                            Status.INTERNAL.withDescription("Unexpected message type: " + request.getType()));
                }
                return;
            }

            if (request.getType() != MqttGrpcType.NEXT) {
                sendStatus(request.getReplyTo(), request.getRequestId(),
                        Status.INTERNAL.withDescription("Unexpected message type: " + request.getType()));
                return;
            }

            //This is the first message for the request so lookup the method
            String fullMethodName = topic.substring(topic.lastIndexOf('/', topic.lastIndexOf('/') - 1) + 1);
            //fullMethodName is e.g. "helloworld.ExampleHelloService/SayHello"
            final ServerMethodDefinition<?, ?> serverMethodDefinition = registry.lookupMethod(fullMethodName);
            if (serverMethodDefinition == null) {
                sendStatus(request.getReplyTo(), request.getRequestId(),
                        Status.UNIMPLEMENTED.withDescription("No method registered for " + fullMethodName));
                return;
            }

            InputStream stream = new ByteArrayInputStream(request.getMessage().toByteArray());
            //TODO: What if this does not match the request type, the parse will not fail
            Object requestParam = serverMethodDefinition.getMethodDescriptor().parseRequest(stream);
            final ServerCallHandler<?, ?> serverCallHandler = serverMethodDefinition.getServerCallHandler();
            final MqttServerCall mqttServerCall = new MqttServerCall<>(client, serverMethodDefinition.getMethodDescriptor(),
                    request.getReplyTo(), request.getRequestId());
            final ServerCall.Listener listener = serverCallHandler.startCall(mqttServerCall, new Metadata());
            listener.onMessage(requestParam);

            if (serverMethodDefinition.getMethodDescriptor().getType().clientSendsOneMessage()) {
                //We do not expect the client to send a completed if there is only one message
                listener.onHalfClose();
                return;
            }

            mqttServerCall.setListener(listener);
            requestIdToServerCall.put(request.getRequestId(), mqttServerCall);

            //TODO: Check this for leaks. How can we be sure everything is gc'd


        })).waitForCompletion(20000);

    }

    public void close(){
        try {
            //TODO: make const timeout, cancel all calls? Empty map?
            client.unsubscribe(Topics.allServicesIn(serverTopic)).waitForCompletion(5000);
        } catch (MqttException e) {
            log.error("Failed to unsub", e);
        }
    }

    private void sendStatus(String replyTo, String requestId, Status status) {
        final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        MqttGrpcResponse reply = MqttGrpcResponse.newBuilder()
                .setRequestId(requestId)
                .setMessage(grpcStatus.toByteString())
                .setType(MqttGrpcType.COMPLETED).build();
        try {
            log.error("Sending error: " + status);
            client.publish(replyTo, reply.toByteArray(), 1, false);
        } catch (MqttException e) {
            //We can only log the exception here as the broker is broken
            log.error("Failed to publish error");
        }

    }


}
