package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.pilz.mqttgrpc.MqttGrpcRequest.Builder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class MqttGrpcClient implements Closeable {

    private static Logger log = LoggerFactory.getLogger(MqttGrpcClient.class);

    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 10 * 1000;

    private final MqttAsyncClient mqttAsyncClient;

    /**
     * The topic prefix of the server e.g. devices/device1
     * The LWT of the server will be expected to be at "server/o/sys/status/getStatus
     */
    private final String serverTopic;

    private boolean serverConnected = false;

    private Collection<BufferObserver> responseObservers = Collections.synchronizedCollection(new ArrayList<>());


    public MqttGrpcClient(MqttAsyncClient mqttAsyncClient, String serverTopic) {
        this.mqttAsyncClient = mqttAsyncClient;
        this.serverTopic = serverTopic;

    }

    /**
     * This will cause the MqttGrpcClient to listen for server lwt messages so that it can then
     * send an error to any response observers when the server is disconnected.
     * A client must call init() before using any other methods on the MqttGrpcClient.
     * @throws StatusException
     */
    public void init() throws StatusException {


        CountDownLatch latch = new CountDownLatch(1);
        //Subscribe for the lwt status message of the server
        try {
            this.mqttAsyncClient.subscribe(Topics.systemStatus(serverTopic), 1, new MqttExceptionLogger((String topic, MqttMessage message) -> {
                final ConnectionStatus connectionStatus = ConnectionStatus.parseFrom(message.getPayload());
                this.serverConnected = connectionStatus.getConnected();
                log.debug("Received server connection status: " + serverConnected);
                if (!serverConnected) {
                    //The server is disconnected so send a "Server unavailable" to every observer and then remove them all.
                    //If we get a server re-connected then it is still valid to use this client
                    //But any existing method calls or streams are invalid.
                    //TODO: How will a client distinguish between this and an unavailable because the client connection is down?
                    //Will that even matter if the client connection retries automatically?
                    //If the client connection does re-connect then this init will have to be called again.
                    final Iterator<BufferObserver> iter = responseObservers.iterator();
                    while (iter.hasNext()) {
                        final BufferObserver responseObserver = iter.next();
                        Status status = Status.UNAVAILABLE.withDescription("Server unavaliable");
                        final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
                        responseObserver.onError(grpcStatus.toByteString());
                    }
                    responseObservers.clear();
                }
                latch.countDown();
                //Wait 10 seconds for completion but this should complete straight away most of the time
            })).waitForCompletion(5000);

            //Send an empty message to the statusprompt topic to prompt the server to send it's status
            this.mqttAsyncClient.publish(Topics.systemStatusPrompt(serverTopic), new MqttMessage()).waitForCompletion();

        } catch (MqttException e) {
            throw new StatusException(Status.UNAVAILABLE.withCause(e));
        }

        //If the server is not running then it will not send a response and the latch will timeout
        try {
            if (!latch.await(10000, TimeUnit.MILLISECONDS)) {
                throw new StatusException(Status.UNAVAILABLE.withDescription("Server unavailable"));
            }
        } catch (InterruptedException e) {
            throw new StatusException(Status.UNAVAILABLE.withCause(e));
        }


    }

    @Override
    public void close() {
        try{
            this.mqttAsyncClient.unsubscribe(Topics.systemStatus(serverTopic));
            final Iterator<BufferObserver> iter = responseObservers.iterator();
            while (iter.hasNext()) {
                final BufferObserver responseObserver = iter.next();
                Status status = Status.UNAVAILABLE.withDescription("MqttGrpcClient closed");
                final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
                responseObserver.onError(grpcStatus.toByteString());
            }
        } catch (Exception ex){
            log.error("Exception during close", ex);
        }
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
    public <T extends MessageLite> StreamObserver<T> sendClientStreamingRequest(String serviceName, String method, BufferObserver responseObserver) {

        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.REQUEST);
        final String requestId = send(serviceName, method, request, responseObserver);

        return new StreamObserver<T>() {
            @Override
            public void onNext(T value) {
                try {
                    sendClientStreamNext(serviceName, method, requestId, value);
                } catch (StatusException e) {
                    //If there is an error sending a client stream value then send that error
                    //on to the original responseObserver associated with the method call
                    //It would probably also be possible here to just throw a StatusRuntimeException
                    //directly but that wouldn't work in the case where you want to chain ClientStreamObservers
                    //via network hops (if there even is such a use case)
                    responseObserver.onError(StatusProto.fromThrowable(e).toByteString());
                    //Because the responseObserver is in error we need to make sure it is removed
                    MqttGrpcClient.this.responseObservers.remove(responseObserver);
                }
            }
            @Override
            public void onError(Throwable t) {
                try {
                    sendClientStreamError(serviceName, method, requestId, StatusProto.fromThrowable(t).toByteString());
                } catch (StatusException e) {
                    responseObserver.onError(StatusProto.fromThrowable(e).toByteString());
                    MqttGrpcClient.this.responseObservers.remove(responseObserver);
                }
            }
            @Override
            public void onCompleted() {
                try {
                    sendClientStreamCompleted(serviceName, method, requestId);
                } catch (StatusException e) {
                    responseObserver.onError(StatusProto.fromThrowable(e).toByteString());
                    MqttGrpcClient.this.responseObservers.remove(responseObserver);
                }

            }
        };
    }


    /**
     * Send an error to a method that takes a input stream (client side stream)
     *
     * @param method   The name of the method to call
     * @param error    The error
     * @param requestId The stream id if the service method takes an input stream from the client otherwise null
     * @throws StatusException
     */
    public void sendClientStreamError(String serviceName, String method, String requestId, ByteString error) throws StatusException {
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.ERROR)
                .setMessage(error)
                .setRequestId(requestId);
        send(serviceName, method, request);
    }


    /**
     * Send the completed token to method that takes a input stream (client side stream)
     *
     * @param method   The name of the method to call
     * @param requestId The stream id if the service method takes an input stream from the client otherwise null
     * @throws StatusException
     */
    public void sendClientStreamCompleted(String serviceName, String method, String requestId) throws StatusException {
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.COMPLETED)
                .setRequestId(requestId);
        send(serviceName, method, request);
    }

    /**
     * Send a stream value to a method that takes a input stream (client side stream)
     *
     * @param method   The name of the method to call
     * @param message  The payload of the message
     * @param requestId The stream id if the service method takes an input stream from the client otherwise null
     * @throws StatusException
     */
    public void sendClientStreamNext(String serviceName, String method, String requestId, MessageLite message) throws StatusException {
        Builder request = MqttGrpcRequest.newBuilder()
                .setType(MqttGrpcType.NEXT)
                .setMessage(message.toByteString())
                .setRequestId(requestId);
        send(serviceName, method, request);
    }

    private String send(String serviceName, String method, Builder request, BufferObserver responseObserver) {

        String requestId = Base64Utils.randomId();
        try {
            request.setRequestId(requestId);
            String replyTo = subscribeForReplies(serviceName, method, requestId, responseObserver);
            request.setReplyTo(replyTo);
            send(serviceName, method, request);
            return requestId;
        } catch (StatusException e) {
            final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(e.getStatus(), null);
            responseObserver.onError(grpcStatus.toByteString());
        }
        return requestId;
    }

    private void send(String serviceName, String method, Builder request) throws StatusException {
        if (!serverConnected) {
            throw new StatusException(Status.UNAVAILABLE.withDescription("Server unavailable or MqttGrpcClient.init() was not called"));
        }
        try {
            mqttAsyncClient.publish(Topics.methodIn(serverTopic, serviceName, method),
                    new MqttMessage(request.build().toByteArray()));
        } catch (MqttException e) {
            throw new StatusException(Status.UNAVAILABLE.fromThrowable(e));
        }
    }


    private String subscribeForReplies(String serviceName, String method, String requestId, BufferObserver responseObserver) throws StatusException {
        if (!serverConnected) {
            throw new StatusException(Status.UNAVAILABLE.withDescription("Server unavailable or MqttGrpcClient.init() was not called"));
        }
        final String replyTo = Topics.replyTo(serverTopic, serviceName, method, requestId);
        log.debug("Subscribing for responses on: " + replyTo);
        final IMqttMessageListener messageListener = new MqttExceptionLogger((String topic, MqttMessage message) -> {
            log.debug("Received response on: " + topic);
            MqttGrpcResponse response = null;
            try {
                response = MqttGrpcResponse.parseFrom(message.getPayload());
            } catch (InvalidProtocolBufferException ex) {
                mqttAsyncClient.unsubscribe(replyTo);
                responseObserver.onError(StatusProto.fromThrowable(ex).toByteString());
                return;
            }
            switch (response.getType()) {
                case NEXT:
                    responseObserver.onNext(response.getMessage());
                    break;
                case SINGLE:
                    responseObserver.onSingle(response.getMessage());
                    //This is the only message in the stream so unsubscribe
                    mqttAsyncClient.unsubscribe(replyTo);
                    break;
                case ERROR:
                    log.debug("ERROR received Unsubscribing to: " + replyTo);
                    responseObservers.remove(responseObserver);
                    mqttAsyncClient.unsubscribe(replyTo);
                    responseObserver.onError(response.getMessage());
                    break;
                case COMPLETED:
                    log.debug("COMPLETED received Unsubscribing to: " + replyTo);
                    responseObservers.remove(responseObserver);
                    mqttAsyncClient.unsubscribe(replyTo);
                    responseObserver.onCompleted();
                    break;
                default:
                    log.error("Unhandled message type");
            }
        });
        //This will throw an exception if it times out.
        try {
            mqttAsyncClient.subscribe(replyTo, 1, messageListener).waitForCompletion(SUBSCRIPTION_TIMEOUT_MILLIS);
        } catch (MqttException e) {
            throw new StatusException(Status.UNAVAILABLE.fromThrowable(e));
        }
        responseObservers.add(responseObserver);
        return replyTo;
    }

}
