package com.pilz.mqttgrpc;

import com.google.protobuf.MessageLite;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;

public class ClientStreamObserverToSender<T extends MessageLite> implements StreamObserver<T> {

    private final ProtoSender protoSender;
    private final String service;
    private final String method;
    private final String streamId;

    public ClientStreamObserverToSender(ProtoSender protoSender, String service, String method, String streamId) {
        this.protoSender = protoSender;
        this.service = service;
        this.method = method;
        this.streamId = streamId;
    }

    @Override
    public void onNext(T value) {
        try {
            protoSender.sendClientStreamNext(service, method, streamId, value);
        } catch (Exception e) {
            //Throw on a Runtime exception to the client.
            //This is all we can do as the client is calling this as a StreamObserver which does not
            //have checked exceptions.
            Status status = io.grpc.Status.UNKNOWN.withDescription("Mqtt publish failed: " + e.getMessage());
            throw new StatusRuntimeException(status);
        }
    }

    @Override
    public void onError(Throwable t) {
        try {
            protoSender.sendClientStreamError(service, method, streamId, StatusProto.fromThrowable(t).toByteString());
        } catch (Exception e) {
            Status status = io.grpc.Status.UNKNOWN.withDescription("Mqtt publish failed: " + e.getMessage());
            throw new StatusRuntimeException(status);
        }
    }



    @Override
    public void onCompleted() {
        try {
            protoSender.sendClientStreamCompleted(service, method, streamId);
        } catch (Exception e) {
            Status status = io.grpc.Status.UNKNOWN.withDescription("Mqtt publish failed: " + e.getMessage());
            throw new StatusRuntimeException(status);
        }

    }
}
