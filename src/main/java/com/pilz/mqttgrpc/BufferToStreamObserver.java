package com.pilz.mqttgrpc;

import com.google.protobuf.MessageLite;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;


@Slf4j
/**
 * Convert a BufferObserver to a StreamObserver
 */
public class BufferToStreamObserver<T extends MessageLite> implements StreamObserver<T> {


    private final BufferObserver bufferObserver;

    public BufferToStreamObserver(BufferObserver bufferObserver) {
        this.bufferObserver = bufferObserver;
    }

    @Override
    public void onNext(T value) {
        bufferObserver.onNext(value.toByteString());
    }

    @Override
    public void onError(Throwable t) {
        if(t instanceof StatusRuntimeException){
            Status status = ((StatusRuntimeException)t).getStatus();
            bufferObserver.onError(StatusConv.toBuffer(status).toByteString());
        } else {
            bufferObserver.onError(StatusConv.toBuffer(Status.UNKNOWN.withCause(t)).toByteString());
        }
    }


    @Override
    public void onCompleted() {
        bufferObserver.onCompleted();
    }

}
