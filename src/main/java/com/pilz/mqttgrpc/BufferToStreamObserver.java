package com.pilz.mqttgrpc;

import com.google.protobuf.MessageLite;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Convert a BufferObserver to a StreamObserver
 */
public class BufferToStreamObserver<T extends MessageLite> implements StreamObserver<T> {

    private static Logger log = LoggerFactory.getLogger(BufferToStreamObserver.class);

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
        bufferObserver.onError(StatusProto.fromThrowable(t).toByteString());
    }


    @Override
    public void onCompleted() {
        bufferObserver.onCompleted();
    }

}
