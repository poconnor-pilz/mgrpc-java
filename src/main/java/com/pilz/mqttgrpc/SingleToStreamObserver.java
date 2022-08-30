package com.pilz.mqttgrpc;

import com.google.protobuf.MessageLite;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert a BufferObserver to a StreamObserver where the StreamObserver sends only one value.
 * Use this class for service methods that are expected to only send a single response.
 * It will be more effcient than using a StreamToBufferObserver as it will only send
 * one onSingle message for the response (instead of onNext and onCompleted)
 * @param <T>
 */
public class SingleToStreamObserver<T extends MessageLite> implements StreamObserver<T> {

    private static Logger log = LoggerFactory.getLogger(SingleToStreamObserver.class);
    private final BufferObserver bufferObserver;
    private boolean receivedOne = false;

    public SingleToStreamObserver(BufferObserver bufferObserver) {
        this.bufferObserver = bufferObserver;
    }

    @Override
    public void onNext(T value) {
        if(receivedOne){
            log.error("Received more than one value for a stream that only expects one");
        } else {
            receivedOne = true;
            bufferObserver.onSingle(value.toByteString());
        }
    }

    @Override
    public void onError(Throwable t) {
        bufferObserver.onError(StatusProto.fromThrowable(t).toByteString());
    }


    @Override
    public void onCompleted() {
        //Ignore the onCompleted. BufferObserver.onSingle will have managed it
    }
}
