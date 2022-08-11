package org.example;

import com.google.protobuf.MessageLite;

public class StreamObserverToBufferObserver<T extends MessageLite> implements MPStreamObserver<T> {

    private final MPBufferObserver replyListener;

    public StreamObserverToBufferObserver(MPBufferObserver bufferObserver) {
        this.replyListener = bufferObserver;
    }

    @Override
    public void onNext(T value) {
        replyListener.onNext(value.toByteString());
    }

    @Override
    public void onError(Throwable t) {
        Logit.error(t);
        //TODO: do something like this
        //replyListener.onError(new ByteString(t.getMessage()));

    }

    @Override
    public void onLast(T value) {
        replyListener.onLast(value.toByteString());
    }
}
