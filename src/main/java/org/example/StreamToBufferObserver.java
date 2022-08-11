package org.example;

import com.google.protobuf.MessageLite;

public class StreamToBufferObserver<T extends MessageLite> implements MPStreamObserver<T> {

    private final MPBufferObserver replyListener;

    public StreamToBufferObserver(MPBufferObserver bufferObserver) {
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
    public void onCompleted() {
        replyListener.onCompleted();
    }
}
