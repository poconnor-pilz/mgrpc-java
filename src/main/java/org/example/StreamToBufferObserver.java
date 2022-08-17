package org.example;

import com.google.protobuf.MessageLite;
import io.grpc.stub.StreamObserver;


public class StreamToBufferObserver<T extends MessageLite> implements StreamObserver<T> {


    private final BufferObserver bufferObserver;

    public StreamToBufferObserver(BufferObserver bufferObserver) {
        this.bufferObserver = bufferObserver;
    }

    @Override
    public void onNext(T value) {
        bufferObserver.onNext(value.toByteString());
    }

    @Override
    public void onError(Throwable t) {
        Logit.error(t);
        //TODO: do something like this
        //replyListener.onError(new ByteString(t.getMessage()));

    }


    @Override
    public void onCompleted() {
        bufferObserver.onCompleted();
    }

}
