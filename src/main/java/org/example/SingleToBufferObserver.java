package org.example;

import com.google.protobuf.MessageLite;
import io.grpc.stub.StreamObserver;
/**
 * Use this class for service methods that are expected to only send a single response.
 * It will be more effcient than using a StreamToBufferObserver as it will only send
 * one onSingle message for the response (instead of onNext and onCompleted)
 * @param <T>
 */
public class SingleToBufferObserver<T extends MessageLite> implements StreamObserver<T> {

    private final BufferObserver bufferObserver;
    private boolean receivedOne = false;

    public SingleToBufferObserver(BufferObserver bufferObserver) {
        this.bufferObserver = bufferObserver;
    }

    @Override
    public void onNext(T value) {
        if(receivedOne){
            Logit.error("Received more than one value for a stream that only expects one");
        } else {
            receivedOne = true;
            bufferObserver.onSingle(value.toByteString());
        }
    }

    @Override
    public void onError(Throwable t) {
        Logit.error(t);
        //TODO: do something like this
        //replyListener.onError(new ByteString(t.getMessage()));

    }


    @Override
    public void onCompleted() {
        //Ignore the onCompleted. BufferObserver.onSingle will have managed it
    }
}
