package org.example;

import com.google.protobuf.ByteString;

public interface BufferObserver {

    /**
     * @see io.grpc.stub.StreamObserver#onNext(Object)
     */
    void onNext(ByteString value);

    /**
     * @see io.grpc.stub.StreamObserver#onError(Throwable)
     * @param error
     */
    void onError(ByteString error);

    /**
     * @see io.grpc.stub.StreamObserver#onCompleted()
     */
    void onCompleted();


    /**
     * This is the only method that doesn't correspond to one from StreamObserver
     * It is when there is only a single value in a stream (i.e. a single reply)
     * Using onSingle to transfer the value over mqtt will save an mqtt message.
     * @param value
     */
    void onSingle(ByteString value);
}
