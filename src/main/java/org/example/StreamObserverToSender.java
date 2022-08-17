package org.example;

import com.google.protobuf.MessageLite;
import io.grpc.stub.StreamObserver;

public class StreamObserverToSender<T extends MessageLite> implements StreamObserver<T> {

    private final ProtoSender protoSender;
    private final String method;
    private final String streamId;

    public StreamObserverToSender(ProtoSender protoSender, String method, String streamId) {
        this.protoSender = protoSender;
        this.method = method;
        this.streamId = streamId;
    }

    @Override
    public void onNext(T value) {
        try {
            protoSender.sendClientStreamNext(method, streamId, value);
        } catch (Exception e) {
            //TODO: handle error
            e.printStackTrace();
        }
    }

    @Override
    public void onError(Throwable t) {
        try {
            //TODO encode the exeption as a protobuf
            protoSender.sendClientStreamError(method, streamId, t.getMessage());
        } catch (Exception e) {
            //TODO: handle error
            e.printStackTrace();
        }
    }



    @Override
    public void onCompleted() {
        try {
            protoSender.sendClientStreamCompleted(method, streamId);
        } catch (Exception e) {
            //TODO: handle error
            e.printStackTrace();
        }

    }
}
