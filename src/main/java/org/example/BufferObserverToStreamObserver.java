package org.example;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.pilz.mqttwrap.Status;

class BufferObserverToStreamObserver<T> implements MPBufferObserver {
    final Parser<T> parser;
    final MPStreamObserver<T> streamObserver;

    BufferObserverToStreamObserver(Parser<T> parser, MPStreamObserver<T> streamObserver) {
        this.parser = parser;
        this.streamObserver = streamObserver;
    }


    @Override
    public void onNext(ByteString value) {
        try {
            streamObserver.onNext(parser.parseFrom(value));
        } catch (InvalidProtocolBufferException e) {
            Logit.error(e);
        }
    }

    @Override
    public void onLast(ByteString value) {
        try {
            streamObserver.onLast(parser.parseFrom(value));
        } catch (InvalidProtocolBufferException e) {
            Logit.error(e);
        }
    }

    @Override
    public void onError(ByteString error) {
        //TODO: Handle error
        Logit.error("Error");
        Status status = Status.newBuilder().setCode(500).setDescription("Could not parse error").build();
        try {
            status = Status.parseFrom(error);
        } catch (InvalidProtocolBufferException e) {
            Logit.error("Failed to parse error");
        }
        streamObserver.onError(new Throwable(status.getDescription()));
    }


}
