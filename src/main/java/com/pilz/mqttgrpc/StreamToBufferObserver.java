package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert a StreamObserver to a BufferObserver
 */
public class StreamToBufferObserver<T> implements BufferObserver {
    private static Logger log = LoggerFactory.getLogger(StreamToBufferObserver.class);
    final Parser<T> parser;
    final StreamObserver<T> streamObserver;

    public StreamToBufferObserver(Parser<T> parser, StreamObserver<T> streamObserver) {
        this.parser = parser;
        this.streamObserver = streamObserver;
    }


    @Override
    public void onNext(ByteString value) {
        try {
            streamObserver.onNext(parser.parseFrom(value));
        } catch (InvalidProtocolBufferException e) {
            log.error("Can't parse protobuf",e);
        }
    }


    public void onCompleted(){
        streamObserver.onCompleted();
    }

    @Override
    public void onSingle(ByteString value) {
        //If there is a single value in a stream  then send the streamObserver
        //an onNext() followed by an onCompleted(). Using onSingle means that every request/response can
        //be completed with two mqtt messages instead of three.
        try {
            streamObserver.onNext(parser.parseFrom(value));
        } catch (InvalidProtocolBufferException e) {
            log.error("Can't parse protobuf", e);
        }
        streamObserver.onCompleted();
    }

    @Override
    public void onError(ByteString error) {
        try {
            final com.google.rpc.Status status = com.google.rpc.Status.parseFrom(error);
            streamObserver.onError(StatusProto.toStatusRuntimeException(status));
        } catch (InvalidProtocolBufferException e) {
            streamObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription("Could not parse error")));
        }
    }



}
