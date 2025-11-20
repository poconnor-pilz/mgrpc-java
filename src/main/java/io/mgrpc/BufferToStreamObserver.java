package io.mgrpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class BufferToStreamObserver {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Helper method to take a protocol buffer and push it to a stream
     * @param parser The parser corresponding to the stream type type e.g. HelloReply.parser()
     * @param message  rpc message
     * @param observer The stream
     * @return true if the stream is finished and associated resources should be closed
     * @param <T> Stream type
     * @throws MessagingException
     */
    public static <T> boolean convert(Parser<T> parser, RpcMessage message, final StreamObserver<T> observer) {


        switch (message.getMessageCase()) {
            case VALUE:
                final T value;
                try {
                    value = parser.parseFrom(message.getValue().getContents());
                } catch (InvalidProtocolBufferException e) {
                    log.error("Failed to parse Value", e);
                    observer.onError(new StatusRuntimeException(Status.INTERNAL.withDescription("Cannot parse response")));
                    return true;
                }
                observer.onNext(value);
                return false;
            case STATUS:
                //convert from google rpc status to grpc status
                Status status = Status.fromCodeValue(message.getStatus().getCode());
                status = status.withDescription(message.getStatus().getMessage());
                if (status.isOk()) {
                    observer.onCompleted();
                } else {
                    final StatusRuntimeException sre = new StatusRuntimeException(status, null);
                    observer.onError(sre);
                }
                return true;
        }
        observer.onError(new StatusRuntimeException(Status.INTERNAL.withDescription("Cannot parse response")));
        log.error("Unknown type of message");
        return true;
    }


}
