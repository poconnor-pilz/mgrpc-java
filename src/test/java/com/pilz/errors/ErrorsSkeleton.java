package com.pilz.errors;

import com.google.protobuf.ByteString;
import com.pilz.mqttgrpc.*;
import io.grpc.examples.helloworld.HelloRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorsSkeleton implements Skeleton {
    private static Logger log = LoggerFactory.getLogger(ErrorsSkeleton.class);
    final IErrorsService service;

    public ErrorsSkeleton(IErrorsService service) {
        this.service = service;
    }


    @Override
    public BufferObserver onRequest(String method, ByteString request, BufferObserver responseObserver) throws Exception {
        switch(method){

            case IErrorsService.SINGLE_RESPONSE_WITH_ERROR:
                service.singleResponseWithError(HelloRequest.parseFrom(request), new SingleToStreamObserver<>(responseObserver));
                return null;

            case IErrorsService.MULTI_RESPONSE_WITH_ERROR:
                service.multiResponseWithError(HelloRequest.parseFrom(request), new BufferToStreamObserver<>(responseObserver));
                return null;

            case IErrorsService.ERROR_IN_CLIENT_STREAM:
                return new StreamToBufferObserver<>(HelloRequest.parser(),
                        service.errorInClientStream(new SingleToStreamObserver<>(responseObserver)));

            case IErrorsService.SINGLE_RESPONSE_WITH_RICH_ERROR:
                service.singleResponseWithRichError(HelloRequest.parseFrom(request), new SingleToStreamObserver<>(responseObserver));
                return null;

            case IErrorsService.SINGLE_RESPONSE_WITH_RICH_CUSTOM_ERROR:
                service.singleResponseWithRichCustomError(HelloRequest.parseFrom(request), new SingleToStreamObserver<>(responseObserver));
                return null;

            case IErrorsService.RICH_ERROR_IN_CLIENT_STREAM:
                return new StreamToBufferObserver<>(HelloRequest.parser(),
                        service.richErrorInClientStream(new SingleToStreamObserver<>(responseObserver)));
        }

        log.error("Unmatched method: " + method);
        return null;    }
}
