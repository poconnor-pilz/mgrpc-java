package com.pilz.errors;

import com.google.protobuf.ByteString;
import com.pilz.mqttgrpc.BufferObserver;
import com.pilz.mqttgrpc.BufferToStreamObserver;
import com.pilz.mqttgrpc.SingleToStreamObserver;
import com.pilz.mqttgrpc.Skeleton;
import io.grpc.examples.helloworld.HelloRequest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ErrorsSkeleton implements Skeleton {

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
                service.singleResponseWithError(HelloRequest.parseFrom(request), new BufferToStreamObserver<>(responseObserver));
                return null;
        }

        log.error("Unmatched method: " + method);
        return null;    }
}
