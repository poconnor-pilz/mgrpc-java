package com.pilz.errors;

import io.grpc.Status;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

public class ErrorsService implements IErrorsService {

    @Override
    public void singleResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {

        Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
        responseObserver.onError(status.asRuntimeException());
    }

    @Override
    public void multiResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
        responseObserver.onNext(reply);
        Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
        responseObserver.onError(status.asRuntimeException());
    }
}
