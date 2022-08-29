package com.pilz.errors;

import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

public interface IErrorsService {

    String SINGLE_RESPONSE_WITH_ERROR = "singleResponseWithError";
    String MULTI_RESPONSE_WITH_ERROR = "multiResponseWithError";

    void singleResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver);

    void multiResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver);
}
