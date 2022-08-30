package com.pilz.errors;

import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

public interface IErrorsService {

    String SINGLE_RESPONSE_WITH_ERROR = "singleResponseWithError";
    String MULTI_RESPONSE_WITH_ERROR = "multiResponseWithError";
    String ERROR_IN_CLIENT_STREAM = "errorInClientStream";
    String SINGLE_RESPONSE_WITH_RICH_ERROR = "singleResponseWithRichError";
    String SINGLE_RESPONSE_WITH_RICH_CUSTOM_ERROR = "singleResponseWithRichCustomError";
    String RICH_ERROR_IN_CLIENT_STREAM = "richErrorInClientStream";

    void singleResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver);

    void multiResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver);

    StreamObserver<HelloRequest> errorInClientStream(StreamObserver<HelloReply> singleResponse);

    void singleResponseWithRichError(HelloRequest request, StreamObserver<HelloReply> responseObserver);

    void singleResponseWithRichCustomError(HelloRequest request, StreamObserver<HelloReply> responseObserver);

    StreamObserver<HelloRequest> richErrorInClientStream(StreamObserver<HelloReply> singleResponse);
}
