package com.pilz.errors;

import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.StreamToBufferObserver;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

public class ErrorsStub implements IErrorsService{

    final ProtoSender sender;

    public ErrorsStub(ProtoSender sender) {
        this.sender = sender;
    }

    @Override
    public void singleResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
        sender.sendRequest(IErrorsService.SINGLE_RESPONSE_WITH_ERROR, request,
                new StreamToBufferObserver<>(HelloReply.parser(), responseObserver));

    }

    @Override
    public void multiResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
        sender.sendRequest(IErrorsService.MULTI_RESPONSE_WITH_ERROR, request,
                new StreamToBufferObserver<>(HelloReply.parser(), responseObserver));

    }

    @Override
    public StreamObserver<HelloRequest> errorInClientStream(StreamObserver<HelloReply> singleResponse) {
        return sender.sendClientStreamingRequest(IErrorsService.ERROR_IN_CLIENT_STREAM,
                new StreamToBufferObserver<>(HelloReply.parser(), singleResponse));
    }

    @Override
    public void singleResponseWithRichError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
        sender.sendRequest(IErrorsService.SINGLE_RESPONSE_WITH_RICH_CUSTOM_ERROR, request,
                new StreamToBufferObserver<>(HelloReply.parser(), responseObserver));

    }

    @Override
    public void singleResponseWithRichCustomError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
        sender.sendRequest(IErrorsService.SINGLE_RESPONSE_WITH_RICH_CUSTOM_ERROR, request,
                new StreamToBufferObserver<>(HelloReply.parser(), responseObserver));

    }

    @Override
    public StreamObserver<HelloRequest> richErrorInClientStream(StreamObserver<HelloReply> singleResponse) {
        return sender.sendClientStreamingRequest(IErrorsService.RICH_ERROR_IN_CLIENT_STREAM,
                new StreamToBufferObserver<>(HelloReply.parser(), singleResponse));
    }
}
