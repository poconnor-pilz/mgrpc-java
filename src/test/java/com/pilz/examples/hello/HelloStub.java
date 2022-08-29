package com.pilz.examples.hello;

import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.StreamToBufferObserver;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

public class HelloStub implements IHelloService{

    final ProtoSender sender;

    public HelloStub(ProtoSender sender) {
        this.sender = sender;
    }

    @Override
    public void requestResponse(HelloRequest request, StreamObserver<HelloReply> singleResponse) {
        sender.sendRequest(IHelloService.REQUEST_RESPONSE, request,
                new StreamToBufferObserver<>(HelloReply.parser(), singleResponse));
    }

    @Override
    public void serverStream(HelloRequest request, StreamObserver<HelloReply> multipleResponses) {
        sender.sendRequest(IHelloService.SERVER_STREAM, request,
                new StreamToBufferObserver<>(HelloReply.parser(), multipleResponses));
    }

    @Override
    public StreamObserver<HelloRequest> clientStream(StreamObserver<HelloReply> singleResponse) {
        return sender.sendClientStreamingRequest(IHelloService.CLIENT_STREAM,
                new StreamToBufferObserver<>(HelloReply.parser(), singleResponse));
    }

    @Override
    public StreamObserver<HelloRequest> clientAndServerStream(StreamObserver<HelloReply> multipleResponses) {
        return sender.sendClientStreamingRequest(IHelloService.CLIENT_AND_SERVER_STREAM,
                new StreamToBufferObserver<>(HelloReply.parser(), multipleResponses));
    }
}
