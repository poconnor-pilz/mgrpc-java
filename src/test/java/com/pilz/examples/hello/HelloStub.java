package com.pilz.examples.hello;

import com.pilz.mqttgrpc.MqttGrpcClient;
import com.pilz.mqttgrpc.StreamToBufferObserver;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

public class HelloStub implements IHelloService{

    final MqttGrpcClient mgClient;
    final String serviceName;

    public HelloStub(MqttGrpcClient mgClient, String serviceName) {
        this.mgClient = mgClient;
        this.serviceName = serviceName;
    }

    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloReply> singleResponse) {
        mgClient.sendRequest(serviceName, IHelloService.SAY_HELLO, request,
                new StreamToBufferObserver<>(HelloReply.parser(), singleResponse));
    }

    @Override
    public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> multipleResponses) {
        mgClient.sendRequest(serviceName, IHelloService.LOTS_OF_REPLIES, request,
                new StreamToBufferObserver<>(HelloReply.parser(), multipleResponses));
    }

    @Override
    public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> singleResponse) {
        return mgClient.sendClientStreamingRequest(serviceName, IHelloService.LOTS_OF_GREETINGS,
                new StreamToBufferObserver<>(HelloReply.parser(), singleResponse));
    }

    @Override
    public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> multipleResponses) {
        return mgClient.sendClientStreamingRequest(serviceName, IHelloService.BIDI_HELLO,
                new StreamToBufferObserver<>(HelloReply.parser(), multipleResponses));
    }
}
