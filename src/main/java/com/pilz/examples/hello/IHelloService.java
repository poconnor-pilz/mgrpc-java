package com.pilz.examples.hello;

import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

public interface IHelloService {

    //Method names - These will be unnecessary when a grpc compiler plugin can generate skeleton and stub
    String REQUEST_RESPONSE = "requestResponse";
    String SERVER_STREAM = "serverStream";
    String CLIENT_STREAM = "clientStream";
    String CLIENT_AND_SERVER_STREAM = "clientAndServerStream";

    /**
     * @param request a single request
     * @param singleResponse a stream containing a single response
     */
    void requestResponse(HelloRequest request, StreamObserver<HelloReply> singleResponse);

    /**
     * @param request A single request from the client
     * @param multipleResponses A stream of responses from the server
     */
    void serverStream(HelloRequest request, StreamObserver<HelloReply> multipleResponses);

    /**
     * @param singleResponse A single response from the service
     * @return A stream on which the client can send multiple requests to the server
     */
    StreamObserver<HelloRequest> clientStream(StreamObserver<HelloReply> singleResponse);

    /**
     * @param multipleResponses A stream of responses from the server
     * @return A stream on which the client can send multiple requests to the server
     */
    StreamObserver<HelloRequest> clientAndServerStream(StreamObserver<HelloReply> multipleResponses);


}
