package com.pilz.examples.hello;

import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
@Slf4j
public class HelloService implements IHelloService {

    /**
     * @param request a single request
     * @param singleResponse a stream containing a single response
     */
    @Override
    public void requestResponse(HelloRequest request, StreamObserver<HelloReply> singleResponse){
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
        singleResponse.onNext(reply);
        singleResponse.onCompleted();
    }

    /**
     * @param request A single request from the client
     * @param multipleResponses A stream of responses from the server
     */
    @Override
    public void serverStream(HelloRequest request, StreamObserver<HelloReply> multipleResponses) {
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
        multipleResponses.onNext(reply);
        multipleResponses.onNext(reply);
        multipleResponses.onCompleted();
    }

    /**
     * @param singleResponse A single response from the service
     * @return A stream on which the client can send multiple requests to the server
     */
    @Override
    public StreamObserver<HelloRequest> clientStream(StreamObserver<HelloReply> singleResponse) {

        return new StreamObserver<HelloRequest>() {
            private ArrayList<String> names = new ArrayList<>();
            @Override
            public void onNext(HelloRequest value) {
                names.add(value.getName());
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error in client stream", t);
            }

            @Override
            public void onCompleted() {
                String everyone = "";
                for(String name: names){
                    everyone += name + ",";
                }
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + everyone).build();
                singleResponse.onNext(reply);
                singleResponse.onCompleted();
            }
        };
    }

    /**
     * @param multipleResponses A stream of responses from the server
     * @return A stream on which the client can send multiple requests to the server
     */
    @Override
    public StreamObserver<HelloRequest> clientAndServerStream(StreamObserver<HelloReply> multipleResponses) {
        return new StreamObserver<HelloRequest>() {
            @Override
            public void onNext(HelloRequest value) {
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + value.getName()).build();
                multipleResponses.onNext(reply);
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error in client stream", t);
            }

            @Override
            public void onCompleted() {
                multipleResponses.onCompleted();
            }
        };
    }
}
