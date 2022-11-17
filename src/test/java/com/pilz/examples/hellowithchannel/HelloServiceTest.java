package com.pilz.examples.hellowithchannel;

import com.pilz.examples.hello.HelloService;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class HelloServiceTest extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

    private static Logger log = LoggerFactory.getLogger(HelloService.class);

    /**
     * @param request a single request
     * @param singleResponse a stream containing a single response
     */
    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloReply> singleResponse){
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
        singleResponse.onNext(reply);
        singleResponse.onCompleted();
    }

    /**
     * @param request A single request from the client
     * @param multipleResponses A stream of responses from the server
     */
    @Override
    public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> multipleResponses) {
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
        HelloReply reply2 = HelloReply.newBuilder().setMessage("Hello again " + request.getName()).build();
        //Send the same response twice in the stream
        multipleResponses.onNext(reply);
        multipleResponses.onNext(reply2);
        multipleResponses.onCompleted();
    }

    /**
     * @param singleResponse A single response from the service
     * @return A stream on which the client can send multiple requests to the server
     */
    @Override
    public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> singleResponse) {

        return new StreamObserver<HelloRequest>() {
            private ArrayList<String> names = new ArrayList<>();
            @Override
            public void onNext(HelloRequest value) {
                log.debug("lotsOfGreetings received " + value);
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
    public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> multipleResponses) {
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
