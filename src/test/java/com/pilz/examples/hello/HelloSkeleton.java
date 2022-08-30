package com.pilz.examples.hello;

import com.google.protobuf.ByteString;
import com.pilz.mqttgrpc.*;
import io.grpc.examples.helloworld.HelloRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloSkeleton implements Skeleton {
    private static Logger log = LoggerFactory.getLogger(HelloSkeleton.class);

    private final IHelloService service;

    public HelloSkeleton(IHelloService service) {
        this.service = service;
    }

    @Override
    public BufferObserver onRequest(String method, ByteString request, BufferObserver responseObserver) throws Exception {

        switch(method){

            case IHelloService.REQUEST_RESPONSE:
                //Use SingleToStreamObserver here because the service will only send one response.
                //It is not necessary to use this but it is more efficient. It will mean that one less
                //mqtt message is sent compared to using BufferToStreamObserver.
                service.requestResponse(HelloRequest.parseFrom(request), new SingleToStreamObserver<>(responseObserver));
                return null;

            case IHelloService.SERVER_STREAM:
                //Use BufferToStreamObserver here because the server stream will have more than one response.
                service.serverStream(HelloRequest.parseFrom(request), new BufferToStreamObserver<>(responseObserver));
                return null;

            case IHelloService.CLIENT_STREAM:
                //Use StreamToBufferObserver to convert the client StreamObserver to a BufferObserver.
                return new StreamToBufferObserver<>(HelloRequest.parser(),
                        service.clientStream(new SingleToStreamObserver<>(responseObserver)));

            case IHelloService.CLIENT_AND_SERVER_STREAM:
                return new StreamToBufferObserver<>(HelloRequest.parser(),
                        service.clientAndServerStream(new BufferToStreamObserver<>(responseObserver)));
        }

        log.error("Unmatched method: " + method);
        return null;
    }
}
