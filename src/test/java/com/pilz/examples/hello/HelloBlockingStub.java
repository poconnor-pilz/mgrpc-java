package com.pilz.examples.hello;

import com.pilz.mqttgrpc.StreamIterator;
import com.pilz.mqttgrpc.StreamWaiter;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;

import java.util.Iterator;

public class HelloBlockingStub {

    final IHelloService service;

    public HelloBlockingStub(IHelloService service) {
        this.service = service;
    }

    public HelloReply requestResponse(HelloRequest request){
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        service.requestResponse(request, waiter);
        return waiter.getSingle();
    }

    public Iterator<HelloReply> serverStream(HelloRequest request){
        StreamIterator<HelloReply> sit = new StreamIterator<>();
        service.serverStream(request, sit);
        return sit;
    }


}
