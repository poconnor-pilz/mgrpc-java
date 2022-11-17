package com.pilz.examples.hellowithchannel;

import com.pilz.mqttgrpc.MqttGrpcRequest;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloCustomError;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestServerCall {


    private static Logger log = LoggerFactory.getLogger(TestServerCall.class);

    static class ExampleHelloServiceImpl extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase{
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            log.debug("ExampleHelloServiceImpl received: " + request);
            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        }
    }

    static class ServerCallTry<ReqT, RespT> extends ServerCall<ReqT,RespT>{

        final MethodDescriptor<ReqT, RespT> methodDescriptor;

        ServerCallTry(MethodDescriptor<ReqT, RespT> methodDescriptor) {
            this.methodDescriptor = methodDescriptor;
        }

        @Override
        public void request(int numMessages) {

        }

        @Override
        public void sendHeaders(Metadata headers) {

        }

        @Override
        public void sendMessage(RespT message) {
            log.debug("Got a response");
        }

        @Override
        public void close(Status status, Metadata trailers) {

        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return methodDescriptor;
        }
    }


    @Test
    public void testInd(){
        String s = "test/abc/def";
        log.debug(s.substring(s.lastIndexOf('/', s.lastIndexOf('/') - 1) + 1) );
    }

    @Test
    public void testServerCall(){

        //Make an InputStream from a HelloRequest
        final HelloRequest hr = HelloRequest.newBuilder().setName("joe").build();
        //final HelloCustomError gr = HelloCustomError.newBuilder().setHelloErrorCode(1).setHelloErrorDescription("blah").build();
        InputStream stream = new ByteArrayInputStream(hr.toByteArray());
        final ExampleHelloServiceImpl exampleHelloService = new ExampleHelloServiceImpl();
        final ServerServiceDefinition serverServiceDefinition = exampleHelloService.bindService();
        final ServerMethodDefinition<?, ?> sayHello = serverServiceDefinition.getMethod("helloworld.ExampleHelloService/SayHello");
        final Object theHelloRequest = sayHello.getMethodDescriptor().parseRequest(stream);
        final ServerCallHandler serverCallHandler = sayHello.getServerCallHandler();
        ServerCall serverCall = new ServerCallTry(sayHello.getMethodDescriptor());
        final ServerCall.Listener listener = serverCallHandler.startCall(serverCall, new Metadata());
        listener.onMessage(theHelloRequest);
        //notify this is the end of the client stream after which the onMessage() will go through
        listener.onHalfClose();


    }

    @Test
    public void testInProcess() throws Exception{
        String uniqueName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(uniqueName)
                .directExecutor()
                .addService(new ExampleHelloServiceImpl() {
                })
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName)
                .directExecutor()
                .build();

        ChannelWrapper wrapper = new ChannelWrapper(channel);
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub = ExampleHelloServiceGrpc.newBlockingStub(wrapper);
        final HelloRequest name = HelloRequest.newBuilder().setName("test name").build();
        HelloReply reply = blockingStub.sayHello(name);
        assertEquals("Hello test name", reply.getMessage());

        channel.shutdown();
        server.shutdown();
    }

}
