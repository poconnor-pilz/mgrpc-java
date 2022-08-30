package com.pilz.errors;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.HelloCustomError;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorsService implements IErrorsService {

    private static Logger log = LoggerFactory.getLogger(ErrorsService.class);

    @Override
    public void singleResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {

        Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
        responseObserver.onError(status.asRuntimeException());
    }

    @Override
    public void multiResponseWithError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
        responseObserver.onNext(reply);
        Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
        responseObserver.onError(status.asRuntimeException());
    }

    @Override
    public StreamObserver<HelloRequest> errorInClientStream(StreamObserver<HelloReply> singleResponse) {
        return new StreamObserver<HelloRequest>() {
            @Override
            public void onNext(HelloRequest value) {}
            @Override
            public void onError(Throwable t) {
                log.debug("Received error", t);
                Status status = ((StatusRuntimeException) t).getStatus();
                final HelloReply.Builder reply = HelloReply.newBuilder();
                try {
                    checkEqual(Status.Code.OUT_OF_RANGE.name(), status.getCode().name());
                    checkEqual("some description", status.getDescription());
                    //All comparisons are ok so return ok
                    reply.setMessage("ok");
                } catch (Exception ex) {
                    //checkEqual will throw this if there is a problem with one of the comparisons above.
                    reply.setMessage(ex.getMessage());
                }
                singleResponse.onNext(reply.build());
                singleResponse.onCompleted();
            }

            @Override
            public void onCompleted() {}
        };
    }

    @Override
    public void singleResponseWithRichError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {

        //This will end up with exactly the same StatusRuntimeException on the client as with singleResponseWithError
        //Except that we can embed a google.rpc.ErrorInfo in the metadata
        //Usually to represent details it will be enough  to use one of the protocol buffers in
        //https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
        //described also at https://cloud.google.com/apis/design/errors
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.OUT_OF_RANGE.getNumber())
                .setMessage("the value is out of range")
                .addDetails(Any.pack(ErrorInfo.newBuilder()
                        .setReason("test failed")
                        .setDomain("com.pilz.errors")
                        .putMetadata("somekey", "somevalue")
                        .build()))
                .build();
        responseObserver.onError(StatusProto.toStatusRuntimeException(status));
    }

    public void singleResponseWithRichCustomError(HelloRequest request, StreamObserver<HelloReply> responseObserver) {

        //This will end up with exactly the same StatusRuntimeException on the client as with singleResponseWithError
        //Except that we can embed a HelloCustomError in the metadata
        //Usually to represent details it will be enough  to use one of the protocol buffers in
        //https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
        //described also at https://cloud.google.com/apis/design/errors
        //But in this case the details are in a HelloCustomError
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.OUT_OF_RANGE.getNumber())
                .setMessage("the value is out of range")
                .addDetails(Any.pack(HelloCustomError.newBuilder()
                        .setHelloErrorCode(20)
                        .setHelloErrorDescription("an error description")
                        .build()))
                .build();
        responseObserver.onError(StatusProto.toStatusRuntimeException(status));
    }


    @Override
    public StreamObserver<HelloRequest> richErrorInClientStream(StreamObserver<HelloReply> singleResponse) {
        return new StreamObserver<HelloRequest>() {
            @Override
            public void onNext(HelloRequest value) {}
            @Override
            public void onError(Throwable t) {

                final HelloReply.Builder reply = HelloReply.newBuilder();
                com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(t);
                //Check the error details are as expected and return "ok" to the test if they are.
                try {
                    checkEqual(Code.OUT_OF_RANGE.name(), Code.forNumber(rpcStatus.getCode()).name());
                    checkEqual("the value is out of range", rpcStatus.getMessage());
                    for (Any any : rpcStatus.getDetailsList()) {
                        if (any.is(ErrorInfo.class)) {
                            ErrorInfo errorInfo = any.unpack(ErrorInfo.class);
                            checkEqual("test failed", errorInfo.getReason());
                            checkEqual("com.pilz.errors", errorInfo.getDomain());
                            checkEqual("somevalue", errorInfo.getMetadataMap().get("somekey"));
                        }
                    }
                    //All comparisons are ok so return ok
                    reply.setMessage("ok");
                } catch (Exception ex){
                    //checkEqual will throw this if there is a problem with one of the comparisons above.
                    reply.setMessage(ex.getMessage());
                }
                singleResponse.onNext(reply.build());
                singleResponse.onCompleted();
            }

            @Override
            public void onCompleted() {}
        };
    }

    private void checkEqual(String expected, String actual) throws Exception{
        if(!expected.equals(actual)){
            throw new Exception("Expected[" + expected + "] Actual[" + actual + "]");
        }
    }


}
