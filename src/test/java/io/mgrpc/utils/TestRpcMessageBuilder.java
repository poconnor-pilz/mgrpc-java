package io.mgrpc.utils;

import com.google.protobuf.MessageLite;
import io.grpc.Status;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.StatusProto;
import io.mgrpc.RpcMessage;
import io.mgrpc.Start;
import io.mgrpc.Value;

public class TestRpcMessageBuilder {
    public static RpcMessage makeStartRequest(String methodName, Start.MethodType methodType, String callId, int sequence, String replyTo){
        HelloRequest request = HelloRequest.newBuilder().setName(""+sequence).build();
        return makeStart(methodName, methodType, callId, sequence, replyTo, request);
    }

    public static RpcMessage makeValueRequest(String callId, int sequence){
        HelloRequest request = HelloRequest.newBuilder().setName(""+sequence).build();
        return makeValue(callId, sequence,  request);
    }

    public static RpcMessage makeValueResponse(String callId, int sequence){
        HelloReply reply = HelloReply.newBuilder().setMessage(""+sequence).build();
        return makeValue(callId, sequence,  reply);
    }

    public static RpcMessage makeStart(String methodName, Start.MethodType methodType, String callId, int sequence, String replyTo, MessageLite payload){
        Start start = Start.newBuilder()
                .setOutTopic(replyTo)
                .setMethodName(methodName)
                .setMethodType(methodType)
                .build();
        RpcMessage rpcMessage = RpcMessage.newBuilder()
                .setCallId(callId)
                .setSequence(sequence)
                .setStart(start).build();
        return rpcMessage;
    }

    public static RpcMessage makeValue(String callId, int sequence, MessageLite payload){
        Value value = Value.newBuilder().setContents(payload.toByteString()).build();
        RpcMessage rpcMessage = RpcMessage.newBuilder()
                .setCallId(callId)
                .setSequence(sequence)
                .setValue(value).build();
        return rpcMessage;
    }

    public static RpcMessage makeStatus(String callId, int sequence, Status status){
        final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        RpcMessage rpcMessage = RpcMessage.newBuilder()
                .setCallId(callId)
                .setSequence(sequence)
                .setStatus(grpcStatus).build();
        return rpcMessage;
    }
}
