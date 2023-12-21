package io.mgrpc;

import io.grpc.MethodDescriptor;

public class MethodTypeConverter {

    public static MethodDescriptor.MethodType fromStart(Start.MethodType methodType) {
        switch (methodType) {
            case UNARY:
                return MethodDescriptor.MethodType.UNARY;
            case SERVER_STREAMING:
                return MethodDescriptor.MethodType.SERVER_STREAMING;
            case CLIENT_STREAMING:
                return MethodDescriptor.MethodType.CLIENT_STREAMING;
            case BIDI_STREAMING:
                return MethodDescriptor.MethodType.BIDI_STREAMING;
            default:
                return MethodDescriptor.MethodType.UNKNOWN;
        }
    }

    public static MethodDescriptor.MethodType fromStart(RpcMessageOrBuilder rpcMessage) {
        if(!rpcMessage.hasStart()){
            throw new RuntimeException("Not a start message");
        }
        return fromStart(rpcMessage.getStart().getMethodType());
    }


    public static Start.MethodType toStart(MethodDescriptor.MethodType methodType) {
        switch (methodType) {
            case UNARY:
                return Start.MethodType.UNARY;
            case SERVER_STREAMING:
                return Start.MethodType.SERVER_STREAMING;
            case CLIENT_STREAMING:
                return Start.MethodType.CLIENT_STREAMING;
            case BIDI_STREAMING:
                return Start.MethodType.BIDI_STREAMING;
            default:
                return Start.MethodType.UNKNOWN;
        }
    }


}
