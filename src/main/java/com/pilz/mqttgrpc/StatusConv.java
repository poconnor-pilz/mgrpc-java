package com.pilz.mqttgrpc;

import io.grpc.Status;

public class StatusConv {

    public static MqttGrpcStatus toBuffer(Status status){
        final MqttGrpcStatus.Builder builder = MqttGrpcStatus.newBuilder();
        builder.setCode(status.getCode().value());
        if(status.getDescription() != null){
            builder.setDescription(status.getDescription());
        }
        return builder.build();
    }

    public static Status toStatus(MqttGrpcStatus MqttGrpcStatus){
        return Status.fromCodeValue(MqttGrpcStatus.getCode()).withDescription(MqttGrpcStatus.getDescription());
    }
}
