package com.pilz.mqttgrpc;

import io.grpc.Status;

public class StatusConv {

    public static MqttGrpcStatus toBuffer(Status status){
        return MqttGrpcStatus.newBuilder().setCode(status.getCode().value()).
                setDescription(status.getDescription()).build();
    }

    public static Status toStatus(MqttGrpcStatus MqttGrpcStatus){
        return Status.fromCodeValue(MqttGrpcStatus.getCode()).withDescription(MqttGrpcStatus.getDescription());
    }
}
