package com.pilz.mqttgrpc;
import com.google.protobuf.ByteString;

public interface Skeleton {

    /**
     * @param method The method name
     * @param request Initial request
     * @param responseObserver Observer of the stream of protobuf responses from the service
     * @return If this method is used to start client side streaming then this should
     * return an BufferObserver that will listen to the client side stream. Otherwise return null.
     * @throws Exception
     */
    BufferObserver onRequest(String method, ByteString request, BufferObserver responseObserver) throws Exception;

}
