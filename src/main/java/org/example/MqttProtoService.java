package org.example;
import com.google.protobuf.ByteString;

public interface MqttProtoService {

    /**
     * @param method The method name
     * @param requestParams The method parameters encoded as a protocol buffer
     * @param replyListener Method replies will be sent to this listener
     * @return If this method is used to start client side streaming then this should
     * return an IProtoListener that will listen to the client side stream. Otherwise return null.
     * @throws Exception
     */
    MqttProtoBufferObserver onProtoRequest(String method, ByteString requestParams, MqttProtoBufferObserver replyListener) throws Exception;

}
