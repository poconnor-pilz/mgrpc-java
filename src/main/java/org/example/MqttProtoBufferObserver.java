package org.example;

import com.google.protobuf.ByteString;

public interface MqttProtoBufferObserver {

    void onNext(ByteString value);

    void onLast(ByteString value);

    void onError(String error);
}
