package org.example;

import com.google.protobuf.ByteString;

public interface MPBufferObserver {

    void onNext(ByteString value);

    void onLast(ByteString value);

    void onError(ByteString error);
}
