package org.example;

import com.google.protobuf.ByteString;

public interface MPBufferObserver {

    void onNext(ByteString value);


    void onCompleted();


    void onError(ByteString error);
}
