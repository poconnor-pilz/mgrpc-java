package io.mgrpc.errors;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ObserverLogger implements StreamObserver {

    private static final Logger log = LoggerFactory.getLogger(ObserverLogger.class);
    private final String name;

    ObserverLogger(String name) {
        this.name = name;
    }

    @Override
    public void onNext(Object o) {
        log.debug("onNext() " + name);
    }

    @Override
    public void onError(Throwable throwable) {
        log.debug("onError() " + name, throwable);

    }

    @Override
    public void onCompleted() {
        log.debug("onNext() " + name);
    }
}
