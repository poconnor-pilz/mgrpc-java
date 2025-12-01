package io.mgrpc.errors;

import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;

public class CancelableObserver implements ClientResponseObserver<HelloRequest, HelloReply> {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ClientCallStreamObserver requestStream;
    public final CountDownLatch latch = new CountDownLatch(1);
    public StatusRuntimeException exception = null;
    private final int delayInMilliseconds;

    public CancelableObserver() {
        this.delayInMilliseconds = 0;
    }

    public CancelableObserver(int delayInMilliseconds) {
        this.delayInMilliseconds = delayInMilliseconds;
    }


    @Override
    public void beforeStart(ClientCallStreamObserver reqStream) {
        requestStream = reqStream;
    }

    public void cancel(String message) {
        log.debug("CancelableObserver cancel()");
        requestStream.cancel(message, null);
    }

    @Override
    public void onNext(HelloReply value) {
        log.debug("CancelableObserver received response. Will pause for {} ms", delayInMilliseconds);
        if(delayInMilliseconds > 0) {
            try {
                Thread.sleep(delayInMilliseconds);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        log.debug("CancelableObserver onError()", t);
        this.exception = (StatusRuntimeException) t;
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        log.debug("CancelableObserver onCompleted()");
    }

}
