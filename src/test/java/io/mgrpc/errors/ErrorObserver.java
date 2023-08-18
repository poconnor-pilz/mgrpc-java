package io.mgrpc.errors;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class ErrorObserver implements StreamObserver {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public final CountDownLatch errorLatch = new CountDownLatch(1);
    public final CountDownLatch nextLatch = new CountDownLatch(1);
    StatusRuntimeException exception = null;

    final String name;

    ErrorObserver(String name) {
        this.name = name;
    }


    public Status waitForStatus(long timeout, TimeUnit timeUnit){
        try {
            if(!errorLatch.await(timeout, timeUnit)){
                throw new RuntimeException("Waiting for status timed out after " +  timeout + " " + timeUnit);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return exception.getStatus();
    }

    public void waitForNext(long timeout, TimeUnit timeUnit){
        try {
            if(!nextLatch.await(timeout, timeUnit)){
                throw new RuntimeException("Waiting for next timed out after " +  timeout + " " + timeUnit);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onNext(Object o) {
        this.nextLatch.countDown();;
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("", throwable);
        exception = (StatusRuntimeException) throwable;
        errorLatch.countDown();
    }

    @Override
    public void onCompleted() {

    }
}
