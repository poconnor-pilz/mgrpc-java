package io.mgrpc.utils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StatusObserver implements StreamObserver {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public final CountDownLatch statusLatch = new CountDownLatch(1);
    public final CountDownLatch nextLatch = new CountDownLatch(1);
    public StatusRuntimeException exception = null;

    public final String name;

    public StatusObserver(String name) {
        this.name = name;
    }


    public Status waitForStatus(long timeout, TimeUnit timeUnit){
        try {
            if(!statusLatch.await(timeout, timeUnit)){
                throw new RuntimeException("Waiting for status timed out after " +  timeout + " " + timeUnit);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if(exception != null) {
            return exception.getStatus();
        } else {
            return Status.OK;
        }
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
        log.debug("onNext()");
        this.nextLatch.countDown();;
    }

    @Override
    public void onError(Throwable throwable) {
        log.debug("onError()");
        log.error("", throwable);
        exception = (StatusRuntimeException) throwable;
        statusLatch.countDown();
    }

    @Override
    public void onCompleted() {
        log.debug("onCompleted()");
        statusLatch.countDown();
    }
}
