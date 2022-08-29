package com.pilz.mqttgrpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamWaiter<V> implements StreamObserver<V> {

    public long INFINITE_TIMEOUT = -1;

    private List<V> values = new ArrayList<>();
    private StatusRuntimeException exception = null;

    private final long timeoutMillis;


    private final CountDownLatch latch = new CountDownLatch(1);

    public StreamWaiter() {
        this.timeoutMillis = INFINITE_TIMEOUT;
    }

    public StreamWaiter(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }



    @Override
    public void onNext(V value) {
       this.values.add(value);
    }

    @Override
    public void onError(Throwable t) {
        this.exception = new StatusRuntimeException(Status.fromThrowable(t));
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    /**
     * @return The list of values returned in the stream
     * @throws Throwable if onCompleted() is not received before the timeout or if there was an error
     * in the stream
     */
    public List<V> getList() throws StatusRuntimeException {
        try {
            if(timeoutMillis < 0){
                latch.await();
            } else if(!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)){
                    //TODO: make special exception type for waiter;
                    throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED.withDescription("Operation timed out"));
            }
        } catch (InterruptedException e) {
            throw new StatusRuntimeException(Status.fromThrowable(e));
        }
        if(this.exception != null){
            throw this.exception;
        }
        return values;
    }

    /**
     * @return The single value returned in the stream
     * @throws Throwable if onCompleted() is not received before the timeout or if there was an error
     * in the stream or if there was not exactly one value in the stream.
     */
    public V getSingle() throws StatusRuntimeException{
        List<V> vals = getList();
        if(vals.size() == 1){
            return vals.get(0);
        }
        if(vals.size() == 0){
            throw new StatusRuntimeException(Status.UNKNOWN.withDescription("No values received."));
        } else {
            throw new StatusRuntimeException(Status.UNKNOWN.withDescription("More than one value received"));
        }
    }
}
