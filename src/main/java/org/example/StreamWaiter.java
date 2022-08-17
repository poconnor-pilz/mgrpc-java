package org.example;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamWaiter<V> implements StreamObserver<V> {

    public long  DEFAULT_TIMEOUT_MILLIS = 10000;

    private List<V> values = new ArrayList<>();
    private Throwable exception = null;

    private final long timeoutMillis;


    private final CountDownLatch latch = new CountDownLatch(1);

    public StreamWaiter() {
        this.timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
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
        this.exception = t;
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
    public List<V> getList() throws Throwable{
        if(!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)){
            //TODO: make special exception type for waiter;
            throw new Exception("Timed out");
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
    public V getSingle() throws Throwable{
        List<V> vals = getList();
        if(vals.size() > 1){
            throw new Exception("More than one value received");
        } else if(vals.size() == 0){
            throw new Exception("No values received");
        } else {
            return vals.get(0);
        }
    }
}
