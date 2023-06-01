package io.mgrpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamWaiter<V> implements StreamObserver<V> {

    private static Logger log = LoggerFactory.getLogger(StreamWaiter.class);

    public static final long INFINITE_TIMEOUT = -1;

    private List<V> values = new ArrayList<>();
    private Status status = null;

    private boolean used = false;

    private final long timeoutMillis;


    private final CountDownLatch latch = new CountDownLatch(1);


    public StreamWaiter(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public StreamWaiter() {
        this(INFINITE_TIMEOUT);
    }


    @Override
    public void onNext(V value) {
        this.values.add(value);
    }

    @Override
    public void onError(Throwable t) {
        this.status = Status.fromThrowable(t).withCause(t);
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
        if(used){
            throw new StatusRuntimeException(Status.INTERNAL.withDescription("get() or getList() can only be used once"));
        }
        used = true;
        try {
            if(timeoutMillis < 0){
                latch.await();
            } else if(!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)){
                throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED.withDescription("Operation timed out"));
            }
        } catch (InterruptedException e) {
            throw new StatusRuntimeException(Status.fromThrowable(e));
        }
        if(this.status != null){
            throw new StatusRuntimeException(status);
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
