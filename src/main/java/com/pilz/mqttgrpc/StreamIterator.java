package com.pilz.mqttgrpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class StreamIterator<V> implements Iterator<V>, StreamObserver<V> {

    private static final int DEFAULT_QUEUE_BUFFER_SIZE = 10;

    private final int queueBufferSize;

    public StreamIterator(int queueBufferSize) {
        this.queueBufferSize = queueBufferSize;
    }

    public StreamIterator() {
        this.queueBufferSize = DEFAULT_QUEUE_BUFFER_SIZE;
    }


    private enum Type{
        NEXT,
        ERROR,
        COMPLETED
    }

    private class QueueItem{
        public final Type type;
        public final V value;
        public final Status status;

        private QueueItem(Type type, V value, Status status) {
            this.type = type;
            this.value = value;
            this.status = status;
        }

        private QueueItem(Status status){
            this(Type.ERROR, null, status);
        }

        private QueueItem(V value){
            this(Type.NEXT, value, null);
        }

        private QueueItem(){
            this(Type.COMPLETED, null, null);
        }
    }



    private BlockingQueue<QueueItem> q = new ArrayBlockingQueue<>(DEFAULT_QUEUE_BUFFER_SIZE);

    //StreamObserver methods
    @Override
    public void onNext(V value) {
        try {
            q.put(new QueueItem(value));
        } catch (InterruptedException e) {
            log.error("Interrupted while putting item on queue", e);
        }
    }

    @Override
    public void onError(Throwable t) {
        try {
            q.put(new QueueItem(Status.fromThrowable(t)));
        } catch (InterruptedException e) {
            log.error("Interrupted while putting item on queue", e);
        }
    }

    @Override
    public void onCompleted() {
        try {
            q.put(new QueueItem());
        } catch (InterruptedException e) {
            log.error("Interrupted while putting item on queue", e);
        }
    }

    //Iterator methods
    @Override
    public boolean hasNext() {
        final QueueItem item = q.peek();
        if(item != null){
            if(item.type == Type.COMPLETED){
                return false;
            }
        }
        //Even if there is no value on the queue next will block until there is
        return true;
    }

    @Override
    /**
     * This will throw a StatusRuntimeException if the next item in the stream is an error.
     */
    public V next() {
        try {
            final QueueItem item = q.take();
            switch(item.type){
                case NEXT:
                    return item.value;
                case ERROR:
                    throw new StatusRuntimeException(item.status);
                case COMPLETED:
                    throw new StatusRuntimeException(Status.INTERNAL.withDescription(
                            "Iterarator failed. hasNext() should have returned false"));
            }
        } catch (InterruptedException e) {
            throw new StatusRuntimeException(Status.fromThrowable(e));
        }

        return null;//Satisfy compiler. This cannot be reached.
    }



}
