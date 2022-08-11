package org.example;

public interface MPStreamObserver<V> {

    /**
     * Receives a value from the stream.
     *
     * <p>Can be called many times but is never called after {@link #onError(Throwable)} or {@link
     * #onLast(V)} are called.
     *
     * <p>Unary calls should not invoke onNext. Instead they should invoke onLast once only.
     *
     * <p>If an exception is thrown by an implementation the caller is expected to terminate the
     * stream by calling {@link #onError(Throwable)} with the caught exception prior to
     * propagating it.
     *
     * @param value the value passed to the stream
     */
    void onNext(V value);

    /**
     * Receives a terminating error from the stream.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onError} no further calls to any method are
     * allowed.
     *
     * TODO: Look at the other comments in grpc StreamObserver about Status and StatusException. Should we use these?
     *
     * @param t the error occurred on the stream
     */
    void onError(Throwable t);

    /**
     * Receives the last or only message in a stream.
     *
     * <p>Unary calls should not invoke onNext. Instead they should invoke onLast once only.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onCompleted} no further calls to any method
     * are allowed.
     */
    void onLast(V value);
}
