package io.mgrpc;

public class MessagingException extends Exception{

    private final Throwable cause;

    public MessagingException(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public synchronized Throwable getCause() {
        return this.cause;
    }
}
