package io.mgrpc;

import io.grpc.Status;

public class MessagingException extends Exception{

    private Status status = Status.UNKNOWN;

    public MessagingException(Status status){
        this.status = status;
    }

    public MessagingException(String message, Status status){
        super(message);
        this.status = status;
    }

    public MessagingException(String message){
        super(message);
    }
    public MessagingException(Throwable cause) {
        super(cause);
    }

    public Status getStatus(){
        return status;
    }

}
