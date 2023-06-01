package io.mgrpc;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Class to help with debugging the grpc flow.
 */
public class ChannelWrapper extends Channel {

    private static Logger log = LoggerFactory.getLogger(ChannelWrapper.class);

    public ChannelWrapper(Channel inner) {
        this.inner = inner;
    }

    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return new CallWrapper<>(inner.newCall(methodDescriptor, callOptions));
    }

    public String authority() {
        return inner.authority();
    }

    private final Channel inner;

    static class CallWrapper<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        private final ClientCall<ReqT, RespT> innerCall;

        private Listener<RespT> responseListener;

        CallWrapper(ClientCall<ReqT, RespT> thinner) {
            innerCall = thinner;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            log.debug("start");
            responseListener = new ListenerWrapper<>(responseListener);
            innerCall.start(responseListener, headers);
        }

        @Override
        public void request(int numMessages) {
            log.debug("request");
            innerCall.request(numMessages);
        }

        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
            log.debug("cancel");
            innerCall.cancel(message, cause);
        }

        @Override
        public void halfClose() {
            log.debug("halfClose");
            innerCall.halfClose();
        }

        @Override
        public void sendMessage(ReqT message) {
            log.debug("sendMessage");
            innerCall.sendMessage(message);
        }
    }

    static class ListenerWrapper<RespT> extends ClientCall.Listener<RespT> {
        final ClientCall.Listener<RespT> inner;

        ListenerWrapper(ClientCall.Listener<RespT> inner) {
            this.inner = inner;
        }

        @Override
        public void onHeaders(Metadata headers) {
            log.debug("listener.onHeaders");
            inner.onHeaders(headers);
        }

        @Override
        public void onMessage(RespT message) {
            log.debug("listener.onMessage");
            inner.onMessage(message);
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            log.debug("listener.onClose");
            inner.onClose(status, trailers);
        }

        @Override
        public void onReady() {
            log.debug("listener.onReady");
            inner.onReady();
        }

    }

}
