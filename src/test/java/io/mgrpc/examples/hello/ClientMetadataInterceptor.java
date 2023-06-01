package io.mgrpc.examples.hello;

import io.grpc.*;

public class ClientMetadataInterceptor implements ClientInterceptor {

    public static final Metadata.Key<String> HOSTNAME_KEY = Metadata.Key.of("hostName", Metadata.ASCII_STRING_MARSHALLER);

    public static final String MYHOST = "myHost";

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(HOSTNAME_KEY, MYHOST);
                super.start(responseListener, headers);
            }
        };
    }
}
