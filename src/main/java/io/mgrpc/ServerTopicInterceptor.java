package io.mgrpc;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class ServerTopicInterceptor implements ClientInterceptor {


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String SERVER_TOPIC = "server-topic";
    public static final CallOptions.Key<String> OPT_SERVER_TOPIC = CallOptions.Key.create(SERVER_TOPIC);
    public static final Metadata.Key<String> META_SERVER_TOPIC = Metadata.Key.of(SERVER_TOPIC, Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                final String serverTopic = callOptions.getOption(OPT_SERVER_TOPIC);
                if(serverTopic == null) {
                    log.warn("Server topic not set");
                } else {
                    headers.put(META_SERVER_TOPIC, serverTopic);
                }
                super.start(responseListener, headers);
            }
        };
    }


}
