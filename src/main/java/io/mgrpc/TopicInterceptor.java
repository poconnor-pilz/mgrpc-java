package io.mgrpc;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Use this interceptor to specify the server topic on which messages should be sent.
 * Each message will have a metadata property in the header with "server-topic" set to the value
 * specified in the constructor
 */
public class TopicInterceptor implements ClientInterceptor {


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String SERVER_TOPIC = "server-topic";
    public static final Metadata.Key<String> META_SERVER_TOPIC = Metadata.Key.of(SERVER_TOPIC, Metadata.ASCII_STRING_MARSHALLER);

    private final String serverTopic;

    /**
     * @param serverTopic The server topic on which messages should be sent.
     * Each message will have a metadata property in the header with "server-topic" set to the value
     * specified
     */
    public TopicInterceptor(String serverTopic) {
        this.serverTopic = serverTopic;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(META_SERVER_TOPIC, serverTopic);
                super.start(responseListener, headers);
            }
        };
    }


}
