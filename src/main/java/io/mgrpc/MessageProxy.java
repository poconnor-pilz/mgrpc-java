package io.mgrpc;

import com.google.common.io.ByteStreams;
import io.grpc.*;
import io.mgrpc.messaging.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A grpc-level proxy. This class is based on GrpcProxy
 * This proxy can be used to create a single http server that makes it possible for clients to
 * communicate with a number of different message servers (or devices)
 * To do so the client must specify grpc metadata as part of the call.
 * Specifically, the client must specify the key "server-topic". The value should be the message broker topic on which
 * the MessageServer is listening for example:
 * server-topic = tenant1/device1
 */
public class MessageProxy<ReqT, RespT>  implements ServerCallHandler<ReqT, RespT> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private MessageChannelFactory channelFactory;


    private final Map<String, MessageChannel> channels = new ConcurrentHashMap<>();


    public MessageProxy(MessageChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }


    public void close(){
        for (MessageChannel channel : channels.values()) {
            channel.close();
        }
    }


    @Override
    public ServerCall.Listener<ReqT> startCall(
            ServerCall<ReqT, RespT> serverCall, Metadata headers) {

        Channel channel = getChannelForServerTopic(headers);

        ClientCall<ReqT, RespT> clientCall
                = channel.newCall(serverCall.getMethodDescriptor(), CallOptions.DEFAULT);
        CallProxy<ReqT, RespT> proxy = new CallProxy<ReqT, RespT>(serverCall, clientCall);
        clientCall.start(proxy.clientCallListener, headers);
        serverCall.request(1);
        clientCall.request(1);
        return proxy.serverCallListener;
    }

    private Channel getChannelForServerTopic(Metadata headers) {

        //Find the right channel for the topic (i.e. find the right device)
        //and send the messages to it
        final String serverTopic = headers.get(TopicInterceptor.META_SERVER_TOPIC);
        if (serverTopic == null || serverTopic.isEmpty()) {
            throw new IllegalArgumentException("Missing server topic header");
        }

        MessageChannel channel = channels.get(serverTopic);
        if (channel == null) {
            synchronized (channels) {
                channel = channels.get(serverTopic);
                if (channel == null) {
                    channel = channelFactory.createMessageChannel(serverTopic);
                    channels.put(serverTopic, channel);
                    logger.debug("Created channel for {}", serverTopic);
                }
            }
        }
        if(!channel.isStarted()){
            synchronized (channel){
                if(!channel.isStarted()){
                    channel.addDisconnectListener((String channelId) -> {
                        channels.remove(serverTopic);
                    });
                    try {
                        channel.start();
                    } catch (MessagingException e) {
                        channels.remove(serverTopic);
                        logger.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return channel;
    }

    private static class CallProxy<ReqT, RespT> {
        final RequestProxy serverCallListener;
        final ResponseProxy clientCallListener;

        public CallProxy(ServerCall<ReqT, RespT> serverCall, ClientCall<ReqT, RespT> clientCall) {
            serverCallListener = new RequestProxy(clientCall);
            clientCallListener = new ResponseProxy(serverCall);
        }

        private class RequestProxy extends ServerCall.Listener<ReqT> {
            private final ClientCall<ReqT, ?> clientCall;
            // Hold 'this' lock when accessing
            private boolean needToRequest;

            public RequestProxy(ClientCall<ReqT, ?> clientCall) {
                this.clientCall = clientCall;
            }

            @Override
            public void onCancel() {
                clientCall.cancel("Server cancelled", null);
            }

            @Override
            public void onHalfClose() {
                clientCall.halfClose();
            }

            @Override
            public void onMessage(ReqT message) {
                clientCall.sendMessage(message);
                synchronized (this) {
                    if (clientCall.isReady()) {
                        clientCallListener.serverCall.request(1);
                    } else {
                        // The outgoing call is not ready for more requests. Stop requesting additional data and
                        // wait for it to catch up.
                        needToRequest = true;
                    }
                }
            }

            @Override
            public void onReady() {
                clientCallListener.onServerReady();
            }

            // Called from ResponseProxy, which is a different thread than the ServerCall.Listener
            // callbacks.
            synchronized void onClientReady() {
                if (needToRequest) {
                    clientCallListener.serverCall.request(1);
                    needToRequest = false;
                }
            }
        }

        private class ResponseProxy extends ClientCall.Listener<RespT> {
            private final ServerCall<?, RespT> serverCall;
            // Hold 'this' lock when accessing
            private boolean needToRequest;

            public ResponseProxy(ServerCall<?, RespT> serverCall) {
                this.serverCall = serverCall;
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                serverCall.close(status, trailers);
            }

            @Override
            public void onHeaders(Metadata headers) {
                serverCall.sendHeaders(headers);
            }

            @Override
            public void onMessage(RespT message) {
                serverCall.sendMessage(message);
                synchronized (this) {
                    if (serverCall.isReady()) {
                        serverCallListener.clientCall.request(1);
                    } else {
                        // The incoming call is not ready for more responses. Stop requesting additional data
                        // and wait for it to catch up.
                        needToRequest = true;
                    }
                }
            }

            @Override
            public void onReady() {
                serverCallListener.onClientReady();
            }

            // Called from RequestProxy, which is a different thread than the ClientCall.Listener
            // callbacks.
            synchronized void onServerReady() {
                if (needToRequest) {
                    serverCallListener.clientCall.request(1);
                    needToRequest = false;
                }
            }
        }
    }

    private static class ByteMarshaller implements MethodDescriptor.Marshaller<byte[]> {
        @Override public byte[] parse(InputStream stream) {
            try {
                return ByteStreams.toByteArray(stream);
            } catch (IOException ex) {
                throw new RuntimeException();
            }
        }

        @Override public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }
    };

    public static class Registry extends HandlerRegistry {
        private final MethodDescriptor.Marshaller<byte[]> byteMarshaller = new ByteMarshaller();
        private final ServerCallHandler<byte[], byte[]> handler;

        private final Map<String, MethodDescriptor> methodsRegistry = new HashMap<>();

        public Registry(ServerCallHandler<byte[], byte[]> handler) {
            this.handler = handler;
        }

        /**
         * A client can choose to inform the proxy about services using this method.
         * This can help where the proxy is being connected to a MessageChannel. It means that the MessageChannel
         * will know about the method type which can help the ChannelMessageTransport to transport the messages more
         * efficiently.
         * @param serviceDescriptor
         */
        public void registerServiceDescriptor(ServiceDescriptor serviceDescriptor) {
            for(MethodDescriptor methodDescriptor: serviceDescriptor.getMethods()){
                methodsRegistry.put(methodDescriptor.getFullMethodName(), methodDescriptor);
            }
        }
        @Override
        public ServerMethodDefinition<?,?> lookupMethod(String methodName, String authority) {

            //If the client has registered a method type already then use that.
            final MethodDescriptor registeredMethodDescriptor = methodsRegistry.get(methodName);
            final MethodDescriptor.MethodType methodType;
            if(registeredMethodDescriptor != null){
                methodType = registeredMethodDescriptor.getType();
            } else {
                methodType = MethodDescriptor.MethodType.UNKNOWN;
            }


            MethodDescriptor<byte[], byte[]> methodDescriptor
                    = MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
                    .setFullMethodName(methodName)
                    .setType(methodType)
                    .build();
            return ServerMethodDefinition.create(methodDescriptor, handler);
        }
    }


}