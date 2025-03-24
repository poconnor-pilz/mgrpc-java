package io.mgrpc;

/*
 * Copyright 2023, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.common.io.ByteStreams;
import io.grpc.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

//This was taken from
//examples/src/main/java/io/grpc/examples/grpcproxy/GrpcProxy.java
//commit: fca1d3c
//It has a small modification to include registerServiceDescriptor
/**
 * A grpc-level proxy. GrpcProxy itself can be used unmodified to proxy any service for both unary
 * and streaming. It doesn't care what type of messages are being used. The Registry class causes it
 * to be called for any inbound RPC, and uses plain bytes for messages which avoids marshalling
 * messages and the need for Protobuf schema information. *
 */
public final class GrpcProxy<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {
    private static final Logger logger = Logger.getLogger(GrpcProxy.class.getName());

    private final Channel channel;


    public GrpcProxy(Channel channel) {
        this.channel = channel;
    }


    @Override
    public ServerCall.Listener<ReqT> startCall(
            ServerCall<ReqT, RespT> serverCall, Metadata headers) {
        ClientCall<ReqT, RespT> clientCall
                = channel.newCall(serverCall.getMethodDescriptor(), CallOptions.DEFAULT);
        CallProxy<ReqT, RespT> proxy = new CallProxy<ReqT, RespT>(serverCall, clientCall);
        clientCall.start(proxy.clientCallListener, headers);
        serverCall.request(1);
        clientCall.request(1);
        return proxy.serverCallListener;
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

            @Override public void onCancel() {
                clientCall.cancel("Server cancelled", null);
            }

            @Override public void onHalfClose() {
                clientCall.halfClose();
            }

            @Override public void onMessage(ReqT message) {
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

            @Override public void onReady() {
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

            @Override public void onClose(Status status, Metadata trailers) {
                serverCall.close(status, trailers);
            }

            @Override public void onHeaders(Metadata headers) {
                serverCall.sendHeaders(headers);
            }

            @Override public void onMessage(RespT message) {
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

            @Override public void onReady() {
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
         * will know about the method type which can help the ChannelMessageConduit to transport the messages more
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

    public static void main(String[] args) throws IOException, InterruptedException {
        String target = "localhost:8980";
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
        logger.info("Proxy will connect to " + target);
        GrpcProxy<byte[], byte[]> proxy = new GrpcProxy<byte[], byte[]>(channel);
        int port = 8981;
        Server server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .fallbackHandlerRegistry(new Registry(proxy))
                .build()
                .start();
        logger.info("Proxy started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutdown();
                try {
                    server.awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                server.shutdownNow();
                channel.shutdownNow();
            }
        });
        server.awaitTermination();
        if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
            System.out.println("Channel didn't shut down promptly");
        }
    }
}