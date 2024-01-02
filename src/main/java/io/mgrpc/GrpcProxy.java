package io.mgrpc;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** A grpc-level proxy.
 * This class is copied (with a couple of small changes)  from
 * https://github.com/ejona86/grpc-java/blob/grpc-proxy/examples/src/main/java/io/grpc/examples/grpcproxy/GrpcProxy.java
 */
public class GrpcProxy extends HandlerRegistry implements ServerCallHandler<byte[], byte[]>{

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private final Channel channel;


    private Map<String, MethodDescriptor> methodsRegistry = new HashMap<>();

    private final MethodDescriptor.Marshaller<byte[]> byteMarshaller = new ByteMarshaller();


    public GrpcProxy(Channel channel) {
        this.channel = channel;
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
                =  MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
                .setFullMethodName(methodName)
                .setType(methodType)
                .build();

        if(registeredMethodDescriptor != null){
            methodDescriptor = registeredMethodDescriptor;
        }
        return ServerMethodDefinition.create(methodDescriptor, this);
    }

    @Override
    public ServerCall.Listener<byte[]> startCall(
            ServerCall<byte[], byte[]> serverCall, Metadata headers) {
        ClientCall<byte[], byte[]> clientCall
                = channel.newCall(serverCall.getMethodDescriptor(), CallOptions.DEFAULT);
        CallProxy<byte[], byte[]> proxy = new CallProxy<byte[], byte[]>(serverCall, clientCall);
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
                        needToRequest = true;
                    }
                }
            }

            @Override public void onReady() {
                clientCallListener.onServerReady();
            }

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
                        needToRequest = true;
                    }
                }
            }

            @Override public void onReady() {
                serverCallListener.onClientReady();
            }

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
                byte[] bytes = new byte[stream.available()];
                DataInputStream dataInputStream = new DataInputStream(stream);
                dataInputStream.readFully(bytes);
                return bytes;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }
    };


    public static void main(String[] args) throws IOException, InterruptedException {
        String target = "localhost:8980";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        logger.info("Proxy will connect to " + target);
        GrpcProxy proxy = new GrpcProxy(channel);
        int port = 8981;
        Server server = ServerBuilder.forPort(port)
                .fallbackHandlerRegistry(proxy)
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
                if (!server.isTerminated()) {
                    server.shutdownNow();
                }
                channel.shutdownNow();
            }
        });
        server.awaitTermination();
        if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
            System.out.println("Channel didn't shut down promptly");
        }
    }
}