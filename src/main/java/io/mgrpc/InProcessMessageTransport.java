package io.mgrpc;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.mgrpc.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Class providing server and channel transports that runs in process. Useful for unit testing mgrpc.
 * To do production level in process grpc it would be better to use io.grpc.inprocess.InProcessServerBuilder
 * which will be more efficient.
 */
public class InProcessMessageTransport {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Map<String, ChannelMessageListener> channelsById = new ConcurrentHashMap<>();
    private ServerMessageListener server;

    private final ServerMessageTransport serverTransport = new InprocServerTransport();

    private static volatile Executor executorSingleton;


    public ChannelMessageTransport getChannelTransport(){
        return new InprocChannelTransport();
    }

    public ServerMessageTransport getServerTransport(){
        return serverTransport;
    }

    private static Executor getExecutorInstance() {
        if (executorSingleton == null) {
            synchronized (MessageServer.class) {
                if (executorSingleton == null) {
                    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum or maybe it should be passed as a constructor parameter?
                    executorSingleton = Executors.newCachedThreadPool();
                }
            }
        }
        return executorSingleton;
    }

    private class InprocServerTransport implements ServerMessageTransport {


        @Override
        public void start(MessageServer server) throws MessagingException {
            if(InProcessMessageTransport.this.server != null){
                String err = "InProcessMessageTransport instance can only be associated with one Server";
                log.error(err);
                throw new MessagingException(err);
            }
            InProcessMessageTransport.this.server = server;
        }

        @Override
        public void close() {}

        @Override
        public void onCallClose(String channelId, String callId) {}

        @Override
        public void request(String channelId, String callId, int numMessages) {
        }

        @Override
        public void send(String channelId, String methodName, boolean serverSendsOneMessage, RpcMessage message) throws MessagingException {
            final ChannelMessageListener channel = channelsById.get(channelId);
            if(channel == null){
                String err = "Channel " + channelId +  " does not exist";
                log.error(err);
                throw new MessagingException(err);
            }
            channel.onMessage(message);
        }

        @Override
        public Executor getExecutor() {
            return getExecutorInstance();
        }

    }

    private class InprocChannelTransport implements ChannelMessageTransport {

        private String channelId;

        @Override
        public void start(MessageChannel channel) throws MessagingException {
            this.channelId = channel.getChannelId();
            channelsById.put(channelId, channel);
        }

        @Override
        public void onCallStart(MethodDescriptor methodDescriptor, CallOptions callOptions, String callId) {}

        @Override
        public void onCallClose(String callId){}

        @Override
        public void close() {
            channelsById.remove(channelId);
        }

        @Override
        public void send(MethodDescriptor methodDescriptor, RpcMessage.Builder rpcMessageBuilder) throws MessagingException {
            server.onMessage(rpcMessageBuilder.build());
        }

        @Override
        public Executor getExecutor() {
            return getExecutorInstance();
        }

    }

}
