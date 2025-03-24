package io.mgrpc;

import io.mgrpc.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Class providing server and channel conduits that runs in process. Useful for unit testing mgrpc.
 * To do production level in process grpc it would be better to use io.grpc.inprocess.InProcessServerBuilder
 * which will be more efficient.
 */
public class InProcessMessageConduit {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Map<String, ChannelMessageListener> channelsById = new ConcurrentHashMap<>();
    private ServerMessageListener server;

    private final ServerMessageConduit serverConduit = new InprocServerConduit();

    private final Map<String, String> callIdToChannelIdMap = new ConcurrentHashMap<>();

    private static volatile Executor executorSingleton;


    public ChannelMessageConduit getChannelConduit(){
        return new InprocChannelConduit();
    }

    public ServerMessageConduit getServerConduit(){
        return serverConduit;
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

    private class InprocServerConduit implements ServerMessageConduit {


        @Override
        public void start(ServerMessageListener server) throws MessagingException {
            if(InProcessMessageConduit.this.server != null){
                String err = "InProcessMessageconduit instance can only be associated with one Server";
                log.error(err);
                throw new MessagingException(err);
            }
            InProcessMessageConduit.this.server = server;
        }

        @Override
        public void close() {}

        @Override
        public void onCallClosed(String callId) {
            callIdToChannelIdMap.remove(callId);
        }

        @Override
        public void request(String callId, int numMessages) {
        }

        @Override
        public void send(RpcMessage message) throws MessagingException {
            final String channelId = callIdToChannelIdMap.get(message.getCallId());
            if(channelId == null){
                String err = "Channel " + channelId +  " does not exist";
                log.error(err);
                throw new MessagingException(err);
            }

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

    private class InprocChannelConduit implements ChannelMessageConduit {

        private String channelId;

        @Override
        public void start(ChannelMessageListener channel) throws MessagingException {
            this.channelId = channel.getChannelId();
            channelsById.put(channelId, channel);
        }

        @Override
        public void onCallClosed(String callId){}

        @Override
        public void request(String callId, int numMessages) {}

        @Override
        public void close() {
            channelsById.remove(channelId);
        }

        @Override
        public void send(RpcMessage.Builder rpcMessageBuilder) throws MessagingException {
            if(rpcMessageBuilder.hasStart()){
                callIdToChannelIdMap.put(rpcMessageBuilder.getCallId(), rpcMessageBuilder.getStart().getChannelId());
            }
            server.onMessage(rpcMessageBuilder.build());
        }

        @Override
        public Executor getExecutor() {
            return getExecutorInstance();
        }

    }

}
