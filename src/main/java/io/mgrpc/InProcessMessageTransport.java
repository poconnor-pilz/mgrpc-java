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
 * Class providing server and channel transports that runs in process. Useful for unit testing mgrpc.
 * To do production level in process grpc it would be better to use io.grpc.inprocess.InProcessServerBuilder
 * which will be more efficient.
 */
public class InProcessMessageTransport {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private RpcMessageHandler server;

    private final InprocServerTransport serverTransport = new InprocServerTransport();

    private final Map<String, InprocChannelTransport> channelTransports = new ConcurrentHashMap<>();
    private final Map<String, String> callIdToChannelIdMap = new ConcurrentHashMap<>();


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

    private class InprocServerTransport implements ServerMessageTransport, RpcMessageHandler {

        private final CallSequencer serverCallSequencer = new CallSequencer(getExecutorInstance(), this);


        @Override
        public void start(ServerMessageListener server) throws MessagingException {
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
        public void onCallClosed(String callId) {
            callIdToChannelIdMap.remove(callId);
            serverCallSequencer.onCallClosed(callId);
        }


        public void queueMessage(RpcMessage message) {
            serverCallSequencer.makeQueue(message.getCallId());
            serverCallSequencer.queueMessage(message);
            if(message.hasStart()){
                //Pump the start message. After this grpc will pump the rest of the messages
                serverTransport.request(message.getCallId(), 1);
            }
        }

        @Override
        public void onRpcMessage(RpcMessage message) {
            server.onRpcMessage(message);
        }


        @Override
        public void request(String callId, int numMessages) {
            serverCallSequencer.request(callId, numMessages);
        }


        @Override
        public void send(RpcMessage message) throws MessagingException {
            final String channelId = callIdToChannelIdMap.get(message.getCallId());
            if(channelId == null){
                throw new MessagingException("Channel not found for call " + message.getCallId());
            }
            final InprocChannelTransport inprocChannelTransport = channelTransports.get(channelId);
            inprocChannelTransport.queueMessage(message);
        }

        @Override
        public Executor getExecutor() {
            return getExecutorInstance();
        }

    }

    private class InprocChannelTransport implements ChannelMessageTransport, RpcMessageHandler {

        private  String channelId;
        private  ChannelMessageListener channel;

        public final CallSequencer channelCallSequencer = new CallSequencer(getExecutorInstance(), this);


        @Override
        public void start(ChannelMessageListener channel) throws MessagingException {
            this.channelId = channel.getChannelId();
            this.channel = channel;
            channelTransports.put(channelId, this);
        }

        @Override
        public void onCallClosed(String callId){
            channelCallSequencer.onCallClosed(callId);
        }

        @Override
        public void request(String callId, int numMessages) {
           channelCallSequencer.request(callId, numMessages);
        }

        public void queueMessage(RpcMessage message) {
            channelCallSequencer.queueMessage(message);
        }
        @Override
        public void send(RpcMessage.Builder rpcMessage) throws MessagingException {
            if(rpcMessage.hasStart()) {
                callIdToChannelIdMap.put(rpcMessage.getCallId(), this.channelId);
                channelCallSequencer.makeQueue(rpcMessage.getCallId());
            }
            serverTransport.queueMessage(rpcMessage.build());
        }

        @Override
        public void close() {
            channelTransports.remove(channelId);
        }


        @Override
        public Executor getExecutor() {
            return getExecutorInstance();
        }

        @Override
        public void onRpcMessage(RpcMessage message) {
            channel.onRpcMessage(message);
        }
    }

}
