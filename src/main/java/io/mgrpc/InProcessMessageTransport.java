package io.mgrpc;

import io.grpc.Status;
import io.mgrpc.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.mgrpc.MethodTypeConverter.methodType;

/**
 * Class providing server and channel transports that runs in process. Useful for unit testing mgrpc.
 * To do production level in process grpc it would be better to use io.grpc.inprocess.InProcessServerBuilder
 * which will be more efficient.
 */
public class InProcessMessageTransport {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private RpcMessageHandler server;

    private final InprocServerTransport serverTransport = new InprocServerTransport();

    private final Map<String, InprocChannelTransport> channelTransportsByCallId = new ConcurrentHashMap<>();

    private Map<String, RpcMessage.Builder> startMessages = new ConcurrentHashMap<>();


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
            channelTransportsByCallId.remove(callId);
            serverCallSequencer.onCallClosed(callId);
            startMessages.remove(callId);
        }


        public void queueSet(RpcSet rpcSet) {
            final RpcMessage firstMessage = rpcSet.getMessages(0);
            serverCallSequencer.makeQueue(firstMessage.getCallId());
            serverCallSequencer.queueSet(rpcSet);
            if(firstMessage.hasStart()){
                //Pump the start message. After this grpc will pump the rest of the messages
                this.request(firstMessage.getCallId(), 1);
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

            final RpcMessage.Builder startMessage = startMessages.get(message.getCallId());
            if (startMessage == null) {
                String err = "No cached start message for call " + message.getCallId();
                log.error(err);
                throw new MessagingException(err);
            }

            final RpcSet.Builder setBuilder = RpcSet.newBuilder();
            if (methodType(startMessage).serverSendsOneMessage()) {
                if (message.hasValue()) {
                    //Send the value and the status as a set in one message to the broker
                    setBuilder.addMessages(message);
                    final RpcMessage.Builder statusBuilder = RpcMessage.newBuilder()
                            .setCallId(message.getCallId())
                            .setSequence(message.getSequence() + 1)
                            .setStatus(GOOGLE_RPC_OK_STATUS);
                    setBuilder.addMessages(statusBuilder);
                } else {
                    if (message.getStatus().getCode() != Status.OK.getCode().value()) {
                        setBuilder.addMessages(message);
                    } else {
                        //Ignore non error status values (non cancel values) as the status will already have been sent automatically above
                        return;
                    }
                }
            } else {
                setBuilder.addMessages(message);
            }

            final InprocChannelTransport inprocChannelTransport = channelTransportsByCallId.get(message.getCallId());
            inprocChannelTransport.queueSet(setBuilder.build());
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
        }

        @Override
        public void onCallClosed(String callId){
            channelCallSequencer.onCallClosed(callId);
        }

        @Override
        public void request(String callId, int numMessages) {
           channelCallSequencer.request(callId, numMessages);
        }

        public void queueSet(RpcSet rpcSet) {
            channelCallSequencer.queueSet(rpcSet);
        }
        @Override
        public void send(RpcMessage.Builder messageBuilder) throws MessagingException {
            RpcMessage.Builder start = startMessages.get(messageBuilder.getCallId());
            if(start == null){
                if(messageBuilder.hasStart()){
                    start = messageBuilder;
                    startMessages.put(messageBuilder.getCallId(), messageBuilder);
                    channelTransportsByCallId.put(messageBuilder.getCallId(), this);
                    channelCallSequencer.makeQueue(messageBuilder.getCallId());
                } else {
                    if(messageBuilder.hasStatus()){
                        log.warn("Call cancelled or half closed  before start. An exception may have occurred");
                        return;
                    } else {
                        throw new RuntimeException("First message sent to transport must be a start message. Call " + messageBuilder.getCallId() + " type " + messageBuilder.getMessageCase());
                    }
                }
            }

            final RpcSet.Builder rpcSet = RpcSet.newBuilder();

            if (methodType(start).clientSendsOneMessage()) {
                //If clientSendsOneMessage we only want to send one broker message containing
                //start, request, status.
                if (messageBuilder.hasStart()) {
                    //Wait for the request value
                    return;
                }
                if (messageBuilder.hasStatus()) {
                    if (messageBuilder.getStatus().getCode() != Status.OK.getCode().value()) {
                        rpcSet.addMessages(messageBuilder);
                    } else {
                        //Ignore non error status values (non cancel values) as the status will already have been sent automatically below
                        return;
                    }
                } else {
                    rpcSet.addMessages(start);
                    rpcSet.addMessages(messageBuilder);
                    final RpcMessage.Builder statusBuilder = RpcMessage.newBuilder()
                            .setCallId(messageBuilder.getCallId())
                            .setSequence(messageBuilder.getSequence() + 1)
                            .setStatus(GOOGLE_RPC_OK_STATUS);
                    rpcSet.addMessages(statusBuilder);
                }
            } else {
                rpcSet.addMessages(messageBuilder);
            }

            serverTransport.queueSet(rpcSet.build());
        }

        @Override
        public void close() {
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
