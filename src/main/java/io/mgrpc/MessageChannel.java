package io.mgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import io.mgrpc.messaging.ChannelMessageListener;
import io.mgrpc.messaging.ChannelMessageTransport;
import io.mgrpc.messaging.DisconnectListener;
import io.mgrpc.messaging.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MessageChannel extends Channel implements ChannelMessageListener {


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 10 * 1000;

    public static final CallOptions.Key<String> OPT_OUT_TOPIC = CallOptions.Key.create("out-topic");


    private final static Metadata EMPTY_METADATA = new Metadata();

    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);

    private static volatile Executor executorSingleton;


    private final ChannelMessageTransport transport;

    private final String channelId;

    private boolean started = false;

    private final Map<String, MsgClientCall> clientCallsById = new ConcurrentHashMap<>();

    private final List<DisconnectListener> disconnectListeners = new ArrayList<>();

    private static final int SINGLE_MESSAGE_STREAM = 0;

    public static final int DEFAULT_QUEUE_SIZE = 100;
    private final int queueSize;


    /**
     * @param transport           PubsubClient
     * @param channelId         The client id for the channel. Should be unique.
     * @param queueSize        The size of the message queue for each call's replies
     */
    public MessageChannel(ChannelMessageTransport transport, String channelId, int queueSize) {
        this.transport = transport;
        this.channelId = channelId;
        this.queueSize = queueSize;
    }

    /**
     * @param transport      PubsubClient
     * @param queueSize   The size of the message queue for each call's replies
     */
    public MessageChannel(ChannelMessageTransport transport, int queueSize) {
        this(transport, Id.random(), queueSize);
    }


    /**
     * @param transport      Mqtt client
     * @param channelId    The client id for the channel. Should be unique.
     */
    public MessageChannel(ChannelMessageTransport transport, String channelId) {
        this(transport, channelId, DEFAULT_QUEUE_SIZE);
    }

    /**
     * @param transport      Mqtt client
     */
    public MessageChannel(ChannelMessageTransport transport) {
        this(transport, Id.random());
    }


    /**
     * A client must call init() before using any other methods on the channel.
     *
     * @throws StatusException
     */
    public void start() throws MessagingException {

        transport.start(this);
        started = true;
    }

    boolean isStarted(){
        return started;
    }

    @Override
    public String getChannelId() {
        return this.channelId;
    }

    @Override
    public void onMessage(RpcMessage message)  {
        final MsgClientCall call = clientCallsById.get(message.getCallId());
        if (call == null) {
            log.error("Could not find call " + message.getCallId() + " for message " + message.getSequence());
            return;
        }
        call.queueServerMessage(message);

    }

    /**
     * The ChannelMessageTransport should call this message on the channel if it knows that the server
     * has disconnected.
     */
    @Override
    public void onDisconnect(String channelId) {
        //Clean up all existing calls because the server has been disconnected.
        //Interrupt the client's call queue with an unavailable status, the call will be cleaned up when it is closed.
        //We could just call close on the call directly but we want the call to finish processing whatever
        //message it has first and if we put it on the queue it means we don't have to worry about thread safety.
        for (MsgClientCall call : clientCallsById.values()) {
            final Status status = Status.UNAVAILABLE.withDescription("Server disconnected");
            final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
            RpcMessage rpcMessage = RpcMessage.newBuilder()
                    .setStatus(grpcStatus)
                    .setCallId(call.callId)
                    .setSequence(MessageProcessor.INTERRUPT_SEQUENCE)
                    .build();
            call.queueServerMessage(rpcMessage);
        }

        for(DisconnectListener listener: disconnectListeners){
            listener.onDisconnect(this.channelId);;
        }

    }

    public void addDisconnectListener(DisconnectListener listener) {
        disconnectListeners.add(listener);
    }


    public void close() {
        transport.close();
    }


    @Override
    public <RequestT, ResponseT> ClientCall newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        MsgClientCall call = new MsgClientCall<>(methodDescriptor, callOptions, transport.getExecutor(), queueSize);
        clientCallsById.put(call.getCallId(), call);
        return call;
    }

    @Override
    public String authority() {
        return "";
    }

    public Stats getStats() {
        return new Stats(clientCallsById.size());
    }


    /**
     * Convert com.google.rpc.Status to io.grpc.Status
     * Copied more or less from StatusProto.toStatus() as that is private
     */
    public static Status googleRpcStatusToGrpcStatus(com.google.rpc.Status statusProto) {
        Status status = Status.fromCodeValue(statusProto.getCode());
        return status.withDescription(statusProto.getMessage());
    }



    private class MsgClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> implements MessageProcessor.MessageHandler {

        final MethodDescriptor<ReqT, RespT> methodDescriptor;
        final CallOptions callOptions;
        final Context.CancellableContext context;
        final Executor clientExecutor;
        final String callId;
        int sequence = 0;
        Listener<RespT> responseListener;
        private ScheduledFuture<?> deadlineCancellationFuture;
        private boolean cancelCalled = false;
        private boolean closed = false;
        /**
         * List of recent sequence ids, Used for checking for duplicate messages
         */
        private Recents recents = new Recents();
        Deadline effectiveDeadline = null;
        Metadata metadata = null;


        private final ContextCancellationListener cancellationListener =
                new ContextCancellationListener();

        private final MessageProcessor messageProcessor;

        private MsgClientCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions,
                              Executor executor, int queueSize) {
            this.clientExecutor = callOptions.getExecutor();
            this.methodDescriptor = methodDescriptor;
            this.callOptions = callOptions;
            this.context = Context.current().withCancellation();
            this.callId = Id.random();
            messageProcessor = new MessageProcessor(executor, queueSize, this);
        }

        public String getCallId() {
            return callId;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {

            this.responseListener = responseListener;
            //If the client specified authentication details then merge them into the metadata
            final CallCredentials credentials = this.callOptions.getCredentials();
            if (credentials != null) {
                MetadataMerger merger = new MetadataMerger(headers);
                CallCredentials.RequestInfo requestInfo = new CallCredentials.RequestInfo() {
                    public MethodDescriptor<?, ?> getMethodDescriptor() {
                        return methodDescriptor;
                    }

                    public CallOptions getCallOptions() {
                        return callOptions;
                    }

                    public SecurityLevel getSecurityLevel() {
                        return SecurityLevel.NONE;
                    }

                    public String getAuthority() {
                        return "";
                    }

                    public Attributes getTransportAttrs() {
                        return null;
                    }
                };
                Executor inlineExecutor = Runnable::run;
                credentials.applyRequestMetadata(requestInfo, inlineExecutor, merger);
                this.metadata = merger.getMetaData();
            } else {
                this.metadata = headers;
            }


            //Listen for cancellations
            context.addListener(cancellationListener, command -> command.run());

            //Close call if deadline exceeded
            effectiveDeadline = DeadlineTimer.min(callOptions.getDeadline(), context.getDeadline());
            if (effectiveDeadline != null) {
                this.deadlineCancellationFuture = DeadlineTimer.start(effectiveDeadline, (String deadlineMessage) -> {
                    //We close the call here which will call listener.onClose(Status.DEADLINE_EXCEEDED)
                    //because this is what the listener expects.
                    //The listener will then call cancel on this call.
                    log.debug("Deadline exceeded for call " + callId);
                    close(Status.DEADLINE_EXCEEDED.augmentDescription(deadlineMessage));
                });
            }

            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId).setSequence(sequence);

            Start.Builder start = Start.newBuilder();
            start.setChannelId(channelId);
            start.setMethodName(methodDescriptor.getFullMethodName());
            start.setMethodType(MethodTypeConverter.toStart(methodDescriptor.getType()));
            if (effectiveDeadline != null) {
                start.setTimeoutMillis(effectiveDeadline.timeRemaining(TimeUnit.MILLISECONDS));
            }
            final String responseTopic = this.callOptions.getOption(OPT_OUT_TOPIC);
            if (responseTopic != null && responseTopic.trim().length() != 0) {
                //If the client specified a responseTopic then set that in the message to the server and
                //close the call. The client should have already done a subscribe to receive the responses
                //Note that deadlines will be ignored in this case (although they will be passed to the server)
                log.debug("replyTo topic = " + responseTopic);
                start.setServerStreamTopic(responseTopic);
                close(Status.OK);
            }
            //Add metadata to start
            Set<String> keys = metadata.keys();
            for (String key : keys) {
                final String mvalue = metadata.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
                MetadataEntry entry = MetadataEntry.newBuilder()
                        .setKey(key)
                        .setValue(mvalue).build();
                start.addMetadata(entry);
            }

            msgBuilder.setStart(start);
            try {
                transport.send(msgBuilder);
            } catch (MessagingException ex) {
                throw new StatusRuntimeException(Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex));
            }

            //TODO should we call responseListener.onReady() here?
//            exec(() -> responseListener.onReady());
        }

        @Override
        public void sendMessage(ReqT message) {

            sequence++;
            //Send message to the server
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId)
                    .setSequence(sequence);


            ByteString valueByteString;
            if (message instanceof MessageLite) {
                valueByteString = ((MessageLite) message).toByteString();
            } else {
                //If we use GrpcProxy then the message will be a byte array
                valueByteString = ByteString.copyFrom((byte[]) message);
            }
            final Value value = Value.newBuilder()
                    .setContents(valueByteString).build();
            msgBuilder.setValue(value);
            try {
                send(methodDescriptor, msgBuilder);
            } catch (MessagingException ex){
                this.close(Status.UNAVAILABLE.withDescription(ex.getMessage()).withCause(ex));
                return;
            }
            responseListener.onReady();
        }


        private final class ContextCancellationListener implements Context.CancellationListener {
            @Override
            public void cancelled(Context context) {
                log.debug("ContextCancellationListener cancelled()");
                Throwable cause = context.cancellationCause();
                if (cause == null) {
                    cause = new Exception("Cancelled without cause");
                }
                cancel(cause.getMessage(), cause);
            }
        }


        public void queueServerMessage(RpcMessage message) {
            this.messageProcessor.queueMessage(message);
        }


        /**
         * onMessage() may be called from multiple threads but only one onMessage will be active at a time.
         * So it is thread safe with respect to itself but cannot use thread locals
         *
         * @param message
         */
        @Override
        public void onProviderMessage(RpcMessage message) {

            log.debug("Received {} {} {} on {}", new Object[]{message.getCallId(),
                    message.getSequence(),
                    message.getMessageCase(),
                    methodDescriptor.getFullMethodName()
                    });

            switch (message.getMessageCase()) {
                case STATUS:
                    close(googleRpcStatusToGrpcStatus(message.getStatus()));
                    return;
                case VALUE:
                    clientExec(() -> {
                        if(message.getSequence() == 1) { //only send headers if this is the first response
                            responseListener.onHeaders(EMPTY_METADATA);
                        }
                        responseListener.onMessage(methodDescriptor.parseResponse(message.getValue().getContents().newInput()));
                        if (message.getSequence() == SINGLE_MESSAGE_STREAM) {
                            //There is only a single message in this stream and it will not be followed by
                            //a completed message so close the stream.
                            close(Status.OK.withDescription(""));
                        }
                    });
                    return;
                default:
                    log.error("Invalid message case");
                    return;
            }
        }

        /**
         * onQueueCapacityExceeded() is not thread safe and can be called at the same time as an
         * ongoing onMessage() call
         */
        @Override
        public void onQueueCapacityExceeded() {
            log.error("Client queue capacity exceeded for call " + callId);

            //Send a cancel on to the server. We cannot send it an error on its client stream as it may only expect one message
            //On the server side the listener.onCancel will cause an error to be sent to the client stream if it has one.
            final com.google.rpc.Status cancelled = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.CANCELLED, null);
            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId)
                    .setSequence(sequence)
                    .setStatus(cancelled);
            try {
                send(methodDescriptor, msgBuilder);
            } catch (MessagingException e) {
                log.error("onQueueCapacityExceeded() failed to send cancelled status", e);
            }

            this.close(Status.RESOURCE_EXHAUSTED.withDescription("Client queue capacity exceeded."));
        }

        public void close(Status status) {
            closed = true;
            log.debug("Closing call {} with status: {} {}", new Object[]{this.callId, status.getCode(), status.getDescription()});
            context.removeListener(cancellationListener);
            cancelTimeouts();
            clientExec(() -> responseListener.onClose(status, EMPTY_METADATA));
            clientCallsById.remove(this.callId);
            transport.onCallClosed(callId);
        }

        public void cancelTimeouts() {
            if (this.deadlineCancellationFuture != null) {
                this.deadlineCancellationFuture.cancel(false);
            }
        }


        @Override
        public void request(int numMessages) {
            //Do nothing here as we don't implement backpressure.
            //This would be used to send a message to the service to tell it to sent on numMessages
            //But our services will send on messages when they have them (for the moment anyway)
//            log.debug("request({})", numMessages);
            transport.request(callId, numMessages);
        }

        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
            //Note that cancel is not called from a synchronized method and so all data
            //accessed here needs to be thread safe.
            if (closed) {
                //In the case of a deadline timeout we will call this.close(Status.CANCELLED)
                //which will call responseListener.onClose(Status.CANCELLED)
                //The listener will then call this.cancel()
                //In this case we do nothing. We don't want to send the cancel on to the server
                //because the server will have its own timeout handler.
                return;
            }
            if (cancelCalled) {
                return;
            }
            log.debug("Call cancelled");
            cancelCalled = true;
            Status status = Status.CANCELLED;
            if (message != null) {
                status = status.withDescription(message);
            } else {
                status = status.withDescription("Call cancelled without message");
            }
            if (cause != null) {
                status = status.withCause(cause);
            }

            final com.google.rpc.Status cancelled = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.CANCELLED, null);
            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId)
                    .setSequence(sequence)
                    .setStatus(cancelled);
            try {
                send(methodDescriptor, msgBuilder);
            } catch (MessagingException ex) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription(ex.getMessage()).withCause(ex));
            }

            close(status);

        }


        @Override
        public void halfClose() {
            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId)
                    .setSequence(sequence)
                    .setStatus(GOOGLE_RPC_OK_STATUS);
            try {
                send(methodDescriptor, msgBuilder);
            } catch (MessagingException ex) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription(ex.getMessage()).withCause(ex));
            }
        }


        private void send(MethodDescriptor methodDescriptor, RpcMessage.Builder messageBuilder) throws MessagingException {
            //fullMethodName will be e.g. "helloworld.ExampleHelloService/LotsOfReplies"
            if (!started) {
                throw new MessagingException("channel.init() was not called");
            }
            final RpcMessage rpcMessage = messageBuilder.build();
            log.debug("Sending {} {} {} to {} ",
                    new Object[]{rpcMessage.getCallId(), rpcMessage.getSequence(), rpcMessage.getMessageCase(), methodDescriptor.getFullMethodName()});
            transport.send(messageBuilder);
        }


        private void clientExec(Runnable runnable) {
            //If the caller supplies an executor we must run on it (otherwise for example BlockingStub will just freeze)
            //It looks like this is only set when a BlockingStub is used and in that case it will be a
            //ClientCalls$ThreadlessExecutor
            if (this.clientExecutor != null) {
                this.clientExecutor.execute(runnable);
            } else {
                runnable.run();
            }
        }


    }



    public static class Stats {
        private final int activeCalls;

        public Stats(int activeCalls) {
            this.activeCalls = activeCalls;
        }

        public int getActiveCalls() {
            return activeCalls;
        }

    }


}
