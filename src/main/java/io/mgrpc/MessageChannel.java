package io.mgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MessageChannel extends Channel implements ChannelListener {


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final CallOptions.Key<String> OPT_OUT_TOPIC = CallOptions.Key.create("out-topic");


    private static final com.google.rpc.Status GOOGLE_RPC_OK_STATUS = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);



    private final ChannelConduit conduit;

    private final String channelId;
    private final int queueSize;

    private final int flowCredit;


    private final Map<String, MsgClientCall> clientCallsById = new ConcurrentHashMap<>();


    private static final int SINGLE_MESSAGE_STREAM = 0;

    public static final int DEFAULT_QUEUE_SIZE = 100;

    public static final int DEFAULT_FLOW_CREDIT = 20;


    /**
     * @param conduit           PubsubClient
     * @param channelId         The client id for the channel. Should be unique.
     * @param queueSize        The size of the message queue for each call's replies
     * @param flowCredit The amount of credit that should be issued for flow control e.g. if flow credit is 20
     *      then the sender will only send 20 messages before waiting for the receiver to send more flow credit.     */
    public MessageChannel(ChannelConduit conduit, String channelId, int queueSize, int flowCredit) {
        log.debug("Creating MessageChannel with channelId={}, queueSize={}, flowCredit{} ",
                channelId, queueSize, flowCredit);
        if(conduit == null) {
            throw new NullPointerException("conduit is null");
        }
        this.conduit = conduit;
        if(channelId == null) {
            this.channelId = Id.random();
        } else {
            this.channelId = channelId;
        }
        if(queueSize <= 0) {
            this.queueSize = DEFAULT_QUEUE_SIZE;
        } else {
            this.queueSize = queueSize;
        }
        if(flowCredit <= 0) {
            this.flowCredit = DEFAULT_FLOW_CREDIT;
        } else {
            this.flowCredit  = flowCredit;
        }
    }

    /**
     * @param conduit      Mqtt client
     */
    public MessageChannel(ChannelConduit conduit) {
        this(conduit, Id.random(), DEFAULT_QUEUE_SIZE, DEFAULT_FLOW_CREDIT);
    }


    @Override
    public String getChannelId() {
        return this.channelId;
    }

    public ChannelConduit getConduit() {
        return conduit;
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

    TopicConduit getTopicConduit(String serverTopic){
        return conduit.getTopicConduit(serverTopic, this);
    }

    /**
     * The ChannelConduit should call this message on the channel if it knows that the server
     * has disconnected.
     */
    @Override
    public void onServerDisconnected(String serverTopic) {
        //Clean up all existing calls because the server has been disconnected.
        //Interrupt the client's call queue with an unavailable status, the call will be cleaned up when it is closed.
        //We could just call close on the call directly but we want the call to finish processing whatever
        //message it has first and if we put it on the queue it means we don't have to worry about thread safety.
        for (MsgClientCall call : clientCallsById.values()) {
            if(call.getServerTopic() != null && call.getServerTopic().equals(serverTopic)) {
                final Status status = Status.UNAVAILABLE.withDescription("Server disconnected");
                final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
                RpcMessage rpcMessage = RpcMessage.newBuilder()
                        .setStatus(grpcStatus)
                        .setCallId(call.callId)
                        .setSequence(MessageProcessor.INTERRUPT_SEQUENCE)
                        .build();
                call.queueServerMessage(rpcMessage);
            }
        }

    }


    public void close() {
        conduit.close(this.channelId);
    }


    @Override
    public <RequestT, ResponseT> ClientCall newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        MsgClientCall call = new MsgClientCall<>(methodDescriptor, callOptions,
                conduit.getExecutor(), this.queueSize, this.flowCredit);
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
        if (statusProto.getMessage() == null || statusProto.getMessage().isEmpty()) {
            return status;
        } else {
            return status.withDescription(statusProto.getMessage());
        }
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
        private Status closedStatus = null;
        Deadline effectiveDeadline = null;
        Metadata metadata = null;

        private TopicConduit topicConduit;

        private String serverTopic;

        private final int flowCredit;
        private int serverCreditUsed = 0;



        private final ContextCancellationListener cancellationListener =
                new ContextCancellationListener();

        private final MessageProcessor messageProcessor;

        private final CreditHandler creditHandler;

        private MsgClientCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions,
                              Executor executor, int queueSize, int flowCredit) {
            this.clientExecutor = callOptions.getExecutor();
            this.methodDescriptor = methodDescriptor;
            this.callOptions = callOptions;
            this.context = Context.current().withCancellation();
            this.callId = Id.random();
            this.messageProcessor = new MessageProcessor(executor, queueSize, this, 0, "channel callid = " + callId);
            this.creditHandler = new CreditHandler("client call " + callId, 1);
            this.flowCredit = flowCredit;
        }

        public String getCallId() {
            return callId;
        }

        public String getServerTopic() {return serverTopic;}

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {

            //Note that start may be called by multiple threads if this call is being run by GrpcProxy
            //As part of a http server. But start is only called once per MsgClientCall so it does not
            //need to be thread safe wrt MsgClientCall (but it does wrt MesssageChannel)
            //This is true for all ClientCall methods on MsgClientCall

            //log.debug("Starting call {} on thread {}", callId, Thread.currentThread().getName());
            serverTopic = headers.get(TopicInterceptor.META_SERVER_TOPIC);
            if(serverTopic == null || serverTopic.isEmpty()) {
                final String err = "No header metadata property value specified for " + TopicInterceptor.SERVER_TOPIC
                        + " The call may not be routed properly";
                //Just warn as a conduit (like the inproc one) may not need a serverTopic
                log.warn(err);
            }

            //This call may block until conduit is started if it is not started already.
            this.topicConduit = getTopicConduit(serverTopic);


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
            this.context.addListener(cancellationListener, command -> command.run());

            //Close call if deadline exceeded
            this.effectiveDeadline = DeadlineTimer.min(callOptions.getDeadline(), context.getDeadline());
            if (effectiveDeadline != null) {
                this.deadlineCancellationFuture = DeadlineTimer.start(effectiveDeadline, (String deadlineMessage) -> {
                    //We close the call here which will call listener.onClose(Status.DEADLINE_EXCEEDED)
                    //because this is what the listener expects.
                    //The listener will then call cancel on this call.
                    log.debug("Deadline exceeded for call " + callId + ": " + deadlineMessage);
                    close(Status.DEADLINE_EXCEEDED.augmentDescription(deadlineMessage));
                });
            }

            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId).setSequence(sequence);

            Start.Builder start = Start.newBuilder();
            start.setChannelId(channelId);
            start.setCredit(this.flowCredit);
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
                //Note that we don't fully close the call here because it still has to be used to
                //send one value message to the server which will start the server stream
                //we just want to close the client call (the responseListener)
                context.removeListener(cancellationListener);
                cancelTimeouts();
                clientExec(() -> responseListener.onClose(Status.OK, new Metadata()));
                clientCallsById.remove(this.callId);
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
                topicConduit.send(msgBuilder);
            } catch (MessagingException ex) {
                throw new StatusRuntimeException(Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex));
            }



            //TODO should we call responseListener.onReady() here?
//            exec(() -> responseListener.onReady());
        }

        @Override
        public void sendMessage(ReqT message) {

            if(closed){
                //This will happen when the client code keeps calling onNext() for a call that has failed
                //(i.e. client code didn't listen for failure and stop the sends)
                log.error("Cannot send message for closed call: " + callId);
                throw new StatusRuntimeException(closedStatus.withDescription("Cannot send message for closed call: " + callId));
            }


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
                //Flow control
                //If we are out of credit then wait for the target to send more credit before flooding it with messages.
                creditHandler.waitForAndDecrementCredit();
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
            if(message.hasFlow() ){
                //Do not send flow or status messages through the queue as MessageProcessor().processQueue will
                //be blocked on the call that is waiting for credit. Instead process it directly.
                onProviderMessage(message);
                return;
            }
            //Run the message through the MessageProcessor to be ordered and checked for duplicates.
            //MessageProcsessor will then call this back in onProviderMessage
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
                case FLOW:
                    creditHandler.addCredit(message.getFlow().getCredit());
                    return;
                case VALUE:
                    clientExec(() -> {
                        if(message.getSequence() == 1) { //only send headers if this is the first response
                            responseListener.onHeaders(new Metadata());
                        }
                        responseListener.onMessage(methodDescriptor.parseResponse(message.getValue().getContents().newInput()));
                        if (message.getSequence() == SINGLE_MESSAGE_STREAM) {
                            //There is only a single message in this stream and it will not be followed by
                            //a completed message so close the stream.
                            close(Status.OK.withDescription(""));
                        } else {
                           //Issue more credit to the server if it has sent all the messages it has credit for.
                           serverCreditUsed++;
                           if(serverCreditUsed == this.flowCredit) {
                               final RpcMessage.Builder flow = RpcMessage.newBuilder()
                                       .setCallId(this.callId)
                                       .setSequence(0) //Flow messages are not ordered. They are processed immediately
                                       .setFlow(Flow.newBuilder().setCredit(this.flowCredit));
                               try {
                                   log.debug("Sending flow message");
                                   send(methodDescriptor, flow);
                               } catch (MessagingException e) {
                                   log.error("Failed to send flow control message", e);
                               }
                               serverCreditUsed = 0;
                           }
                        }
                    });
                    return;
                default:
                    log.error("Invalid message case");
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
            closedStatus = status;
            log.debug("Closing call {} with status: {} {}", new Object[]{this.callId, status.getCode(), status.getDescription()});
            context.removeListener(cancellationListener);
            cancelTimeouts();
            clientExec(() -> responseListener.onClose(status, new Metadata()));
            clientCallsById.remove(this.callId);
            topicConduit.onCallClosed(callId);
        }

        public void cancelTimeouts() {
            if (this.deadlineCancellationFuture != null) {
                this.deadlineCancellationFuture.cancel(false);
            }
        }


        @Override
        public void request(int numMessages) {
            topicConduit.request(callId, numMessages);
            messageProcessor.request(numMessages);
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
            final RpcMessage rpcMessage = messageBuilder.build();
            log.debug("Sending {} {} {} to {} ",
                    new Object[]{rpcMessage.getCallId(), rpcMessage.getSequence(), rpcMessage.getMessageCase(), methodDescriptor.getFullMethodName()});
            topicConduit.send(messageBuilder);
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
