package io.mgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.ServerConduit;
import io.mgrpc.messaging.ServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static io.mgrpc.RpcMessage.MessageCase.START;


public class MessageServer implements ServerListener {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private MInternalHandlerRegistry registry = new MInternalHandlerRegistry();

    private HandlerRegistry fallBackRegistry = null;
    private final static Metadata EMPTY_METADATA = new Metadata();

    private final static int CANCELLED_CODE = Status.CANCELLED.getCode().value();


    private static volatile Executor executorSingleton;


    private final ServerConduit conduit;

    private final Map<String, MessageHandler> handlersByCallId = new ConcurrentHashMap<>();

    /**
     * For various reasons a message might come in for a call that has been removed/is finished
     * This could be a from a client that didn't detect that this server went down and is still
     * sending messages for the call or maybe some stray message from the broker.
     * We will ignore these messages. The value in this map is the time the message was recieved
     * Every 10 minutes we will remove items in the map that are over 10 minutes old to make
     * sure that it doesn't grow infinitely.
     * (This will only apply to calls that do not have type clientSendsOneMessage() i.e. client
     * streaming calls)
     */
    final ConcurrentHashMap<String, Long> recentlyRemovedCallIds = new ConcurrentHashMap<>();

    private static final int DEFAULT_QUEUE_SIZE = 100;
    private final int queueSize;

    private  ScheduledFuture<?> clearRecentlyRemovedTimer;

    /**
     * @param conduit PubsubClient
     * @param queueSize         The size of the incoming message queue for each call
     */
    public MessageServer(ServerConduit conduit, int queueSize) {
        this.conduit = conduit;
        this.queueSize = queueSize;
    }


    public MessageServer(ServerConduit conduit) {
        this(conduit, DEFAULT_QUEUE_SIZE);
    }


    public void start() throws MessagingException {


        conduit.start(this);

        //Every 10 minutes clear out recentlyRemovedCallIds that are more than 10 minutes old
        clearRecentlyRemovedTimer = TimerService.get().scheduleAtFixedRate(() -> {
            final long tenMinutesAgo = System.currentTimeMillis() - (10 * 60 * 1000);
            recentlyRemovedCallIds.values().removeIf(time -> time < tenMinutesAgo);
        }, 10, 10, TimeUnit.MINUTES);

    }

    public void close() {
        conduit.close();
        if(clearRecentlyRemovedTimer != null){
            clearRecentlyRemovedTimer.cancel(false);
        }
    }


    public void addService(BindableService service) {
        //TODO: Make a removeService
        registry.addService(service);
    }

    public void addService(ServerServiceDefinition service) {
        //TODO: Make a removeService
        registry.addService(service);
    }

    public void setFallBackRegistry(HandlerRegistry fallBackRegistry) {
        this.fallBackRegistry = fallBackRegistry;
    }


    public void removeAllServices() {
        this.registry = new MInternalHandlerRegistry();
    }


    @Override
    public void onMessage(RpcMessage message) {

        final String callId = message.getCallId();
        if (callId.isEmpty()) {
            log.error("Every message sent from the client must have a callId");
            return;
        }

        //Check if this message is for a removed call and ignore it if it is
        //A message could arrive for a removed call in the case where the server was disconnected
        //but the client didn't detect this and is still sending messages for a previous call.
        //This could also occur when the call's queue has reached its limit but the client hasn't
        //received the error message yet.
        if (recentlyRemovedCallIds.contains(callId)) {
            log.warn("Message received for removed call {}. Ignoring", callId);
            return;
        }

        MessageHandler handler = handlersByCallId.get(callId);
        if (handler == null) {
            handler = new MessageHandler(callId, conduit.getExecutor(), queueSize);
            handlersByCallId.put(callId, handler);
        }
        //put the message on the handler's queue
        handler.queueClientMessage(message);

    }

    /**
     * The ServerConduit should call this message on the server if it knows that a channel
     * has disconnected.
     */
    @Override
    public void onChannelDisconnected(String channelId) {
        //When a client is disconnected we need to cancel all calls for it so that they get cleaned up.
        for (String callId : this.handlersByCallId.keySet()) {
            MessageHandler messageHandler = this.handlersByCallId.get(callId);
            String foundChannelId = messageHandler.getChannelId();
            if (foundChannelId != null && foundChannelId.equals(channelId)) {
                final Status status = Status.CANCELLED.withDescription("Client disconnected");
                final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
                RpcMessage rpcMessage = RpcMessage.newBuilder()
                        .setStatus(grpcStatus)
                        .setCallId(callId)
                        .setSequence(MessageProcessor.INTERRUPT_SEQUENCE)
                        .build();
                //Note that because this is a cancel it will get processed immediately by queueClientMessage
                //so setting INTERRUPT_SEQUENCE above was strictly unnecessary.
                messageHandler.queueClientMessage(rpcMessage);
            }
        }
    }



    private void sendStatus(String callId, int sequence, Status status) {
        final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        RpcMessage message = RpcMessage.newBuilder()
                .setStatus(grpcStatus)
                .setCallId(callId)
                .setSequence(sequence)
                .build();
        if (!status.isOk()) {
            log.error("Sending error: " + status);
        } else {
            log.debug("Sending completed: " + status);
        }
        try {
            conduit.send(message);
        } catch (MessagingException e) {
            //We cannot do anything here to help the client because there is no way of sending a message so just log.
            log.error("Failed to send status", e);
        }
    }




    public Stats getStats() {
        return new Stats(handlersByCallId.size());
    }

    public void removeCall(String callId) {
        final MessageHandler handler = handlersByCallId.get(callId);
        if (handler != null && handler.serverCall != null) {
            handlersByCallId.remove(callId);
            //Only put handlers that take a client stream in the recentlyRemovedCallIds
            //This is because we do not expect any more messages for a service where the client only sends one message
            if (!handler.serverCall.methodDescriptor.getType().clientSendsOneMessage()) {
                recentlyRemovedCallIds.put(callId, System.currentTimeMillis());
            }
        }
        log.debug("Call {} removed for client messages", callId);
    }


    private class MessageHandler implements MessageProcessor.MessageHandler {
        private final String callId;
        private String channelId;

        private MsgServerCall serverCall;

        /**
         * This will be set when the call is half closed or removed so that it will no longer
         * process client calls.
         */
        private boolean removed = false;


        private final MessageProcessor messageProcessor;

        private MessageHandler(String callId, Executor executor, int queueSize) {
            this.callId = callId;
            messageProcessor = new MessageProcessor(executor, queueSize, this);
        }

        public String getChannelId() {
            return channelId;
        }

        public void queueClientMessage(RpcMessage message) {

            //Check if this is a cancel message.
            //If we queue a cancel message it won't get processed until after the previous message
            //which is what we are trying to cancel so we need to cancel straight away
            //We should only ever receive an error of type CANCELLED because the grpc client
            //implementation converts any error status to CANCELLED anyway.
            //(grpc models CANCELLED as RST_STREAM on the wire but we use a Status because we have high
            //level messaging)
            if (message.hasStatus()) {
                if (message.getStatus().getCode() == CANCELLED_CODE) {
                    //Don't run the cancel on the mqtt message thread
                    conduit.getExecutor().execute(() -> {
                        log.debug("Canceling");
                        //If the call was constructed, cancel it.
                        if (serverCall != null) {
                            serverCall.cancel();
                        } else {
                            log.debug("ServerCall null in queueClientMessage() for cancel message. Call" + callId);
                        }
                        this.remove();
                    });
                    return;
                }
            }
            messageProcessor.queueMessage(message);
        }


        /**
         * onMessage() may be called from multiple threads but only one onMessage will be active at a time.
         * So it is thread safe with respect to itself but cannot use thread locals
         *
         * @param message
         */
        @Override
        public void onProviderMessage(RpcMessage message) {

            if (removed) {
                log.debug("Message received for removed handler {}, returning", callId);
                return;
            }

            try {
                if (message.getMessageCase() != START) {
                    if (serverCall == null) {
                        //We never received a valid first message for this call or the first message is not of type START
                        log.error("Unrecognised call id: " + callId);
                        this.remove();
                        return;
                    }
                    //This is a non-start message so just send it on to the service
                    serverCall.onClientMessage(message);
                    return;
                }

                //This is a start message so we need to construct the server call.
                final Start start = message.getStart();

                String fullMethodName = start.getMethodName();
                //fullMethodName is e.g. "helloworld.ExampleHelloService/SayHello"
                ServerMethodDefinition<?, ?> serverMethodDefinition = registry.lookupMethod(fullMethodName);
                if (serverMethodDefinition == null) {
                    if (fallBackRegistry != null) {
                        serverMethodDefinition = fallBackRegistry.lookupMethod(fullMethodName);
                    }
                }
                if (serverMethodDefinition == null) {
                    sendStatus(callId, 1,
                            Status.UNIMPLEMENTED.withDescription("No method registered for " + fullMethodName));
                    return;
                }

                log.debug("Received {} {} {} on {}", new Object[]{message.getCallId(),
                        message.getSequence(),
                        message.getMessageCase(),
                        serverMethodDefinition.getMethodDescriptor().getFullMethodName()});


                //Note that we cannot validate the type of the method here with something like
                //serverMethodDefinition.getMethodDescriptor().getType() != MethodTypeConverter.fromStart(start.getMethodType())
                //Because the type may not be set in the case of GrpcProxy  where the proxy cannot know the type of the
                //method being implemented by the remote server (whether the remote server is http or message/mqtt)

                final ServerCallHandler<?, ?> serverCallHandler = serverMethodDefinition.getServerCallHandler();

                serverCall = new MsgServerCall<>(serverMethodDefinition.getMethodDescriptor(),
                        start, callId);

                //Extract the channelId from the header. It may be used later to find calls for a
                //particular client that need to be cleaned up because the client has disconnected.
                this.channelId = start.getChannelId();

                serverCall.start(serverCallHandler);

            } catch (Exception ex) {
                log.error("Error processing RpcMessage", ex);
            }

        }

        /**
         * onQueueCapacityExceeded() is not thread safe and can be called at the same time as
         * ongoing onMessage() call
         */
        @Override
        public void onQueueCapacityExceeded() {
            log.error("Service queue capacity exceeded for call " + callId);
            if (serverCall != null) {
                serverCall.close(Status.RESOURCE_EXHAUSTED.withDescription("Service queue capacity exceeded."), EMPTY_METADATA);
            }
        }


        public void remove() {
            MessageServer.this.removeCall(this.callId);
            this.removed = true;
        }


        /**
         * This represents the call. start() will start the call and call the service method implementation.
         * After start() the ServerCall will have a listener and can send messages to the request/client stream of the method
         * by calling listener.onMessage(). When the method implementation sends a message to the response/server
         * stream of the method it will call ServerCall.sendMessage()
         */
        private class MsgServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {

            final MethodDescriptor<ReqT, RespT> methodDescriptor;
            final Start start;
            final String callId;
            int sequence = 0;
            private Listener listener;
            private boolean cancelled = false;
            private ScheduledFuture<?> deadlineCancellationFuture = null;

            private Context.CancellableContext cancellableContext = null;
            private Context context = null;

            MsgServerCall(MethodDescriptor<ReqT, RespT> methodDescriptor, Start start, String callId) {
                this.methodDescriptor = methodDescriptor;
                this.start = start;
                this.callId = callId;
            }

            public void start(ServerCallHandler<?, ?> serverCallHandler) {
                log.debug("Starting call " + callId + " to " + this.methodDescriptor.getFullMethodName());
                if (start.getTimeoutMillis() > 0) {
                    Deadline deadline = Deadline.after(start.getTimeoutMillis(), TimeUnit.MILLISECONDS);
                    this.deadlineCancellationFuture = DeadlineTimer.start(deadline, (String deadlineMessage) -> {
                        cancel();
                        MessageHandler.this.remove();
                    });
                }
                //TODO: Populate the key,value pairs of cancellableContext with e.g. auth credentials from this.header
                //For the moment just put in a dummy key to help with identification/debugging of context
                cancellableContext = Context.ROOT.withCancellation();
                Context.Key<Integer> akey = Context.key("akey");
                context = cancellableContext.withValue(akey, 99);

                Metadata metadata = new Metadata();
                final List<MetadataEntry> entries = start.getMetadataList();
                for (MetadataEntry entry : entries) {
                    final Metadata.Key<String> key = Metadata.Key.of(entry.getKey(), Metadata.ASCII_STRING_MARSHALLER);
                    metadata.put(key, entry.getValue());
                }
                //Not that serverCallHandler.startCall() will call the implementation of e.g. sayHello() so
                //all the context etc must be set up so that sayHello can add cancel listeners, get creds,
                //possibly send an error on the responseStream etc.
                //After the implementation of sayHello is called the rest of the interaction is done via
                //the streams that sayHello returns and accepts
                context.run(() -> {
                    this.listener = serverCallHandler.startCall(serverCall, metadata);
                });
            }


            @Override
            public void request(int numMessages) {
                //log.debug("request(" + numMessages + ")");
                conduit.request(callId, numMessages);
            }

            @Override
            public void sendHeaders(Metadata headers) {

            }

            public void onClientMessage(RpcMessage message) {

                Value value;
                switch (message.getMessageCase()) {
                    case VALUE:
                        value = message.getValue();
                        break;
                    case STATUS:
                        //MgMessageHandler will have already checked for a cancel so this is just an ok end of stream
                        //We do not call remove() here as the call needs to remain available to handle a cancel
                        //It will be removed when close() is called by the listener/service implementation
                        listener.onHalfClose();
                        return;
                    default:
                        log.error("Unrecognised message case " + message.getMessageCase());
                        return;
                }

                Object objValue;
                //Note that methodDescriptor.parseRequest will not fail no matter what is in the protocol buffer
                //There is no type information in protocol buffers. https://protobuf.dev/programming-guides/encoding/
                //So there is no way to tell the difference between the wrong type and a different version of the type.
                //The object is constructed with default non-null values (see for example the
                //default constructor in the generated code for HelloRequest). Then if the parse code finds a value
                //at the expected index in the buffer it will set it in the object. Otherwise it does nothing.
                objValue = methodDescriptor.parseRequest(value.getContents().newInput());

                //Make sure the listener is run in context so that the listener/observer code can get e.g.
                //auth credentials or add a Context.CancellationListener etc.
                //Note its importan to use the run method here which will swap this.context back out of the
                //threadlocal in a finally block. If it didn't then the context and its listeners could remain
                //in the threadlocal indefinitely and not get garbage collected.
                context.run(() -> {
                    listener.onMessage(objValue);
                });


            }


            @Override
            public void sendMessage(RespT message) {
                //Send the response up to the client

                sequence++;

                ByteString valueByteString;
                if (message instanceof MessageLite) {
                    valueByteString = ((MessageLite) message).toByteString();
                } else {
                    //If we use GrpcProxy then the message will be a byte array
                    valueByteString = ByteString.copyFrom((byte[]) message);
                }

                RpcMessage rpcMessage = RpcMessage.newBuilder()
                        .setValue(Value.newBuilder().setContents(valueByteString))
                        .setCallId(callId)
                        .setSequence(sequence).build();

                try {
                    log.debug("Sending {} {} {} to {} ",
                            new Object[]{rpcMessage.getCallId(), rpcMessage.getSequence(), rpcMessage.getMessageCase(), methodDescriptor.getFullMethodName()});
                    conduit.send(rpcMessage);
                } catch (MessagingException e) {
                    log.error("Failed to send", e);
                    throw new StatusRuntimeException(Status.UNAVAILABLE);//.withCause(e));
                }
            }

            public void cancelTimeouts() {
                if (this.deadlineCancellationFuture != null) {
                    this.deadlineCancellationFuture.cancel(false);
                }
            }

            @Override
            public void close(Status status, Metadata trailers) {
                //close will be called when the service implementation calls onCompleted (among other things)
                sequence++;
                sendStatus(callId, sequence, status);
                cancelTimeouts();
                MessageHandler.this.remove();
                conduit.onCallClosed(callId);
            }

            public void cancel() {
                //Note that cancel is not called from a synchronized method and so all data
                //accessed here needs to be thread safe.
                log.debug("cancel()");
                if (cancelled) {
                    return;
                }
                cancelled = true;
                cancelTimeouts();
                MessageHandler.this.remove();
                //listener or cancellableContext could be null here in the case where a cancel
                //gets in so quickly that the call hasn't even been fully started yet.
                //This only seems to happen in bad test code that doesn't wait for the call to start.
                if (listener != null) {
                    //listener.onCancel() here will call StreamingServerCallListener.onCancel()
                    //This will call responseObserver.onCancelHandler.run() and it will
                    //put an error in the service request stream if it has one (i.e. if it takes more than one request)
                    //The error status will be: CANCELLED: client cancelled
                    listener.onCancel();
                } else {
                    //This only seems to happen in bad test code that doesn't wait for the call to start.
                    log.warn("Listener null during cancel for call " + callId + " The call isn't fully started yet");
                }
                //Call cancel on the context. For whatever reason, grpc allows two ways for the
                //implementation to listen for cancels. One is in the onCancelHandler() above and
                //the other is to do a Context.addListener(new Context.CancellationListener()...
                //so we need to call this listener here by calling context.cancel()
                if (cancellableContext != null) {
                    cancellableContext.cancel(null);
                } else {
                    //This only seems to happen in bad test code that doesn't wait for the call to start.
                    log.warn("cancellableContext null during cancel for call" + callId + " The call isn't fully started yet");
                }
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
                return methodDescriptor;
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
