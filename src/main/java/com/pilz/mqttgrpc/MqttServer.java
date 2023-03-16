package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.protobuf.ProtoMethodDescriptorSupplier;
import io.grpc.protobuf.StatusProto;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

import static com.pilz.mqttgrpc.RpcMessage.MessageCase.START;


public class MqttServer {

    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private MqttInternalHandlerRegistry registry = new MqttInternalHandlerRegistry();

    private final static Metadata EMPTY_METADATA = new Metadata();

    private final static int CANCELLED_CODE = Status.CANCELLED.getCode().value();


    private static volatile Executor executorSingleton;

    private static Executor getExecutorInstance() {
        if (executorSingleton == null) {
            synchronized (MqttServer.class) {
                if (executorSingleton == null) {
                    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum or maybe it should be passed as a constructor parameter?
                    executorSingleton = Executors.newCachedThreadPool();
                }
            }
        }
        return executorSingleton;
    }

    private final MqttAsyncClient client;

    private final Map<String, MgMessageHandler> handlersByCallId = new ConcurrentHashMap<>();

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

    private final String serverTopic;
    private final Executor executor;
    private static final int DEFAULT_QUEUE_SIZE = 100;
    private final int queueSize;

    private static final int SINGLE_MESSAGE_STREAM = 0;

    public MqttServer(MqttAsyncClient client, String serverTopic, int queueSize, Executor executor) {
        this.client = client;
        this.serverTopic = serverTopic;
        this.executor = executor;
        this.queueSize = queueSize;
    }

    public MqttServer(MqttAsyncClient client, String serverTopic, int queueSize) {
        this(client, serverTopic, queueSize, getExecutorInstance());
    }

    public MqttServer(MqttAsyncClient client, String serverTopic) {
        this(client, serverTopic, DEFAULT_QUEUE_SIZE, getExecutorInstance());
    }


    public void addService(BindableService service) {
        //TODO: Make a removeService
        registry.addService(service);
    }

    public void addService(ServerServiceDefinition service) {
        //TODO: Make a removeService
        registry.addService(service);
    }


    public void removeAllServices() {
        this.registry = new MqttInternalHandlerRegistry();
    }

    public void init() throws MqttException {
        String allServicesIn = Topics.allServicesIn(serverTopic);
        log.debug("subscribe server at: " + allServicesIn);

        client.subscribe(allServicesIn, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
            //We use an MqttExceptionLogger here because if a we throw an exception in the subscribe handler
            //it will disconnect the mqtt client
            final RpcMessage message = RpcMessage.parseFrom(mqttMessage.getPayload());
            log.debug("Received {} {} {} on : {}", new Object[]{message.getMessageCase(),
                    message.getSequence(), Id.shrt(message.getCallId()), topic});
            final String callId = message.getCallId();
            if (callId.isEmpty()) {
                log.error("Every message sent from the client must have a callId");
                return;
            }

            //Check if this message is for a removed call and ignore it if it is
            //A message could arrive for a removed call in the case where the server was disconnected
            //but the client didn't detect this and is still sending messages for a previous call.
            //This could also occur when the call's queue has reached its limit but the client hasn't
            //got the error message yet.
            if (recentlyRemovedCallIds.contains(callId)) {
                log.warn("Message received for removed call {}. Ignoring", Id.shrt(callId));
                return;
            }

            MgMessageHandler handler = handlersByCallId.get(callId);
            if (handler == null) {
                handler = new MgMessageHandler(callId, executor, queueSize);
                handlersByCallId.put(callId, handler);
            }
            //put the message on the handler's queue
            handler.queueClientMessage(new MessageProcessor.MessageWithTopic(topic, message));

        })).waitForCompletion(20000);


        //Every 10 minutes clear out recentlyRemovedCallIds that are more than 10 minutes old
        TimerService.get().scheduleAtFixedRate(() -> {
            final long tenMinutesAgo = System.currentTimeMillis() - (10 * 60 * 1000);
            recentlyRemovedCallIds.values().removeIf(time -> time < tenMinutesAgo);
        }, 10, 10, TimeUnit.MINUTES);

    }

    public void close() {
        try {
            //TODO: make const timeout, cancel all calls? Empty map?
            client.unsubscribe(Topics.allServicesIn(serverTopic)).waitForCompletion(5000);
        } catch (MqttException e) {
            log.error("Failed to unsub", e);
        }
    }

    private void sendStatus(String replyTo, String callId, int sequence, Status status) {
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
        publish(replyTo, message);
    }

    public Stats getStats() {
        return new Stats(handlersByCallId.size());
    }

    private void publish(String topic, RpcMessage message) {
        try {
            log.debug("Sending {} {} {} on: {} ",
                    new Object[]{message.getMessageCase(), message.getSequence(), Id.shrt(message.getCallId()), topic});
            client.publish(topic, message.toByteArray(), 1, false);
        } catch (MqttException e) {
            //We can only log the exception here as the broker is broken
            log.error("Failed to publish message to broker", e);
        }

    }

    public void removeCall(String callId) {
        final MgMessageHandler handler = handlersByCallId.get(callId);
        if (handler != null && handler.serverCall != null) {
            handlersByCallId.remove(callId);
            //Only put handlers that take a client stream in the recentlyRemovedCallIds
            //This is because we do not expect any more messages for a service where the client only sends one message
            if (!handler.serverCall.methodDescriptor.getType().clientSendsOneMessage()) {
                recentlyRemovedCallIds.put(callId, System.currentTimeMillis());
            }
        }
        log.debug("Call {} removed for client messages", Id.shrt(callId));
    }


    private class MgMessageHandler implements MessageProcessor.MessageHandler {
        private final String callId;

        private MqttServerCall serverCall;

        /**
         * This will be set when the call is half closed or removed so that it will no longer
         * process client calls.
         */
        private boolean removed = false;


        private final MessageProcessor messageProcessor;

        private MgMessageHandler(String callId, Executor executor, int queueSize) {
            this.callId = callId;
            messageProcessor = new MessageProcessor(executor, queueSize, this);
        }

        public void queueClientMessage(MessageProcessor.MessageWithTopic messageWithTopic) {

            //Check if this is a cancel message.
            //If we queue a cancel message it won't get processed until after the previous message
            //which is what we are trying to cancel so we need to cancel straight away
            //We should only ever receive an error of type CANCELLED because the grpc client
            //implementation converts any error status to CANCELLED anyway.
            //(grpc models CANCELLED as RST_STREAM on the wire but we use a Status because we have high
            //level messaging)
            if (messageWithTopic.message.hasStatus()) {
                if (messageWithTopic.message.getStatus().getCode() == CANCELLED_CODE) {
                    //Don't run the cancel on the mqtt message thread
                    executor.execute(()->{
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
            messageProcessor.queueMessage(messageWithTopic);
        }


        /**
         * onMessage() may be called from multiple threads but only one onMessage will be active at a time.
         * So it is thread safe with respect to itself but cannot use thread locals
         *
         * @param messageWithTopic
         */
        @Override
        public void onBrokerMessage(MessageProcessor.MessageWithTopic messageWithTopic) {

            if (removed) {
                log.debug("Message received for removed handler {}, returning", callId);
                return;
            }
            final RpcMessage message = messageWithTopic.message;
            final String topic = messageWithTopic.topic;

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
                final Header header = message.getStart().getHeader();
                if (header == null) {
                    log.error("Received start message without a header");
                    return;
                }

                //This is the first message for the call so lookup the method and construct an MqttServerCall
                String fullMethodName = topic.substring(topic.lastIndexOf('/', topic.lastIndexOf('/') - 1) + 1);
                //fullMethodName is e.g. "helloworld.ExampleHelloService/SayHello"
                //TODO: Verify that the fullMethodName matches the methoddescriptor in the First
                final ServerMethodDefinition<?, ?> serverMethodDefinition = registry.lookupMethod(fullMethodName);
                if (serverMethodDefinition == null) {
                    sendStatus(header.getReplyTo(), callId, 1,
                            Status.UNIMPLEMENTED.withDescription("No method registered for " + fullMethodName));
                    return;
                }
                final ServerCallHandler<?, ?> serverCallHandler = serverMethodDefinition.getServerCallHandler();

                serverCall = new MqttServerCall<>(client, serverMethodDefinition.getMethodDescriptor(),
                        header, callId);

                serverCall.start(serverCallHandler);

                serverCall.onClientMessage(message);
            } catch (Exception ex) {
                log.error("Error processing MgMessage", ex);
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
            MqttServer.this.removeCall(this.callId);
            this.removed = true;
        }


        /**
         * This represents the call. start() will start the call and call the service method implementation.
         * After start() the ServerCall will have a listener and can send messages to the request/client stream of the method
         * by calling listener.onMessage(). When the method implementation sends a message to the response/server
         * stream of the method it will call ServerCall.sendMessage()
         */
        private class MqttServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {

            final MethodDescriptor<ReqT, RespT> methodDescriptor;
            final MqttAsyncClient client;
            final Header header;
            final String callId;
            int sequence = 0;
            private Listener listener;
            private boolean cancelled = false;
            private ScheduledFuture<?> deadlineCancellationFuture = null;

            private Context.CancellableContext cancellableContext = null;
            private Context context = null;

            MqttServerCall(MqttAsyncClient client, MethodDescriptor<ReqT, RespT> methodDescriptor, Header header, String callId) {
                this.methodDescriptor = methodDescriptor;
                this.client = client;
                this.header = header;
                this.callId = callId;
            }

            public void start(ServerCallHandler<?, ?> serverCallHandler){

                if (header.getTimeoutMillis() > 0) {
                    Deadline deadline = Deadline.after(header.getTimeoutMillis(), TimeUnit.MILLISECONDS);
                    this.deadlineCancellationFuture = DeadlineTimer.start(deadline, (String deadlineMessage) -> {
                        cancel();
                        MgMessageHandler.this.remove();
                    });
                }
                //TODO: Populate the key,value pairs of cancellableContext with e.g. auth credentials from this.header
                //For the moment just put in a dummy key to help with identification/debugging of context
                cancellableContext = Context.ROOT.withCancellation();
                Context.Key<Integer> akey = Context.key("akey");
                context = cancellableContext.withValue(akey, 99);

                //Not that serverCallHandler.startCall() will call the implementation of e.g. sayHello() so
                //all the context etc must be set up so that sayHello can add cancel listeners, get creds,
                //possibly send an error on the responseStream etc.
                //After the implementation of sayHello is called the rest of the interaction is done via
                //the streams that sayHello returns and accepts
                context.run(()->{
                    this.listener = serverCallHandler.startCall(serverCall, EMPTY_METADATA);
                });
            }


            @Override
            public void request(int numMessages) {

            }

            @Override
            public void sendHeaders(Metadata headers) {

            }

            public void onClientMessage(RpcMessage message) {

                Value value;
                switch (message.getMessageCase()) {
                    case START:
                        value = message.getStart().getValue();
                        break;
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

                //TODO: What if this does not match the request type, the parse will not fail - use the methoddescriptor
                Object objValue;
                objValue = methodDescriptor.parseRequest(value.getContents().newInput());

                //Make sure the listener is run in context so that the listener/observer code can get e.g.
                //auth credentials or add a Context.CancellationListener etc.
                //Note its importan to use the run method here which will swap this.context back out of the
                //threadlocal in a finally block. If it didn't then the context and its listeners could remain
                //in the threadlocal indefinitely and not get garbage collected.
                context.run(()->{
                    listener.onMessage(objValue);
                });

                if (message.getSequence() == SINGLE_MESSAGE_STREAM) {
                    //We do not expect the client to send a completed if there is only one message
                    //We do not call remove() here as the call needs to remain available to handle a cancel
                    //It will be removed when close() is called by the listener/service implementation
                    listener.onHalfClose();
                }

            }


            @Override
            public void sendMessage(RespT message) {
                //Send the response up to the client
                //Only increment sequence if there is potentially more than one message in the stream.
                if (!methodDescriptor.getType().serverSendsOneMessage()) {
                    sequence++;
                }
                final ByteString msgBytes = ((MessageLite) message).toByteString();
                Value value = Value.newBuilder()
                        .setContents(msgBytes).build();
                RpcMessage rpcMessage = RpcMessage.newBuilder()
                        .setValue(value)
                        .setCallId(callId)
                        .setSequence(sequence).build();
                publish(header.getReplyTo(), rpcMessage);
            }

            public void cancelTimeouts() {
                if (this.deadlineCancellationFuture != null) {
                    this.deadlineCancellationFuture.cancel(false);
                }
            }

            @Override
            public void close(Status status, Metadata trailers) {
                sequence++;
                sendStatus(header.getReplyTo(), callId, sequence, status);
                cancelTimeouts();
                MgMessageHandler.this.remove();
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
                MgMessageHandler.this.remove();
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
                if(cancellableContext != null){
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
