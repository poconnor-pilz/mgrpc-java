package com.pilz.mqttgrpc;

import com.google.protobuf.MessageLite;
import io.grpc.*;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.*;

public class MqttChannel extends Channel {


    private static Logger log = LoggerFactory.getLogger(MqttChannel.class);
    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 10 * 1000;
    private final static Metadata EMPTY_METADATA = new Metadata();

    private static volatile Executor executorSingleton;
    private static Executor getExecutorInstance(){
        if(executorSingleton == null){
            synchronized (MqttChannel.class){
                if(executorSingleton == null){
                    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum
                    executorSingleton = Executors.newCachedThreadPool();
                }
            }
        }
        return executorSingleton;
    }


    private final MqttAsyncClient client;
    /**
     * The topic prefix of the server e.g. devices/device1
     */
    private final String serverTopic;

    private boolean serverConnected;

    private final Map<String, MqttClientCall> clientCallsById = new ConcurrentHashMap<>();


    private static final int SINGLE_MESSAGE_STREAM = 0;

    private final Executor executor;
    private static final int DEFAULT_QUEUE_SIZE = 100;
    private final int queueSize;

    public MqttChannel(MqttAsyncClient client, String serverTopic,  int queueSize, Executor executor) {
        this.client = client;
        this.serverTopic = serverTopic;
        this.executor = executor;
        this.queueSize = queueSize;
    }

    public MqttChannel(MqttAsyncClient client, String serverTopic, int queueSize) {
        this(client, serverTopic, queueSize, getExecutorInstance());
    }

    public MqttChannel(MqttAsyncClient client, String serverTopic) {
        this(client, serverTopic, DEFAULT_QUEUE_SIZE, getExecutorInstance());
    }


    /**
     * This will cause the MqttGrpcClient to listen for server lwt messages so that it can then
     * send an error to any response observers when the server is disconnected.
     * A client must call init() before using any other methods on the MqttGrpcClient.
     *
     * @throws StatusException
     */
    public void init() throws StatusRuntimeException {
        //TODO: take the code from MqttGrpcClient and refactor it to work here.

        final String replyTo = Topics.allServicesOut(serverTopic);
        log.debug("Subscribing for responses on: " + replyTo);

        final IMqttMessageListener messageListener = new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
            final RpcMessage message = RpcMessage.parseFrom(mqttMessage.getPayload());
            final MqttClientCall call = clientCallsById.get(message.getCallId());
            if (call == null) {
                log.error("Could not find call with callId: " + Id.shrt(message.getCallId()) + " for message " + message.getSequence());
                return;
            }
            call.queueServerMessage(message);
        });

        try {
            //This will throw an exception if it times out.
            client.subscribe(replyTo, 1, messageListener).waitForCompletion(SUBSCRIPTION_TIMEOUT_MILLIS);
        } catch (MqttException e) {
            throw new StatusRuntimeException(Status.UNAVAILABLE.fromThrowable(e));
        }
        serverConnected = true;

    }

    public void close() {
        try {
            //TODO: make const timeout, cancel all calls? Empty map?
            client.unsubscribe(Topics.allServicesIn(serverTopic)).waitForCompletion(5000);
        } catch (MqttException e) {
            log.error("Failed to unsub", e);
        }
    }


    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        MqttClientCall call = new MqttClientCall<>(methodDescriptor, callOptions, executor, queueSize);
        clientCallsById.put(call.getCallId(), call);
        return call;
    }

    @Override
    public String authority() {
        return serverTopic;
    }

    public Stats getStats(){
        return new Stats(clientCallsById.size());
    }

    private class MqttClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> implements MessageProcessor.MessageHandler {

        final MethodDescriptor<ReqT, RespT> methodDescriptor;
        final CallOptions callOptions;
        final Context.CancellableContext context;
        final Executor clientExecutor;
        final String callId;
        int sequence = 0;
        final String replyTo;
        Listener<RespT> responseListener;
        private ScheduledFuture<?> deadlineCancellationFuture;
        private boolean cancelCalled = false;
        private boolean closed = false;
        /**List of recent sequence ids, Used for checking for duplicate messages*/
        private Recents recents = new Recents();
        Deadline effectiveDeadline = null;


        private final ContextCancellationListener cancellationListener =
                new ContextCancellationListener();

        private final MessageProcessor messageProcessor;

        private MqttClientCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions,
                               Executor executor, int queueSize) {
            this.clientExecutor = callOptions.getExecutor();
            this.methodDescriptor = methodDescriptor;
            this.callOptions = callOptions;
            this.context = Context.current().withCancellation();
            //Use a random 10 byte id. It encodes to base32 evenly (16 chars - 5 bits per char). It is valid for topics.
            //It is easier than base64UrlSafe to read in logs and match.
            //The probability of collision for 10,000 concurrent calls is zero (for 100,000 it is about 4E-15)
            this.callId = Id.randomBase32(10);
            this.replyTo = Topics.replyTo(serverTopic, methodDescriptor.getFullMethodName(), callId);
            messageProcessor = new MessageProcessor(executor, queueSize, this);
        }

        public String getCallId() {
            return callId;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            this.responseListener = responseListener;

            if (context.isCancelled()) {
                //Call is already cancelled
                clientExec(()->close(Status.CANCELLED));
                return;
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
                    close(Status.DEADLINE_EXCEEDED.augmentDescription(deadlineMessage));
                });
            }
            //TODO should we call responseListener.onReady() here?
//            exec(() -> responseListener.onReady());
        }

        @Override
        public void sendMessage(ReqT message) {

            //Send message to the server
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId);

            final Value value = Value.newBuilder()
                    .setContents(((MessageLite) message).toByteString()).build();
            if (sequence == 0) {
                //This is the start of the call so make a Start message with a header
                Header.Builder header = Header.newBuilder();
                header.setReplyTo(replyTo);
                if(effectiveDeadline != null){
                    header.setTimeoutMillis(effectiveDeadline.timeRemaining(TimeUnit.MILLISECONDS));
                }
                final Start start = Start.newBuilder()
                        .setHeader(header.build())
                        .setValue(value).build();
                msgBuilder.setStart(start);
            } else {
                msgBuilder.setValue(value);
            }

            //Only increment sequence if there is potentially more than one message in the stream.
            if (!methodDescriptor.getType().clientSendsOneMessage()) {
                sequence++;
            }
            msgBuilder.setSequence(sequence);

            //TODO: If the send fails then should this send an error back to the listener?
            //or will the exception suffice?
            sendToBroker(methodDescriptor.getFullMethodName(), msgBuilder);
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
           this.messageProcessor.queueMessage(new MessageProcessor.MessageWithTopic(message));
        }


        /**
         * onMessage() may be called from multiple threads but only one onMessage will be active at a time.
         * So it is thread safe with respect to itself but cannot use thread locals
         * @param messageWithTopic
         */
        @Override
        public void onBrokerMessage(MessageProcessor.MessageWithTopic messageWithTopic) {

            final RpcMessage message = messageWithTopic.message;

            switch(message.getMessageCase()){
                case STATUS:
                    log.debug("Received completed response");
                    close(toStatus(message.getStatus()));
                    return;
                case VALUE:
                    log.debug("Received message response");
                    clientExec(() -> {
                        responseListener.onHeaders(EMPTY_METADATA);
                        responseListener.onMessage(methodDescriptor.parseResponse(message.getValue().getContents().newInput()));
                        if (message.getSequence() == SINGLE_MESSAGE_STREAM) {
                            //There is only a single message in this stream and it will not be followed by
                            //a completed message so close the stream.
                            close(Status.OK);
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
            this.close(Status.RESOURCE_EXHAUSTED.withDescription("Client queue capacity exceeded."));

            //Send a cancel on to the server. We cannot send it an error on its input stream as it may only expect one message
            //On the server side the listener.onCancel will cause an error to be sent to the server input stream if it has one.
            final com.google.rpc.Status cancelled = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.CANCELLED, null);
            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId)
                    .setSequence(sequence)
                    .setStatus(cancelled);
            sendToBroker(methodDescriptor.getFullMethodName(), msgBuilder);

        }

        public void close(Status status) {
            closed = true;
            log.debug("Closing call with status " + status.getDescription());
            context.removeListener(cancellationListener);
            cancelTimeouts();
            clientExec(() -> responseListener.onClose(status, EMPTY_METADATA));
            clientCallsById.remove(this.callId);
        }

        public void cancelTimeouts(){
            if(this.deadlineCancellationFuture != null){
                this.deadlineCancellationFuture.cancel(false);
            }
        }


        @Override
        public void request(int numMessages) {
            //Do nothing here as we don't implement backpressure.
            //This would be used to send a message to the service to tell it to sent on numMessages
            //But our services will send on messages when they have them (for the moment anyway)
            log.debug("request({})", numMessages);
        }

        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
            //Note that cancel is not called from a synchronized method and so all data
            //accessed here needs to be thread safe.
            if(closed){
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
            close(status);

            final com.google.rpc.Status cancelled = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.CANCELLED, null);
            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId)
                    .setSequence(sequence)
                    .setStatus(cancelled);
            sendToBroker(methodDescriptor.getFullMethodName(), msgBuilder);

        }



        @Override
        public void halfClose() {
            //This will be sent by the client (e.g a stub) when the client stream is complete (or after one unary request)
            if (methodDescriptor.getType().clientSendsOneMessage()) {
                //Don't send a completed as the server doesn't care and it's an extra unnecessary mqtt message
                return;
            }
            final com.google.rpc.Status ok = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(Status.OK, null);
            sequence++;
            final RpcMessage.Builder msgBuilder = RpcMessage.newBuilder()
                    .setCallId(callId)
                    .setSequence(sequence)
                    .setStatus(ok);
            //TODO: If the send fails then should this send an error back to the listener?
            //or will the exception suffice?
            sendToBroker(methodDescriptor.getFullMethodName(), msgBuilder);
        }


        private void sendToBroker(String fullMethodName, RpcMessage.Builder messageBuilder) throws StatusRuntimeException {
            //fullMethodName will be e.g. "helloworld.ExampleHelloService/LotsOfReplies"
            if (!serverConnected) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Server unavailable or init() was not called"));
            }
            try {
                final String topic = Topics.methodIn(serverTopic, fullMethodName);
                final RpcMessage message = messageBuilder.build();
                log.debug("Sending {} {} {} on :{} ",
                        new Object[]{message.getMessageCase(), message.getSequence(), message.getCallId(), topic});
                client.publish(topic, new MqttMessage(message.toByteArray()));
            } catch (MqttException e) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.fromThrowable(e));
            }
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

        //Copied more or less from StatusProto.toStatus() as that is private
        private Status toStatus(com.google.rpc.Status statusProto) {
            Status status = Status.fromCodeValue(statusProto.getCode());
            return status.withDescription(statusProto.getMessage());
        }
    }

    public static class Stats{
        private final int activeCalls;

        public Stats(int activeCalls) {
            this.activeCalls = activeCalls;
        }

        public int getActiveCalls(){
            return activeCalls;
        }
    }



}
