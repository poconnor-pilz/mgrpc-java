package com.pilz.mqttgrpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;

public class MqttChannel extends Channel {


    private static Logger log = LoggerFactory.getLogger(MqttChannel.class);
    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 10 * 1000;
    private final static Metadata EMPTY_METADATA = new Metadata();

    private static volatile Executor executor;
    private static Executor getExecutorInstance(){
        if(executor == null){
            synchronized (MqttChannel.class){
                if(executor== null){
                    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum
                    executor = Executors.newCachedThreadPool();
                }
            }
        }
        return executor;
    }


    private final MqttAsyncClient client;
    /**
     * The topic prefix of the server e.g. devices/device1
     */
    private final String serverTopic;

    private boolean serverConnected;

    private final Map<String, MqttClientCall> clientCallsById = new ConcurrentHashMap<>();


    private static final int SINGLE_MESSAGE_STREAM = 0;


    public MqttChannel(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopic = serverTopic;
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
            final MgMessage message = MgMessage.parseFrom(mqttMessage.getPayload());
            final MqttClientCall call = clientCallsById.get(message.getCall());
            if (call == null) {
                log.error("Could not find call with callId: " + message.getCall());
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
        MqttClientCall call = new MqttClientCall<>(methodDescriptor, callOptions, getExecutorInstance());
        clientCallsById.put(call.getCallId(), call);
        return call;
    }

    @Override
    public String authority() {
        return serverTopic;
    }


    private class MqttClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        final MethodDescriptor<ReqT, RespT> methodDescriptor;
        final CallOptions callOptions;
        final Context context;
        final Executor clientExecutor;
        final Executor executor;
        final String callId;
        int sequence = 0;
        final String replyTo;
        Listener<RespT> responseListener;
        private ScheduledFuture<?> deadlineCancellationFuture;
        private boolean cancelCalled = false;
        private boolean closed = false;
        /**List of recent sequence ids, Used for checking for duplicate messages*/
        private Recents recents = new Recents();


        private static final int QUEUE_BUFFER_SIZE = 100;
        private final BlockingQueue<MgMessage> messageQueue = new ArrayBlockingQueue<>(QUEUE_BUFFER_SIZE);


        private final ContextCancellationListener cancellationListener =
                new ContextCancellationListener();
        private int sequenceOfLastProcessedMessage = -1;


        private MqttClientCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Executor executor) {
            this.clientExecutor = callOptions.getExecutor();
            this.methodDescriptor = methodDescriptor;
            this.callOptions = callOptions;
            this.executor = executor;
            this.context = Context.current();
            this.callId = Base64Uuid.id();
            this.replyTo = Topics.replyTo(serverTopic, methodDescriptor.getServiceName(),
                    methodDescriptor.getBareMethodName(), callId);
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
            final Deadline effectiveDeadline = DeadlineTimer.min(callOptions.getDeadline(), context.getDeadline());
            if (effectiveDeadline != null) {
                this.deadlineCancellationFuture = DeadlineTimer.start(effectiveDeadline, (String deadlineMessage) -> {
                    //TODO: We do not need to send a cancellation message to the server here but
                    //we do need to send on the deadline to the server in the first request so
                    //that it can set its own deadline timer

                    //We close the call here which will call listener.onClose(Status.DEADLINE_EXCEEDED)
                    //because this is what the listener expects.
                    //The listener will then cancel on this call.
                    close(Status.DEADLINE_EXCEEDED.augmentDescription(deadlineMessage));
                });
            }
            //TODO should we call responseListener.onReady() here?
//            exec(() -> responseListener.onReady());
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


        public void queueServerMessage(MgMessage message) {
            try {
                messageQueue.put(message);
                //Process queue on thread pool
                this.executor.execute(() -> processQueue());
            } catch (InterruptedException e) {
                log.error("Interrupted while putting message on queue", e);
            }
        }

        public synchronized void processQueue() {
            //This method will be called by multiple threads but it is synchronized so that the
            //service method call will only process one message in a stream at a time i.e. the
            //service method *call* behaves like an actor. However, the service method itself may have
            //many calls ongoing concurrently (unless the service developer synchronizes it).
            MgMessage message = messageQueue.poll();
            while (!cancelCalled && (message != null)) {
                handleServerMessage(message);
                message = messageQueue.poll();
            }
        }


        public void handleServerMessage(MgMessage message){

            final int sequence = message.getSequence();
            if(sequence < 0){
                log.error("Message received with sequence less than zero");
                return;
            }
            //Check to see if a message with this sequence has recently been processed
            //If so then this message is a duplicate sent by the broker so ignore it
            if(recents.contains(sequence)){
                log.warn("Duplicate message received, ignoring" + message);
                return;
            }
            recents.add(sequence);

            //Check for messages that are out of sequence.
            boolean sequential = true;
            if(sequenceOfLastProcessedMessage == -1){
                if( (sequence!=0) && (sequence!=1) ){
                    sequential = false;
                }
            } else {
                if(sequence - sequenceOfLastProcessedMessage != 1){
                    sequential = false;
                }
            }
            if(!sequential){
                //Put this message back on the queue and wait for the sequential message
                //TODO: Set a "cancellation" timer here that closes the call and sends an error to the client stream if no sequential message arrives
                queueServerMessage(message);
                return;
            }
            sequenceOfLastProcessedMessage = sequence;

            if (message.getType() == MgType.STATUS) {
                log.debug("Received completed response");
                final com.google.rpc.Status grpcStatus;
                try {
                    grpcStatus = com.google.rpc.Status.parseFrom(message.getContents());
                    close(toStatus(grpcStatus));
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Failed to parse status from server");
                    close(Status.UNKNOWN.withDescription("Failed to parse status from server"));
                }
            } else {
                log.debug("Received message response");
                clientExec(() -> {
                    responseListener.onHeaders(EMPTY_METADATA);
                    responseListener.onMessage(methodDescriptor.parseResponse(message.getContents().newInput()));
                    if (message.getSequence() == SINGLE_MESSAGE_STREAM) {
                        //There is only a single message in this stream and it will not be followed by
                        //a completed message so close the stream.
                        close(Status.OK);
                    }
                });
            }
        }

        public void close(Status status) {
            if(closed){
                //
                return;
            }
            closed = true;
            log.debug("Closing call with status " + status.getDescription());
            context.removeListener(cancellationListener);
            ScheduledFuture<?> f = deadlineCancellationFuture;
            if (f != null) {
                f.cancel(false);
            }
            clientExec(() -> responseListener.onClose(status, EMPTY_METADATA));
            clientCallsById.remove(this.callId);
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
            if(closed){
                //In the case of a deadline timeout we will call close on the listener and on this
                //The listener will then call close on this with Status.CANCELLED
                //In this case we do nothing. We don't want to send the cancel on to the server
                //because the server will have its own timeout handler.
                return;
            }
            log.debug("Call cancelled");
            if (cancelCalled) {
                return;
            }
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
            final MgMessage.Builder msgBuilder = MgMessage.newBuilder()
                    .setCall(callId)
                    .setSequence(sequence)
                    .setType(MgType.STATUS)
                    .setContents(cancelled.toByteString());
            sendToBroker(methodDescriptor.getServiceName(), methodDescriptor.getBareMethodName(), msgBuilder);

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
            final MgMessage.Builder msgBuilder = MgMessage.newBuilder()
                    .setCall(callId)
                    .setSequence(sequence)
                    .setType(MgType.STATUS)
                    .setContents(ok.toByteString());
            //TODO: If the send fails then should this send an error back to the listener?
            //or will the exception suffice?
            sendToBroker(methodDescriptor.getServiceName(), methodDescriptor.getBareMethodName(), msgBuilder);
        }

        @Override
        public void sendMessage(ReqT message) {

            //Send message to the server
            final MgMessage.Builder msgBuilder = MgMessage.newBuilder()
                    .setCall(callId);

            if (sequence == 0) {
                //This is the first message in the stream so it needs to be a START with a header
                msgBuilder.setType(MgType.START);
                final MgHeader header = MgHeader.newBuilder()
                        .setReplyTo(replyTo).build();
                msgBuilder.setHeader(header);
            } else {
                msgBuilder.setType(MgType.NEXT);
            }

            msgBuilder.setContents(((MessageLite) message).toByteString()).build();
            //Only increment sequence if there is potentially more than one message in the stream.
            if (!methodDescriptor.getType().clientSendsOneMessage()) {
                sequence++;
            }
            msgBuilder.setSequence(sequence);

            //TODO: If the send fails then should this send an error back to the listener?
            //or will the exception suffice?
            sendToBroker(methodDescriptor.getServiceName(), methodDescriptor.getBareMethodName(), msgBuilder);
            responseListener.onReady();
        }

        private void sendToBroker(String serviceName, String method, MgMessage.Builder messageBuilder) throws StatusRuntimeException {
            if (!serverConnected) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Server unavailable or init() was not called"));
            }
            try {
                final String topic = Topics.methodIn(serverTopic, serviceName, method);
                final MgMessage message = messageBuilder.build();
                log.debug("Sending message type: {} sequence: {} call: {} topic:{} ",
                        new Object[]{message.getType(), message.getSequence(), message.getCall(), topic});
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


}
