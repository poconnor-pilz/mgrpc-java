package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.*;

import static com.pilz.mqttgrpc.MgMessage.MessageCase.START;
import static com.pilz.mqttgrpc.MgMessage.MessageCase.STATUS;


public class MqttServer {

    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private MqttInternalHandlerRegistry registry = new MqttInternalHandlerRegistry();

    private static volatile Executor executor;
    private static Executor getExecutorInstance(){
        if(executor == null){
            synchronized (MqttServer.class){
                if(executor== null){
                    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum
                    executor = Executors.newCachedThreadPool();
                }
            }
        }
        return executor;
    }




    private final MqttAsyncClient client;

    private final Map<String, MgMessageHandler> handlersByCallId = new ConcurrentHashMap<>();

    private final String serverTopic;

    private static final int SINGLE_MESSAGE_STREAM = 0;

    public MqttServer(MqttAsyncClient client, String serverTopic) {
        this.client = client;
        this.serverTopic = serverTopic;
    }

    public void addService(BindableService service) {
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
            final MgMessage message = MgMessage.parseFrom(mqttMessage.getPayload());
            log.debug("Received {} message on : {}", message.getMessageCase(), topic);
            final String callId = message.getCallId();
            if (callId.isEmpty()) {
                log.error("Every message sent from the client must have a callId");
                return;
            }

            MgMessageHandler handler = handlersByCallId.get(callId);
            if (handler == null) {
                handler = new MgMessageHandler(callId, getExecutorInstance());
                handlersByCallId.put(callId, handler);
            }

            //put the message on the handler's queue
            handler.queueClientMessage(new CallMessage(topic, message));
            log.debug("Put {} message on queue", message.getMessageCase());

            //TODO: Check this for leaks. How can we be sure everything is gc'd

        })).waitForCompletion(20000);

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
        MgMessage message = MgMessage.newBuilder()
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

    private void publish(String topic, MgMessage message) {
        try {
            log.debug("Sending message Type: {} Sequence: {} Topic:{} ", new Object[]{message.getMessageCase(), message.getSequence(), topic});
            client.publish(topic, message.toByteArray(), 1, false);
        } catch (MqttException e) {
            //We can only log the exception here as the broker is broken
            log.error("Failed to publish message to broker", e);
        }

    }

    class CallMessage {
        final String topic;
        final MgMessage message;

        CallMessage(String topic, MgMessage message) {
            this.topic = topic;
            this.message = message;
        }
    }


    private class MgMessageHandler {
        private final String callId;

        private MqttServerCall serverCall;

        /**
         * This will be set when the call is half closed or removed so that it will no longer
         * process client calls.
         */
        private boolean removed = false;

        private final Executor executor;

        private int sequenceOfLastProcessedMessage = -1;

        private static final int MAX_QUEUED_MESSAGES = 100;

        /**List of recent sequence ids, Used for checking for duplicate messages*/
        private Recents recents = new Recents();
        //Messages are ordered by sequence
        private final BlockingQueue<CallMessage> messageQueue = new PriorityBlockingQueue<>(11,
                Comparator.comparingInt(o -> o.message.getSequence()));


        private MgMessageHandler(String callId, Executor executor) {
            this.callId = callId;
            this.executor = executor;
        }

        public void queueClientMessage(CallMessage callMessage) {

            //First check if this is a cancel message.
            //If we queue a cancel message it won't get processed until after the previous message
            //which is what we are trying to cancel so we need to cancel straight away
            if (callMessage.message.getMessageCase() == STATUS) {
                boolean statusOk = false;
                com.google.rpc.Status grpcStatus = callMessage.message.getStatus();
                statusOk = (grpcStatus.getCode() == Status.OK.getCode().value());
                if (!statusOk) {
                    log.debug("Cancel or error received. Will cancel immediately");
                    //If the call was constructed, cancel it.
                    if (serverCall != null) {
                        serverCall.cancel();
                    }
                    this.remove();
                }
            }
            try {
                messageQueue.put(callMessage);
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
            CallMessage callMessage = messageQueue.poll();
            while (!removed && (callMessage != null)) {
                handleClientMessage(callMessage.topic, callMessage.message);
                callMessage = messageQueue.poll();
            }
        }

        public void handleClientMessage(String topic, MgMessage message) {

            log.debug("Handling {} message", message.getMessageCase());
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
                queueClientMessage(new CallMessage(topic, message));
                return;
            }
            sequenceOfLastProcessedMessage = sequence;


            try {
                if (message.getMessageCase() != START) {
                    if (serverCall == null) {
                        //We never received a valid first message for this call.
                        //TODO: ordering. What if the second or third message in a client stream arrives after the first message
                        //Should we just make an exception in that case and fail the call?
                        //It would be better if we had an ordering system that gets called first
                        //It should have a queue of messages per callId. Then it should sort them.
                        //It should time out a queue receives e.g. seq=2, seq=3 etc doesn't receive 1 in a certain time then
                        //the queue is emptied and abandoned (if more messages arrive later for that callId then the same thing
                        //will happen to them.) If it is abandoned then we must send an error to that requestId.
                        //How do we distinguish between a stray callId from some previous connection and this condition?
                        log.error("Unrecognised call id: " + callId);
                        this.remove();
                        return;
                    }
                    serverCall.onClientMessage(message);
                    return;
                }

                //This is the first message we have for this call id so it needs to be a FIRST
                if (message.getMessageCase() != START) {
                    log.error("First message in client stream must be FIRST: " + message);
                    return;
                }

                final Header header = message.getStart().getHeader();
                if(header == null){
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
                        header.getReplyTo(), callId);
                final ServerCall.Listener listener = serverCallHandler.startCall(serverCall, new Metadata());
                serverCall.setListener(listener);
                serverCall.onClientMessage(message);
            } catch (Exception ex) {
                log.error("Error processing MgMessage", ex);
            }

        }

        public void remove() {
            MqttServer.this.handlersByCallId.remove(this.callId);
            this.removed = true;
            log.debug("Call {} removed for client messages", callId);
        }

        private class MqttServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {

            final MethodDescriptor<ReqT, RespT> methodDescriptor;
            final MqttAsyncClient client;
            final String replyTo;
            final String callId;
            int sequence = 0;
            private Listener listener;
            private boolean cancelled = false;


            MqttServerCall(MqttAsyncClient client, MethodDescriptor<ReqT, RespT> methodDescriptor, String replyTo, String callId) {
                this.methodDescriptor = methodDescriptor;
                this.client = client;
                this.replyTo = replyTo;
                this.callId = callId;
            }

            public void setListener(Listener listener) {
                this.listener = listener;
            }

            public Listener getListener() {
                return this.listener;
            }

            @Override
            public void request(int numMessages) {

            }

            @Override
            public void sendHeaders(Metadata headers) {

            }

            public void onClientMessage(MgMessage message) {

                Value value;
                switch(message.getMessageCase()){
                    case START:
                        value = message.getStart().getValue();
                        break;
                    case VALUE:
                        value = message.getValue();
                        break;
                    case STATUS:
                        //MgMessageHandler will have already checked for a cancel so this is just an ok end of stream
                        listener.onHalfClose();
                        MgMessageHandler.this.remove();
                        return;
                    default:
                        log.error("Unrecognised message case " + message.getMessageCase());
                        return;
                }

                //TODO: What if this does not match the request type, the parse will not fail - use the methoddescriptor
                Object objValue;
                objValue = methodDescriptor.parseRequest(value.getContents().newInput());

                listener.onMessage(objValue);
                if (message.getSequence() == SINGLE_MESSAGE_STREAM) {
                    //We do not expect the client to send a completed if there is only one message
                    listener.onHalfClose();
                    MgMessageHandler.this.remove();
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
                MgMessage mgMessage = MgMessage.newBuilder()
                        .setValue(value)
                        .setCallId(callId)
                        .setSequence(sequence).build();
                publish(replyTo, mgMessage);
            }

            @Override
            public void close(Status status, Metadata trailers) {
                sequence++;
                sendStatus(replyTo, callId, sequence, status);
                MgMessageHandler.this.remove();
            }


            public void cancel() {
                log.debug("server call cancelled");
                if (cancelled) {
                    return;
                }
                this.cancelled = true;
                this.getListener().onCancel();
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


}
