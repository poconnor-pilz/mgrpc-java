package com.pilz.mqttgrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

public class MqttServer {

    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private final MqttInternalHandlerRegistry registry = new MqttInternalHandlerRegistry();

    //TODO: What kind of thread pool should we use here. It should probably be limited to a fixed maximum
    private final Executor executor = Executors.newCachedThreadPool();

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

    public void init() throws MqttException {
        String allServicesIn = Topics.allServicesIn(serverTopic);
        log.debug("subscribe server at: " + allServicesIn);

        client.subscribe(allServicesIn, 1, new MqttExceptionLogger((String topic, MqttMessage mqttMessage) -> {
            //We use an MqttExceptionLogger here because if a we throw an exception in the subscribe handler
            //it will disconnect the mqtt client
            final MgMessage message = MgMessage.parseFrom(mqttMessage.getPayload());
            log.debug("Received {} message on : {}", message.getType(), topic );
            final String callId = message.getCall();
            if (callId.isEmpty()) {
                log.error("Every message sent from the client must have a callId");
                return;
            }

            MgMessageHandler handler = handlersByCallId.get(callId);
            if (handler == null) {
                handler = new MgMessageHandler(callId);
                handlersByCallId.put(callId, handler);
                final MgMessageHandler fHandler = handler;
                //Each handler gets its own queue and thread to process the client stream
                //The thread will terminate when the client stream is finished
                executor.execute(() -> {
                    fHandler.processQueue();
                });
            }

            //put a message on the queue
            handler.queueMessage(new CallMessage(topic, message));
            log.debug("Put {} message on queue", message.getType());

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
                .setType(MgType.STATUS)
                .setCall(callId)
                .setSequence(sequence)
                .setContents(grpcStatus.toByteString())
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
            log.debug("Sending message Type: {} Sequence: {} Topic:{} ", new Object[]{message.getType(), message.getSequence(), topic});
            client.publish(topic, message.toByteArray(), 1, false);
        } catch (MqttException e) {
            //We can only log the exception here as the broker is broken
            log.error("Failed to publish message to broker");
        }

    }

    class CallMessage {
        boolean endOfQueue = false;
        final String topic;
        final MgMessage message;

        CallMessage(String topic, MgMessage message) {
            this.topic = topic;
            this.message = message;
        }
    }

    class TerminalMessage extends CallMessage{
        TerminalMessage() {
            super(null, null);
            this.endOfQueue = true;
        }
    }


    private class MgMessageHandler {
        private final String callId;

        private MqttServerCall serverCall;

        private static final int QUEUE_BUFFER_SIZE = 100;
        private final BlockingQueue<CallMessage> messageQueue = new ArrayBlockingQueue<>(QUEUE_BUFFER_SIZE);


        private MgMessageHandler(String callId) {
            this.callId = callId;
        }

        public void queueMessage(CallMessage callMessage) {
            //This method may be called by multiple threads
            //Use a queue here because for a particular call the messages in a stream
            //must be handled one by one.
            //Service methods themselves are re-entrant (unless the developer marks that method as synchronized)
            //i.e. there can be more than one call concurrently to a service method but within a call
            //the messages are expected by the call to be delivered one by one to its input stream
            //TODO: messages should be sorted first by sequence in case mqtt sends them in wrong order

            //First check if this is a cancel message.
            //If we queue a cancel message it won't get processed until after the previous message
            //which is what we are trying to cancel so we need to cancel straight away
            if(!callMessage.endOfQueue){
                if(callMessage.message.getType() == MgType.STATUS){
                    boolean statusOk = false;
                    try {
                        com.google.rpc.Status grpcStatus = com.google.rpc.Status.parseFrom(callMessage.message.getContents());
                        statusOk = (grpcStatus.getCode() == Status.OK.getCode().value());
                    } catch (InvalidProtocolBufferException e) {
                        log.debug("Failed to parse status from client");
                    }
                    if(!statusOk){
                        log.debug("Cancel received");
                        if(serverCall != null){
                            serverCall.cancel();
                        }
                        handlersByCallId.remove(callId);
                        //TODO: can we stop the queue more immediately here?
                        callMessage = new TerminalMessage();
                    }
                }
            }
            try {
                messageQueue.put(callMessage);
            } catch (InterruptedException e) {
                log.error("Interrupted while putting message on queue", e);
            }
        }
        public void processQueue(){
            //This method will only be called by a single thread.
            while(true){
                try {
                    CallMessage callMessage = messageQueue.take();
                    if(callMessage.endOfQueue){
                        break;
                    } else {
                        handleMessage(callMessage.topic, callMessage.message);
                    }
                } catch (InterruptedException e) {
                    log.error("Interrupted while processing queue", e);
                    break;
                }
            }
            log.debug("Finished processing queue");
            handlersByCallId.remove(callId);
        }

        public void handleMessage(String topic, MgMessage message) {

            log.debug("Handling {} message", message.getType());
            try {
                if (message.getType() != MgType.START) {
                    if (serverCall == null) {
                        //We never received a valid start messsage for this call.
                        //TODO: ordering. What if the second or third message in a client stream arrives after the start message
                        //Should we just make an exception in that case and fail the call?
                        //It would be better if we had an ordering system that gets called first
                        //It should have a queue of messages per callId. Then it should sort them.
                        //It should time out a queue receives e.g. seq=2, seq=3 etc doesn't receive 1 in a certain time then
                        //the queue is emptied and abandoned (if more messages arrive later for that callId then the same thing
                        //will happen to them.) If it is abandoned then we must send an error to that requestId.
                        //How do we distinguish between a stray callId from some previous connection and this condition?
                        log.error("Unrecognised call id: " + callId);
                        MgMessageHandler.this.queueMessage(new TerminalMessage());
                        return;
                    }
                    serverCall.onClientMessage(message);
                    return;
                }

                //This is the first message we have for this call id so it needs to be a START
                if (message.getType() != MgType.START) {
                    log.error("First message in client stream must be MgType.START: " + message);
                    return;
                }

                final MgHeader header = message.getHeader();
                if (header == null) {
                    log.error("First message in client stream must have a header: " + message);
                    return;
                }

                //This is the first message for the call so lookup the method and construct an MqttServerCall
                String fullMethodName = topic.substring(topic.lastIndexOf('/', topic.lastIndexOf('/') - 1) + 1);
                //fullMethodName is e.g. "helloworld.ExampleHelloService/SayHello"
                //TODO: Verify that the fullMethodName matches the methoddescriptor in the MsgStart
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

                if (message.getType() == MgType.STATUS) {
                    //MgMessageHandler will have already checked for a cancel so this is just an ok end of stream
                    listener.onHalfClose();
                    //Send a dead letter to the queue to stop processing;
                    MgMessageHandler.this.queueMessage(new TerminalMessage());
                } else {
                    //TODO: What if this does not match the request type, the parse will not fail - use the methoddescriptor
                    Object requestParam = methodDescriptor.parseRequest(message.getContents().newInput());
                    listener.onMessage(requestParam);
                    if (message.getSequence() == SINGLE_MESSAGE_STREAM) {
                        //We do not expect the client to send a completed if there is only one message
                        listener.onHalfClose();
                        //Send a dead letter to the queue to stop processing;
                        MgMessageHandler.this.queueMessage(new TerminalMessage());
                    }
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
                MgMessage mgMessage = MgMessage.newBuilder()
                        .setType(MgType.NEXT)
                        .setCall(callId)
                        .setSequence(sequence)
                        .setContents(msgBytes).build();
                publish(replyTo, mgMessage);
            }

            @Override
            public void close(Status status, Metadata trailers) {
                sequence++;
                sendStatus(replyTo, callId, sequence, status);
                //Send a dead letter to the queue to stop processing;
                MgMessageHandler.this.queueMessage(new TerminalMessage());
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
