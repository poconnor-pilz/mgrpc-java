package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class CallSequencer {

    private final Map<String, MessageProcessor> processors = new ConcurrentHashMap();

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int DEFAULT_QUEUE_SIZE = 100;

    private final Executor executor;
    private final RpcMessageHandler messageHandler;


    public CallSequencer(Executor executor, RpcMessageHandler messageHandler) {
        this.executor = executor;
        this.messageHandler = messageHandler;
    }

    /**
     * Make a queue for a call if it doesn't exist already.
     * @param callId
     */
    public void makeQueue(String callId){
        MessageProcessor processor = processors.get(callId);
        if(processor == null){
            processor = new MessageProcessor(callId, DEFAULT_QUEUE_SIZE, executor, messageHandler);
            processors.put(callId, processor);
        }

    }
    public void queueMessage(RpcMessage message){
        MessageProcessor processor = processors.get(message.getCallId());
        if(processor != null) {
            processor.queueMessage(message);
        } else {
            String err = "Message received for call without queue. This may be a stray message for a call that is already terminated. callId = " + message.getCallId();
            log.error(err);
        }
    }


    public void request(String callId, int numMessages){
        final MessageProcessor processor = processors.get(callId);
        if(processor == null){
            log.error("Failed to find processor for call " + callId);
            return;
        }
        processor.request(numMessages);
    }

    public void onCallClosed(String callId) {
        final MessageProcessor processor = processors.remove(callId);
    }


}
