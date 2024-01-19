package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CallSequencer {

    private final Map<String, MessageProcessor> processors = new ConcurrentHashMap();

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int DEFAULT_QUEUE_SIZE = 100;

    public CallSequencer() {
    }

    /**
     * Make a queue for a call if it doesn't exist already
     * @param callId
     */
    public void makeQueue(String callId){
        MessageProcessor processor = processors.get(callId);
        if(processor == null){
            processor = new MessageProcessor(callId, DEFAULT_QUEUE_SIZE);
            processors.put(callId, processor);
        }

    }
    public void queueMessage(RpcMessage message){
        MessageProcessor processor = processors.get(message.getCallId());
        if(processor != null) {
            processor.queueMessage(message);
        } else {
            String err = "No queue for call " + message.getCallId();
            log.error(err);
            throw new RuntimeException(err);
        }
    }

    /**
     * Block until a message is available that is in sequence
     * @param callId
     * @return an in sequence message.
     */
    public RpcMessage getNextMessage(String callId){
        final MessageProcessor processor = processors.get(callId);
        if(processor == null){
            log.error("Failed to find processor for call " + callId);
            return null;
        }
        return processor.getNextMessage();
    }
    public void onCallClosed(String callId) {
        final MessageProcessor processor = processors.remove(callId);
        if(processor != null){
            processor.close();
        }
    }


}
