package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;

/**
 * This class is used to re-order out of order messages from the broker
 * and remove duplicates
 */
public class MessageProcessor {

    private static final int UNINITIALISED_SEQUENCE = -1;
    public static final int INTERRUPT_SEQUENCE = -2;

    public static final int TERMINATE_SEQUENCE = -3;

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    //Messages are ordered by sequence
    private final PriorityQueue<RpcMessage> messageQueue = new PriorityQueue<>(3,
            Comparator.comparingInt(o -> o.getSequence()));

    private final int queueSize;

    private int numOutstandingMessages = 0;

    private final Executor executor;
    private final RpcMessageHandler messageHandler;

    /**
     * List of recent sequence ids, Used for checking for duplicate messages
     */
    private final Recents recents = new Recents();

    private final String callId;

    private boolean queueCapacityExceeded = false;

    private int sequenceOfLastProcessedMessage = UNINITIALISED_SEQUENCE;

    public MessageProcessor(String callId, int queueSize, Executor executor, RpcMessageHandler messageHandler) {
        this.queueSize = queueSize;
        this.callId = callId;
        this.executor = executor;
        this.messageHandler = messageHandler;
    }





    public void request(int numMessages){
        processQueue(numMessages, null);
    }

    public void queueMessage(RpcMessage message) {
      processQueue(0, message);
    }

    public synchronized void processQueue(int numMessages, RpcMessage message){


        numOutstandingMessages += numMessages;

        if(message != null) {
            final int sequence = message.getSequence();
            if (sequence < 0) {
                if (sequence != INTERRUPT_SEQUENCE) {
                    log.error("Non-interrupt message received with sequence less than zero");
                    return;
                }
            }
            if (recents.contains(sequence)) {
                log.warn("{} with sequence {}, is duplicate. Ignoring.", message.getMessageCase(), sequence);
            } else {
                messageQueue.add(message);
            }
        }

        if(numOutstandingMessages <= 0){
            return;
        }


        List<RpcMessage> list = new ArrayList(numOutstandingMessages);
        for(int i = 0; i < numOutstandingMessages; i++){
            final RpcMessage msg = messageQueue.poll();
            if(msg == null){
                break;
            }
            if(outOfOrder(msg.getSequence())){
                //Put this out-of-order message back on the ordered queue and wait for the in-order message to arrive.
                messageQueue.add(msg);
                break;
            }
            list.add(msg);
            sequenceOfLastProcessedMessage = msg.getSequence();
        }

        numOutstandingMessages -= list.size();
        this.executor.execute(()->{
            for(int i = 0; i < list.size(); i++)
                messageHandler.onRpcMessage(list.get(i));
            }
        );
    }


    private boolean outOfOrder(int sequence) {
        if (sequence == INTERRUPT_SEQUENCE || sequence == TERMINATE_SEQUENCE) {
            //An interrupt or terminate message should be processed immediately
            return false;
        }
        if (sequenceOfLastProcessedMessage == UNINITIALISED_SEQUENCE) {
            //The first message we receive for a call must have sequence 0 or 1
            if ((sequence != 0) && (sequence != 1)) {
                return true;
            }
        } else {
            //The sequence of each message must be one more than the previous
            if (sequence - sequenceOfLastProcessedMessage != 1) {
                return true;
            }
        }
        return false;
    }

}
