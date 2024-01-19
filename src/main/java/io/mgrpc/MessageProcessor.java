package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

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
    private final BlockingQueue<RpcMessage> messageQueue = new PriorityBlockingQueue<>(1,
            Comparator.comparingInt(o -> o.getSequence()));

    private final int queueSize;

    /**
     * List of recent sequence ids, Used for checking for duplicate messages
     */
    private final Recents recents = new Recents();

    private final String callId;

    private boolean queueCapacityExceeded = false;

    private int sequenceOfLastProcessedMessage = UNINITIALISED_SEQUENCE;

    public MessageProcessor(String callId, int queueSize) {
        this.queueSize = queueSize;
        this.callId = callId;
    }


    public interface MessageHandler {
        /**
         * onProcessorMessage() may be called from multiple threads but only one onProviderMessage will be active at a time.
         * So it is thread safe with respect to itself but cannot use thread locals
         *
         * @param message
         */
        void onProcessorMessage(RpcMessage message);

        /**
         * onQueueCapacityExceeded() is not thread safe and can be called at the same time as an
         * ongoing onMessage() call
         */
        void onProcessorQueueCapacityExceeded(String callId);
    }

    public void close() {
        try {
            messageQueue.put(RpcMessage.newBuilder().setSequence(TERMINATE_SEQUENCE).build());
        } catch (InterruptedException e) {
            log.error("Interrupted while putting termination message on queue", e);
        }
    }

    public void queueMessage(RpcMessage message) {
        try {
            if (queueCapacityExceeded) {
                //Some messages may come in from the broker after the queue is exceeded, ignore them.
                log.warn("Ignoring message after queue exceeded");
                return;
            }
            if ((messageQueue.size() + 1) > queueSize) {
                log.error("Queue capacity ({}) exceeded for call {}",
                        queueSize, message.getCallId());
                queueCapacityExceeded = true;
                return;
            }
//            log.debug("Queueing {} with sequence {}.", messageWithTopic.message.getMessageCase(),
//                    messageWithTopic.message.getSequence());
            messageQueue.put(message);
        } catch (InterruptedException e) {
            log.error("Interrupted while putting message on queue", e);
        }
    }

    public RpcMessage getNextMessage() {

        while (!queueCapacityExceeded) {
            RpcMessage message;
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                log.error("Interrupted while processing queue", e);
                return null;
            }

            final int sequence = message.getSequence();

            if (sequence == TERMINATE_SEQUENCE) {
                log.debug("processed terminate message");
                return null;
            }

            if (sequence < 0) {
                if (sequence != INTERRUPT_SEQUENCE) {
                    log.error("Non-interrupt message received with sequence less than zero");
                    return null;
                }
            }
            if (recents.contains(sequence)) {
                log.warn("{} with sequence {}, is duplicate. Ignoring.", message.getMessageCase(), sequence);
            } else {
                if (outOfOrder(sequence)) {
                    //Put this out-of-order message back on the ordered queue and wait for the in-order message to arrive.
                    try {
                        log.warn("{} with sequence {}, is out of order. Putting back on queue.", message.getMessageCase(), sequence);
                        messageQueue.put(message);
                        //Give it some time for the right message to arrive and be ordered by the queue
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        log.error("Interrupted while putting message back on queue", e);
                        return null;
                    }
                } else {
                    if (sequence != INTERRUPT_SEQUENCE) {
                        sequenceOfLastProcessedMessage = sequence;
                        //only add to recents if it has not been put back on queue
                        recents.add(sequence);
                    }
                    return message;
                }
            }
        }
        return null;
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
