package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;


/**
 * This class is used to re-order out of order messages from the broker,
 * remove duplicates and then call a MessageHandler.onMessage from a thread pool
 * such that only one instance of onMessage for a particular call is ongoing at a time.
 * i.e. MessageHandler.onMessage does not have to be thread safe but the same thread is not always
 * used to call it (so it can't use thread locals)
 * This is used to cater for messging systems that do not guarantee ordering or duplicates e.g.
 * https://docs.aws.amazon.com/iot/latest/developerguide/mqtt.html#mqtt-differences
 *
 */
public class MsgProcessor {

    private static final int UNINITIALISED_SEQUENCE = -1;
    public static final int INTERRUPT_SEQUENCE = -2;

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    //Messages are ordered by sequence
    private final BlockingQueue<RpcMessage> messageQueue = new PriorityBlockingQueue<>(1,
            Comparator.comparingInt(o -> o.getSequence()));

    private final int queueSize;


    /**
     * List of recent sequence ids, Used for checking for duplicate messages
     */
    private final Recents recents = new Recents();

    private final Executor executor;
    private final MessageHandler messageHandler;

    private boolean queueCapacityExceeded = false;

    private int sequenceOfLastProcessedMessage = UNINITIALISED_SEQUENCE;

    public MsgProcessor(Executor executor, int queueSize, MessageHandler messageHandler) {
        this.queueSize = queueSize;
        this.executor = executor;
        this.messageHandler = messageHandler;
    }


    public interface MessageHandler {
        /**
         * onMessage() may be called from multiple threads but only one onMessage will be active at a time.
         * So it is thread safe with respect to itself but cannot use thread locals
         * @param message
         */
        void onProviderMessage(RpcMessage message);

        /**
         * onQueueCapacityExceeded() is not thread safe and can be called at the same time as an
         * ongoing onMessage() call
         */
        void onQueueCapacityExceeded();
    }


    public void queueMessage(RpcMessage message) {
        //It would be simpler here to dedicate a single thread to a call
        //But this would mean that a call with low activity would hog that thread for its duration
        //With java project loom this would not matter as threads are cheap.
        //So it might be worth doing that when loom becomes available.

        //(Note that this is more like the actor model than classic gRPC because we are using a queue instead
        // of flow control. Although we may implement flow control later)
        //"In Akka, actors are purely reactive components: an actor is passive until a message is sent to it.
        //When a message arrives to the actor's mailbox one thread is allocated to the actor,
        //the message is extracted from the mailbox and the actor applies the behavior.
        //When the processing is done, the thread is returned to the pool.
        //This way actors don't occupy any CPU resources when they are inactive."
        try {
            if(queueCapacityExceeded){
                //Some messages may come in from the broker after the queue is exceeded, ignore them.
                log.warn("Ignoring message after queue exceeded");
                return;
            }
            if ((messageQueue.size() + 1) > queueSize) {
                log.error("Queue capacity ({}) exceeded for call {}",
                        queueSize, Id.shrt(message.getCallId()));
                this.messageHandler.onQueueCapacityExceeded();
                queueCapacityExceeded = true;
                return;
            }
//            log.debug("Queueing {} with sequence {}.", messageWithTopic.message.getMessageCase(),
//                    messageWithTopic.message.getSequence());
            messageQueue.put(message);
            //Process queue on thread pool
            this.executor.execute(() -> processQueue());
        } catch (InterruptedException e) {
            log.error("Interrupted while putting message on queue", e);
        }
    }

    private synchronized void processQueue() {
        //This method will be called by multiple threads but it is synchronized so that the
        //service method call will only process one message in a stream at a time i.e. the
        //service method *call* behaves like an actor. However, the service method itself may have
        //many calls ongoing concurrently (unless the service developer synchronizes it).
        RpcMessage message  = messageQueue.poll();
        while (message != null && !queueCapacityExceeded) {

            final int sequence = message.getSequence();
            if (sequence < 0) {
                if(sequence != INTERRUPT_SEQUENCE) {
                    log.error("Non-interrupt message received with sequence less than zero");
                    return;
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
                    } catch (InterruptedException e) {
                        log.error("Interrupted while putting message back on queue", e);
                    }
                    return;
                }
                if(sequence != INTERRUPT_SEQUENCE) {
                    sequenceOfLastProcessedMessage = sequence;
                    //only add to recents if it has not been put back on queue
                    recents.add(sequence);
                }

                log.debug("Handling {} {} {}", new Object[]{message.getMessageCase(), message.getSequence(),
                        Id.shrt(message.getCallId())});
                try {
                    this.messageHandler.onProviderMessage(message);
                } catch (Exception ex){
                    log.error("Exception processing message in thread: " + Thread.currentThread().getName(), ex);
                }
            }

            //get the next message and process it.
            message = messageQueue.poll();
        }
    }

    private boolean outOfOrder(int sequence) {
        if(sequence == INTERRUPT_SEQUENCE){
            //An interrupt message should be processed immediately
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
