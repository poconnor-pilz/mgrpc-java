package com.pilz.mqttgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * This is used to cater for brokers that differ from the mqtt spec and do not guarantee ordering or duplicates e.g.
 * https://docs.aws.amazon.com/iot/latest/developerguide/mqtt.html#mqtt-differences
 */
public class MessageProcessor {

    private static Logger log = LoggerFactory.getLogger(MessageProcessor.class);
    //Messages are ordered by sequence
    private final BlockingQueue<MessageWithTopic> messageQueue = new PriorityBlockingQueue<>(1,
            Comparator.comparingInt(o -> o.message.getSequence()));

    private final int queueSize;


    /**
     * List of recent sequence ids, Used for checking for duplicate messages
     */
    private final Recents recents = new Recents();

    private final Executor executor;
    private final MessageHandler messageHandler;

    private boolean queueCapacityExceeded = false;

    private int sequenceOfLastProcessedMessage = -1;

    public MessageProcessor(Executor executor, int queueSize, MessageHandler messageHandler) {
        this.queueSize = queueSize;
        this.executor = executor;
        this.messageHandler = messageHandler;
    }


    public static class MessageWithTopic {
        final String topic;
        final RpcMessage message;

        MessageWithTopic(String topic, RpcMessage message) {
            this.topic = topic;
            this.message = message;
        }

        MessageWithTopic(RpcMessage message) {
            this("", message);
        }

    }

    public interface MessageHandler {
        /**
         * onMessage() may be called from multiple threads but only one onMessage will be active at a time.
         * So it is thread safe with respect to itself but cannot use thread locals
         * @param messageWithTopic
         */
        void onBrokerMessage(MessageWithTopic messageWithTopic);

        /**
         * onQueueCapacityExceeded() is not thread safe and can be called at the same time as an
         * ongoing onMessage() call
         */
        void onQueueCapacityExceeded();
    }


    public void queueMessage(MessageWithTopic messageWithTopic) {
        //It would be simpler here to dedicate a single thread to a call
        //But this would mean that a call with low activity would hog that thread for it's duration
        //With java project loom this would not matter as threads are cheap.
        //So it might be worth doing that when loom becomes available.
        try {
            if(queueCapacityExceeded){
                //Some messages may come in from the broker after the queue is exceeded, ignore them.
                log.warn("Ignoring message after queue exceeded");
                return;
            }
            if ((messageQueue.size() + 1) > queueSize) {
                log.error("Queue capacity ({}) exceeded for call {}",
                        queueSize, messageWithTopic.message.getCallId());
                this.messageHandler.onQueueCapacityExceeded();
                queueCapacityExceeded = true;
                return;
            }
//            log.debug("Queueing {} with sequence {}.", messageWithTopic.message.getMessageCase(),
//                    messageWithTopic.message.getSequence());
            messageQueue.put(messageWithTopic);
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
        MessageWithTopic messageWithTopic = messageQueue.poll();
        while (messageWithTopic != null && !queueCapacityExceeded) {

            RpcMessage message = messageWithTopic.message;
            final int sequence = message.getSequence();
            if (sequence < 0) {
                log.error("Message received with sequence less than zero");
                return;
            }

            if (recents.contains(sequence)) {
                log.warn("{} with sequence {}, is duplicate. Ignoring.", message.getMessageCase(), sequence);
            } else {
                if (outOfOrder(sequence)) {
                    //Put this out-of-order message back on the ordered queue and wait for the in-order message to arrive.
                    try {
                        log.warn("{} with sequence {}, is out of order. Putting back on queue.", message.getMessageCase(), sequence);
                        messageQueue.put(messageWithTopic);
                    } catch (InterruptedException e) {
                        log.error("Interrupted while putting message back on queue", e);
                    }
                    return;
                }
                sequenceOfLastProcessedMessage = sequence;
                //only add to recents if it has not been put back on queue
                recents.add(sequence);

                log.debug("Handling {} with sequence {}", message.getMessageCase(), message.getSequence());
                this.messageHandler.onBrokerMessage(messageWithTopic);
            }

            //get the next message and process it.
            messageWithTopic = messageQueue.poll();
        }
    }

    private boolean outOfOrder(int sequence) {
        if (sequenceOfLastProcessedMessage == -1) {
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
