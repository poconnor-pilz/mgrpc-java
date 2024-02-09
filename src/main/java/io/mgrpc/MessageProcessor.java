package io.mgrpc;

import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
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

    private int outStandingRequests = 0;

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

    public void queueSet(RpcSet rpcSet) {
        processQueue(0, rpcSet);
    }

    public synchronized void processQueue(int requestedMessages, RpcSet rpcSet){


//        log.debug("ProcessQueue requestedMessages = " + requestedMessages + " message = " + (rpcSet == null ? 0 : 1));
        outStandingRequests += requestedMessages;

        if(rpcSet != null) {
            for(RpcMessage message: rpcSet.getMessagesList()) {
                final int sequence = message.getSequence();
                if (sequence < 0) {
                    if (sequence != INTERRUPT_SEQUENCE) {
                        log.error("Non-interrupt message received with sequence less than zero for call " + callId);
                        return;
                    }
                }
                if (recents.contains(sequence)) {
                    log.warn("{} with sequence {} for call {}, is duplicate. Ignoring.", new Object[]{message.getMessageCase(), sequence, callId});
                } else {
                    messageQueue.add(message);
                }
            }
        }

        if(outStandingRequests <= 0){
            return;
        }


        List<RpcMessage> list = new ArrayList(outStandingRequests);
        for(int i = 0; i < outStandingRequests; i++){
            final RpcMessage msg = messageQueue.poll();
            if(msg == null){
                break;
            }
            if(outOfOrder(msg.getSequence())){
                //Put this out-of-order message back on the ordered queue and wait for the in-order message to arrive.
                log.warn("Message for call " + callId + " is out of order sequence = " + msg.getSequence());
                messageQueue.add(msg);
                break;
            }
            list.add(msg);
            sequenceOfLastProcessedMessage = msg.getSequence();
        }

        if(list.size() == 0){
            return;
        }

        outStandingRequests -= list.size();
        if(outStandingRequests > 0){
            //This condition could occur if some implementation does manual flow control and requests more
            //more messages than are available in the queue right now. If this is the case and we proceed normally then
            //another message could come through quickly and be handled in the executor at the end of this method.
            //There is no guarantee that that executor would be scheduled in order.
            //i.e. In we do not want to send messages to a call that has not just requested them in order to make
            //sure that requests are always handled serially.
            final String err = "Requested messages exceed available messages. This may be caused by manual flow control requesting more messages than are available in the queue";
            log.error(err);
            this.executor.execute(()-> {
                final Status status = Status.ABORTED.withDescription(err);
                final com.google.rpc.Status grpcStatus = StatusProto.fromStatusAndTrailers(status, null);
                messageHandler.onRpcMessage(RpcMessage.newBuilder().setCallId(callId).setStatus(grpcStatus).build());
            });
            return;
        }
        this.executor.execute(()-> {
            for (int i = 0; i < list.size(); i++) {
                final RpcMessage rpcMessage = list.get(i);
                messageHandler.onRpcMessage(list.get(i));
            }
        });
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
