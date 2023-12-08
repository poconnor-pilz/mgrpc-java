package io.mgrpc.jms;

import javax.jms.*;

public class JmsCallQueues {

    public Queue producerQueue;
    public MessageProducer producer;
    public Queue consumerQueue;
    public MessageConsumer consumer;

    public void close(Session session) throws JMSException {
        if(producer != null){
            producer.close();
        }
        if(consumer != null){
            consumer.close();
        }
        //TODO: JMS doesn't allow deleting queues. Find out how to delete the queue using artemis api.
        //it may be the case that it is released when all producers, consumers are closed.
    }
}
