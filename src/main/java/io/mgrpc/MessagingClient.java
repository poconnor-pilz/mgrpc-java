package io.mgrpc;

/**
 * Interface to messagin client. Adapters should implement this to work with different message protocols.
 */
public interface MessagingClient {


    /**
     * Return the topic separator for the messaging system. For mqtt this would be "/"
     */
    String topicSeparator();

    /**
     * Publish a message to the messaging system. This call is expected to be non-blocking
     * @param topic The topic to which to send the message
     * @param buffer The message contents
     * @throws MessagingException Throw an exception if the publish fails
     */
    void publish(String topic, byte[] buffer) throws MessagingException;

    /**
     * Subscribe to a topic for messages. This call should block until the subscription suceeds
     * and it should throw an exception on failure or timeout.
     */
    void subscribe(String topic, MessagingListener listener) throws MessagingException;

    /**
     * Subscribe for all topics beginning with topicPrefix e.g. for mqtt if this method is called
     * with myTopic/subTopic then the implementation would be expected to wildcard subscribe for
     * myTopic/subTopic/#
     */
    void subscribeAll(String topicPrefix, MessagingListener listener) throws MessagingException;

    void unsubscribe(String topic) throws MessagingException;

    /**
     * Unubscribe for all topics beginning with topicPrefix e.g. for mqtt if this method is called
     * with myTopic/subTopic then the implementation would be expected to wildcard unsubscribe for
     * myTopic/subTopic/#
     */
    void unsubscribeAll(String topicPrefix) throws MessagingException;
}
