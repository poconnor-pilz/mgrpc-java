package io.mgrpc;

public class ServerTopics {

    public static final String IN = "i";
    public static final String OUT = "o";
    public static final String SVC = "svc";
    public static final String SYS = "sys";
    public static final String STATUS = "status";
    public static final String PROMPT = "prompt";
    public static final String CLIENT = "client";


    /**
     * Root topic for server e.g. "device1"
     */
    public final String root;
    public final String sep;
    /**
     * Server status will be reported at this topic and LWT should also be sent here.
     * Has the form {root}/o/sys/status
     * The server will send a connected=true message to this when it starts up or if it when a client sends
     * it a message on statusPrompt
     */
    public final String status;
    /**
     * If a client sends any message to this topic then the server will send status to the status topic
     * Has the form {root}/i/sys/status/prompt
     */
    public final String statusPrompt;
    /**
     * Clients should send status message to this topic postfixed with channelId
     * Has the form {root}/i/sys/status/client
     */
    public final String statusClients;
    /**
     * The topic on which a server will listen for all input messages for all services
     * Has the form {root}/i/svc
     */
    public final String servicesIn;

    public ServerTopics(String root, String topicSeparator) {
        this.root = root;
        this.sep = topicSeparator;
        this.status = make(sep, root, OUT, SYS, STATUS);
        this.statusPrompt = make(sep, root, IN, SYS, STATUS, PROMPT);
        this.statusClients = make(sep, this.status, CLIENT);
        this.servicesIn = make(sep, root, IN , SVC);
    }


    /**
     * Given a fullMethodName return the topic on which to send messages for that method.
     * @param fullMethodName e.g. "helloworld.ExampleHelloService/LotsOfReplies"
     * @return The topic on which to send input messages for a method. Will replace dots with slashes
     * because some brokers (e.g. artemis) will do this anyway so it is better to be consistent.
     * Has the form {root}/i/svc/helloworld/ExampleHelloService/LotsOfReplies
     */
    public String methodIn(String fullMethodName){
        //fullMethodName will be e.g. "helloworld.ExampleHelloService/LotsOfReplies"
        //Replace dots with slashes in fullMethodName because some brokers (e.g. artemis) will replace
        //dots with slashes anyway when handling mqtt. So it's better to do this up front here and
        //then on the server side put the dots back in before calling the method.
        final String fullMethodNameWithSlashes = fullMethodName.replace(".", sep);
        return make(sep, servicesIn, fullMethodNameWithSlashes);
    }

    /**
     * Return the inverse of {@link #methodIn(String)}
     * @param topic e.g. {root}/i/svc/helloworld/ExampleHelloService/LotsOfReplies
     * @return helloworld.ExampleHelloService/LotsOfReplies
     */
    public String fullMethodNameFromTopic(String topic){
        final String fullMethodNameWithSlashes = topic.substring(servicesIn.length() + 1);
        final int lastSlashPos = fullMethodNameWithSlashes.lastIndexOf(sep);
        if(lastSlashPos == -1){
            //This should never happen
            throw new RuntimeException("A grpc topic should always have at least on slash");
        }
        return fullMethodNameWithSlashes.substring(0, lastSlashPos).replace(sep, ".") + sep
                + fullMethodNameWithSlashes.substring(lastSlashPos + 1);
    }

    /**
     * Return the replyTopic for a method call. Dots in fullMethodName will be replaced with slashes.
     * @param replyTopicPrefix e.g. "myServer/o/svc/dlfxl55d7hsn6lwl" (where "dlfxl55d7hsn6lwl" is channelId)
     * @param fullMethodName e.g. "helloworld.ExampleHelloService/LotsOfReplies"
     * @return e.g "myServer/helloworld/ExampleHelloService/LotsOfReplies/ppjupponvo5vtpzt"
     */
    public static String replyTopic(String replyTopicPrefix, String topicSeparator, String fullMethodName){
        return make(topicSeparator, replyTopicPrefix, fullMethodName.replace(".", topicSeparator));
    }

    /**
     * @return The topic on which a server will send all output messages for a particular client
     * Has the form {root}/o/svc/{channelId}
     */
    public String servicesOutForChannel(String channelId){
        return make(sep, root, OUT , SVC, channelId);
    }


    public static String out(String topicSeparator, String server, String ... segments){
        return make(topicSeparator, make(topicSeparator, server, OUT), make(topicSeparator, segments));
    }



    /**
     * Convenience method for constructing a topic from a set of strings
     * It just inserts the '/' separator between them
     */
    public static String make(String topicSeparator, String ... segments)
    {
        if(segments.length == 0) {
            return null;
        }

        if(segments.length == 1) {
            return segments[0];
        }

        StringBuffer result = new StringBuffer(segments[0]);
        for(int i = 1; i < segments.length; i++) {
            result.append(topicSeparator).append(segments[i]);
        }

        return result.toString();
    }

}
