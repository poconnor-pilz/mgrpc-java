package io.mgrpc;

import org.eclipse.paho.client.mqttv3.MqttTopic;

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
     * Clients should send status message to this topic postfixed with clientId
     * Has the form {root}/i/sys/status/client/#
     */
    public final String statusClients;
    /**
     * The topic on which a server will listen for all input messages for all services
     * Has the form {root}/i/svc/#
     */
    public final String servicesIn;

    public ServerTopics(String root) {
        this.root = root;
        this.status = make(root, OUT, SYS, STATUS);
        this.statusPrompt = make(root, IN, SYS, STATUS, PROMPT);
        this.statusClients = make(this.status, CLIENT, "#");
        this.servicesIn = make(root, IN , SVC, "#");
    }


    /**
     * @param fullMethodName e.g. "helloworld.ExampleHelloService/LotsOfReplies"
     * @return The topic on which to send input messages for a method.
     * Has the form {root}/i/svc/{fullMethodName}
     */
    public String methodIn(String fullMethodName){
        //fullMethodName will be e.g. "helloworld.ExampleHelloService/LotsOfReplies"
        return make(root, IN , SVC, fullMethodName);
    }

    /**
     * @return The topic on which a server will send all output messages for a particular client
     * Has the form {root}/o/svc/{clientId}
     */
    public String servicesOutForClient(String clientId){
        return make(root, OUT , SVC, clientId);
    }


    public static String out(String server, String ... segments){
        return make(make(server, OUT), make(segments));
    }



    /**
     * Convenience method for constructing a topic from a set of strings
     * It just inserts the '/' separator between them
     */
    public static String make(String ... segments)
    {
        if(segments.length == 0) {
            return null;
        }

        if(segments.length == 1) {
            return segments[0];
        }

        StringBuffer result = new StringBuffer(segments[0]);
        for(int i = 1; i < segments.length; i++) {
            result.append(MqttTopic.TOPIC_LEVEL_SEPARATOR).append(segments[i]);
        }

        return result.toString();
    }

}
