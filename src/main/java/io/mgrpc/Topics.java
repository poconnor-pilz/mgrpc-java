package io.mgrpc;

import org.eclipse.paho.client.mqttv3.MqttTopic;

public interface Topics {

    String IN = "i";
    String OUT = "o";
    String SVC = "svc";
    String SYS = "sys";
    String STATUS = "status";
    String STATUSPROMPT = "statusprompt";



    static String methodIn(String server, String fullMethodName){
        //fullMethodName will be e.g. "helloworld.ExampleHelloService/LotsOfReplies"
        return make(server, IN , SVC, fullMethodName);
    }



    static String servicesIn(String server){
        return make(server, IN , SVC);
    }

    static String servicesOut(String server, String clientId){
        return make(server, OUT , SVC, clientId);
    }


    static String out(String server, String ... segments){
        return make(make(server, OUT), make(segments));
    }

    static String systemStatus(String server){
        return make(server, OUT, SYS, STATUS);
    }
    static String systemStatusPrompt(String server){
        return make(server, OUT, SYS, STATUSPROMPT);
    }



    /**
     * Convenience method for constructing a topic from a set of strings
     * It just inserts the '/' separator between them
     */
    static String make(String ... segments)
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
