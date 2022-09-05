package com.pilz.mqttgrpc;

import org.eclipse.paho.client.mqttv3.MqttTopic;

public class TopicMaker {
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
