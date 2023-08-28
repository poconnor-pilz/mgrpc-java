package io.mgrpc;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

public class EmbeddedBroker {

    private static boolean STARTED = false;
    public static void start() throws Exception {

        if(STARTED){
            return;
        }
        synchronized (EmbeddedBroker.class) {
            EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
            embeddedActiveMQ.start();
            STARTED = true;
        }
    }

    public static void main(String[] args) throws Exception{
        start();
    }

}
