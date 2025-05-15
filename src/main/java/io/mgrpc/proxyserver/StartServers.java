package io.mgrpc.proxyserver;

import io.mgrpc.MessageServer;
import io.mgrpc.mqtt.MqttServerConduit;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;

public class StartServers {


        public static void main(String[] args) throws Exception {

        if(args.length != 1) {
            System.out.println("Usage: StartProxy mqtt broker url (e.g. tcp://localhost:1887)");
            return;
        }


        final IMqttAsyncClient serverMqtt = new ClientFactory(args[0]).createMqttClient();

        int numServers = 10;

        for(int i = 0; i < numServers; i++){
            String topic = "mgrpc/server-" + i;
            MessageServer server = new MessageServer(new MqttServerConduit(serverMqtt, topic));
            server.addService(new HelloServiceForTest());
            server.start();
        }

        //Prevent main from terminating
        Thread.currentThread().join();
    }
}
