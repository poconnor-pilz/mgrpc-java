package org.example.greeter;

import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.MessageServer;
import io.mgrpc.mqtt.MqttServerConduit;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.example.mqttutils.MqttUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;

public class GreeterService extends GreeterGrpc.GreeterImplBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    //The root topic for all services for this server
    public static final String SERVER_TOPIC = "mgrpc/helloexample";

    public static void main(final String[] args) throws Exception {

        //Start the embedded MQTT broker which will listen on port 1887.
        //The port is specified in src/main/resources/broker.properties
        MqttUtils.EmbeddedBroker.start();
        MqttAsyncClient serverMqtt = MqttUtils.makeClient(MqttUtils.getBrokerUrl());
        //Set up the server at topic serverTopic
        MessageServer server = new MessageServer(new MqttServerConduit(serverMqtt, SERVER_TOPIC));
        //Add our service
        server.addService(new GreeterService());
        server.start();
    }

    /**
     * @param request a single request
     * @param singleResponse a stream containing a single response
     */
    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloReply> singleResponse){
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
        singleResponse.onNext(reply);
        singleResponse.onCompleted();
    }

    /**
     * @param request A single request from the client
     * @param multipleResponses A stream of responses from the server
     */
    @Override
    public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> multipleResponses) {
        for(int i = 0; i < request.getNumResponses(); i++){
            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName() + " " + i).build();
            multipleResponses.onNext(reply);
        }
        multipleResponses.onCompleted();
    }

    /**
     * @param singleResponse A single response from the service
     * @return A stream on which the client can send multiple requests to the server
     */
    @Override
    public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver<HelloReply> singleResponse) {

        return new StreamObserver<>() {
            private final ArrayList<String> names = new ArrayList<>();

            @Override
            public void onNext(HelloRequest value) {
                log.debug("lotsOfGreetings received " + value);
                names.add(value.getName());
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error in client stream", t);
            }

            @Override
            public void onCompleted() {
                log.debug("lotsOfGreetings onCompleted()");
                String everyone = "";
                for (String name : names) {
                    everyone += name + ",";
                }
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + everyone).build();
                singleResponse.onNext(reply);
                singleResponse.onCompleted();
            }
        };
    }

    /**
     * @param multipleResponses A stream of responses from the server
     * @return A stream on which the client can send multiple requests to the server
     */
    @Override
    public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloReply> multipleResponses) {
        return new StreamObserver<>() {
            @Override
            public void onNext(HelloRequest value) {
                log.debug("bidiHello received " + value);
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + value.getName()).build();
                multipleResponses.onNext(reply);
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error in client stream", t);
            }

            @Override
            public void onCompleted() {
                multipleResponses.onCompleted();
            }
        };
    }
}
