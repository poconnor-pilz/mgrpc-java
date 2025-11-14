package org.example.greeter;

import io.grpc.Channel;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.MessageChannel;
import io.mgrpc.TopicInterceptor;
import io.mgrpc.mqtt.MqttChannelConduit;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.example.mqttutils.MqttUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;

public class ClientStreaming {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Example that executes a server streaming RPC (stream of requests, single response)
     * Before running this example make sure that the GreeterService has been started.
     */
    public static void main(String[] args) throws Exception {


        String brokerUrl = "tcp://localhost:1887";
        MqttAsyncClient clientMqtt = MqttUtils.makeClient(brokerUrl);

        MessageChannel baseChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));
        //Make sure all messages on the channel are routed through GreeterService.SERVER_TOPIC
        Channel channel = TopicInterceptor.intercept(baseChannel, GreeterService.SERVER_TOPIC);

        //Client streaming
        //Send a stream of HelloRequest messages and return a single HelloReply message
        //To see the stream of requests look at the log for GreeterService


        //Set up an observer to receive the response
        class ResponseObserver implements StreamObserver<HelloReply> {

            public HelloReply reply;
            public CountDownLatch latch = new CountDownLatch(1);

            @Override
            public void onNext(HelloReply helloReply) {
                this.reply = helloReply;
            }
            @Override
            public void onCompleted() {
                this.latch.countDown();
            }
            @Override
            public void onError(Throwable throwable) {}
        };

        ResponseObserver responseObserver = new ResponseObserver();

        final GreeterGrpc.GreeterStub stub = GreeterGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        HelloRequest jane = HelloRequest.newBuilder().setName("jane").build();
        HelloRequest john = HelloRequest.newBuilder().setName("john").build();
        StreamObserver<HelloRequest> clientStreamObserver = stub.lotsOfGreetings(responseObserver);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onNext(jane);
        clientStreamObserver.onNext(john);
        clientStreamObserver.onCompleted();

        //Wait for the response observer to receive the stream completion
        responseObserver.latch.await();
        log.debug("Response observer reply = " + responseObserver.reply.toString());


        baseChannel.close();
        clientMqtt.disconnect().waitForCompletion();
        clientMqtt.close();

    }

}
