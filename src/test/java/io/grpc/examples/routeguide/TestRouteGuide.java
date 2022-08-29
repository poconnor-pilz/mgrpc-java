package io.grpc.examples.routeguide;

import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.ProtoServiceManager;
import com.pilz.utils.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class TestRouteGuide {



    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private RouteGuideService service;
    private RouteGuideStub stub;



    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {
        MqttUtils.startEmbeddedBroker();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClientsAndBroker() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
        MqttUtils.stopEmbeddedBroker();
    }

    @BeforeEach
    void setup() throws Exception{

        String serviceBaseTopic = "routeguideservice";

        //Set up the server
        ProtoServiceManager protoServiceManager = new ProtoServiceManager(serverMqtt);
        service = new RouteGuideService(RouteGuideUtil.parseFeatures(RouteGuideUtil.getDefaultFeaturesFile()));
        RouteGuideSkeleton skeleton = new RouteGuideSkeleton(service);
        protoServiceManager.subscribeService(serviceBaseTopic, skeleton);

        //Setup the client stub
        ProtoSender sender = new ProtoSender(clientMqtt, serviceBaseTopic);
        stub = new RouteGuideStub(sender);
    }





}
