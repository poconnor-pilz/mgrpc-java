package io.grpc.examples.routeguide;

import com.pilz.mqttgrpc.MqttGrpcClient;
import com.pilz.mqttgrpc.MqttGrpcServer;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

/**
 * This test attempts to mimic the sample code in
 * io.grpc.examples.routeguide.RouteGuideClient
 * io.grpc.examples.routeguide.RouteGuideClientTest
 */
public class TestRouteGuide {



    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private RouteGuideService service;
    private RouteGuideStub stub;


    private static final String DEVICE = "device";

    private static final String SERVICE_NAME = "routeguide";

    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {
        MqttUtils.startEmbeddedBroker();
        serverMqtt = MqttUtils.makeClient(Topics.systemStatus(DEVICE));
        clientMqtt = MqttUtils.makeClient(null);
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
        MqttGrpcServer mqttGrpcServer = new MqttGrpcServer(serverMqtt, DEVICE);
        service = new RouteGuideService(RouteGuideUtil.parseFeatures(RouteGuideUtil.getDefaultFeaturesFile()));
        RouteGuideSkeleton skeleton = new RouteGuideSkeleton(service);
        mqttGrpcServer.subscribeService(serviceBaseTopic, skeleton);

        //Setup the client stub
        MqttGrpcClient mgClient = new MqttGrpcClient(clientMqtt, DEVICE);
        stub = new RouteGuideStub(mgClient, SERVICE_NAME);
    }

}
