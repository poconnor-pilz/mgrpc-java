package io.grpc.examples.routeguide;

import com.pilz.mqttgrpc.MqttGrpcClient;
import com.pilz.mqttgrpc.StreamToBufferObserver;
import io.grpc.stub.StreamObserver;

public class RouteGuideStub implements IRouteGuideService{

    final MqttGrpcClient mgClient;
    final String serviceName;

    public RouteGuideStub(MqttGrpcClient mgClient, String serviceName) {
        this.mgClient = mgClient;
        this.serviceName = serviceName;
    }

    @Override
    public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
        mgClient.sendRequest(serviceName, IRouteGuideService.GET_FEATURE, request,
                new StreamToBufferObserver<>(Feature.parser(), responseObserver));
    }

    @Override
    public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {
        mgClient.sendRequest(serviceName, IRouteGuideService.LIST_FEATURES, request,
                new StreamToBufferObserver<>(Feature.parser(), responseObserver));
    }

    @Override
    public StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver) {
        return mgClient.sendClientStreamingRequest(serviceName, IRouteGuideService.RECORD_ROUTE,
                new StreamToBufferObserver<>(RouteSummary.parser(), responseObserver));
    }

    @Override
    public StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver) {
        return mgClient.sendClientStreamingRequest(serviceName, IRouteGuideService.ROUTE_CHAT,
                new StreamToBufferObserver<>(RouteNote.parser(), responseObserver));
    }
}
