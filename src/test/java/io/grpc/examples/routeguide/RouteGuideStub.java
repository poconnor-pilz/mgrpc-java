package io.grpc.examples.routeguide;

import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.StreamToBufferObserver;
import io.grpc.stub.StreamObserver;

public class RouteGuideStub implements IRouteGuideService{

    final ProtoSender sender;
    final String serviceName;

    public RouteGuideStub(ProtoSender sender, String serviceName) {
        this.sender = sender;
        this.serviceName = serviceName;
    }

    @Override
    public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
        sender.sendRequest(serviceName, IRouteGuideService.GET_FEATURE, request,
                new StreamToBufferObserver<>(Feature.parser(), responseObserver));
    }

    @Override
    public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {
        sender.sendRequest(serviceName, IRouteGuideService.LIST_FEATURES, request,
                new StreamToBufferObserver<>(Feature.parser(), responseObserver));
    }

    @Override
    public StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver) {
        return sender.sendClientStreamingRequest(serviceName, IRouteGuideService.RECORD_ROUTE,
                new StreamToBufferObserver<>(RouteSummary.parser(), responseObserver));
    }

    @Override
    public StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver) {
        return sender.sendClientStreamingRequest(serviceName, IRouteGuideService.ROUTE_CHAT,
                new StreamToBufferObserver<>(RouteNote.parser(), responseObserver));
    }
}
