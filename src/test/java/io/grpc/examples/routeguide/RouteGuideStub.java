package io.grpc.examples.routeguide;

import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.StreamToBufferObserver;
import io.grpc.stub.StreamObserver;

public class RouteGuideStub implements IRouteGuideService{

    final ProtoSender sender;

    public RouteGuideStub(ProtoSender sender) {
        this.sender = sender;
    }

    @Override
    public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
        sender.sendRequest(IRouteGuideService.GET_FEATURE, request,
                new StreamToBufferObserver<>(Feature.parser(), responseObserver));
    }

    @Override
    public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {
        sender.sendRequest(IRouteGuideService.LIST_FEATURES, request,
                new StreamToBufferObserver<>(Feature.parser(), responseObserver));
    }

    @Override
    public StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver) {
        return sender.sendClientStreamingRequest(IRouteGuideService.RECORD_ROUTE,
                new StreamToBufferObserver<>(RouteSummary.parser(), responseObserver));
    }

    @Override
    public StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver) {
        return sender.sendClientStreamingRequest(IRouteGuideService.ROUTE_CHAT,
                new StreamToBufferObserver<>(RouteNote.parser(), responseObserver));
    }
}
