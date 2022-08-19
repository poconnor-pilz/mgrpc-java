package io.grpc.examples.routeguide;

import io.grpc.stub.StreamObserver;
import com.pilz.mqttgrpc.ProtoSender;

public class RouteGuideStub implements IRouteGuideService{

    final ProtoSender protoSender;

    public RouteGuideStub(ProtoSender protoSender) {
        this.protoSender = protoSender;
    }

    @Override
    public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
//        protoSender.sendRequest(IRouteGuideService.METHOD_GET_FEATURE, request,
//                new BufferToStreamObserver<>(Feature.parser(), responseObserver));
    }

    @Override
    public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {

    }

    @Override
    public StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver) {
        return null;
    }

    @Override
    public StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver) {
        return null;
    }
}
