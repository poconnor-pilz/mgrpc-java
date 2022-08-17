package io.grpc.examples.routeguide;

import io.grpc.stub.StreamObserver;

public interface IRouteGuideService {
    void getFeature(Point request, StreamObserver<Feature> responseObserver);

    void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver);

    StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver);

    StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver);
}
