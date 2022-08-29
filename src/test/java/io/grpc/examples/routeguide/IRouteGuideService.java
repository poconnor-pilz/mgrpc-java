package io.grpc.examples.routeguide;

import io.grpc.stub.StreamObserver;

public interface IRouteGuideService {

    String GET_FEATURE = "getFeature";
    String LIST_FEATURES = "listFeatures";
    String RECORD_ROUTE = "recordRoute";
    String ROUTE_CHAT = "routeChat";

    void getFeature(Point request, StreamObserver<Feature> responseObserver) throws Exception;

    void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver)throws Exception;

    StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver)throws Exception;

    StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver)throws Exception;
}
