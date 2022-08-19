package io.grpc.examples.routeguide;

import io.grpc.stub.StreamObserver;

public interface IRouteGuideService {

    String METHOD_GET_FEATURE = "getFeature";
    String METHOD_LIST_FEATURES = "listFeatures";
    String METHOD_RECORD_ROUTE = "recordRoute";
    String METHOD_ROUTE_CHAT = "routeChat";

    void getFeature(Point request, StreamObserver<Feature> responseObserver) throws Exception;

    void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver)throws Exception;

    StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver)throws Exception;

    StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver)throws Exception;
}
