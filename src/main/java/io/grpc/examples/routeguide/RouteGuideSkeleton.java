package io.grpc.examples.routeguide;

import com.google.protobuf.ByteString;
import com.pilz.mqttgrpc.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RouteGuideSkeleton implements Skeleton {

    private final IRouteGuideService service;

    public RouteGuideSkeleton(IRouteGuideService service) {
        this.service = service;
    }

    @Override
    public BufferObserver onRequest(String method, ByteString request, BufferObserver responseObserver) throws Exception {
        switch(method){
            case IRouteGuideService.GET_FEATURE:
                service.getFeature(Point.parseFrom(request), new SingleToStreamObserver<>(responseObserver));
                return null;
            case IRouteGuideService.LIST_FEATURES:
                service.listFeatures(Rectangle.parseFrom(request), new BufferToStreamObserver<>(responseObserver));
                return null;
            case IRouteGuideService.RECORD_ROUTE:
                return new StreamToBufferObserver<>(Point.parser(),
                        service.recordRoute(new BufferToStreamObserver<>(responseObserver)));
            case IRouteGuideService.ROUTE_CHAT:
                return new StreamToBufferObserver<>(RouteNote.parser(),
                        service.routeChat(new BufferToStreamObserver<>(responseObserver)));

        }
        log.error("Unmatched method: " + method);
        return null;
    }
}
