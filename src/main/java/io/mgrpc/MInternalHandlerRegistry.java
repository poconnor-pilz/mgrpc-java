package io.mgrpc;

import io.grpc.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//Copied from io.grpc.util.MutableHandlerRegistry
//Saves us having to include grpc.core in our build
public class MInternalHandlerRegistry extends HandlerRegistry {
    private final ConcurrentMap<String, ServerServiceDefinition> services
            = new ConcurrentHashMap<>();

    /**
     * Registers a service.
     *
     * @return the previously registered service with the same service descriptor name if exists,
     *         otherwise {@code null}.
     */
    @Nullable
    public ServerServiceDefinition addService(ServerServiceDefinition service) {
        return services.put(service.getServiceDescriptor().getName(), service);
    }

    /**
     * Registers a service.
     *
     * @return the previously registered service with the same service descriptor name if exists,
     *         otherwise {@code null}.
     */
    @Nullable
    public ServerServiceDefinition addService(BindableService bindableService) {
        return addService(bindableService.bindService());
    }

    /**
     * Removes a registered service
     *
     * @return true if the service was found to be removed.
     */
    public boolean removeService(ServerServiceDefinition service) {
        return services.remove(service.getServiceDescriptor().getName(), service);
    }

    /**
     *  Note: This does not necessarily return a consistent view of the map.
     */
    @Override
    @ExperimentalApi("https://github.com/grpc/grpc-java/issues/2222")
    public List<ServerServiceDefinition> getServices() {
        return Collections.unmodifiableList(new ArrayList<>(services.values()));
    }

    /**
     * Note: This does not actually honor the authority provided.  It will, eventually in the future.
     */
    @Override
    @Nullable
    public ServerMethodDefinition<?, ?> lookupMethod(String methodName, @Nullable String authority) {
        String serviceName = MethodDescriptor.extractFullServiceName(methodName);
        if (serviceName == null) {
            return null;
        }
        ServerServiceDefinition service = services.get(serviceName);
        if (service == null) {
            return null;
        }
        return service.getMethod(methodName);
    }

}
