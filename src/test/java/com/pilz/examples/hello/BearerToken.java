package com.pilz.examples.hello;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;

import java.util.concurrent.Executor;

public class BearerToken extends CallCredentials{

    public static final String BEARER_TYPE = "Bearer";

    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);


    private String value;

    public BearerToken(String value) {
        this.value = value;
    }

    @Override
    public void applyRequestMetadata(CallCredentials.RequestInfo requestInfo, Executor executor, CallCredentials.MetadataApplier metadataApplier) {
        executor.execute(() -> {
            try {
                Metadata headers = new Metadata();
                headers.put(AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, value));
                metadataApplier.apply(headers);
            } catch (Throwable e) {
                metadataApplier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
        });
    }

    @Override
    public void thisUsesUnstableApi() {
        // noop
    }
}
