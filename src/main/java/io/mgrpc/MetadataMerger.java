package io.mgrpc;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;

public class MetadataMerger extends  CallCredentials.MetadataApplier {

    private final Metadata metadata;
    public Status status = Status.OK;

    public MetadataMerger(Metadata metadata) {
        this.metadata = metadata;
    }

    public Metadata getMetaData(){
        return this.metadata;
    }

    @Override
    public void apply(Metadata headers) {
        this.metadata.merge(headers);
    }

    @Override
    public void fail(Status status) {
        this.status = status;
    }
}
