package io.hyperfoil.tools.h5m.api;

import jakarta.json.bind.annotation.JsonbTransient;

import java.util.concurrent.CompletableFuture;

public class Upload {
    public final long uploadId;

    @JsonbTransient
    public final CompletableFuture<Void> future;

    public Upload(long uploadId, CompletableFuture<Void> future) {
        this.uploadId = uploadId;
        this.future = future;
    }
}
