package io.hyperfoil.tools.h5m.queue;

import io.quarkus.logging.Log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks completion of all work items triggered by a single upload.
 * The counter is incremented when work items are created (including cascade)
 * and decremented when each work item finishes. The future completes
 * when all work reaches zero, or completes exceptionally on failure.
 */
public class UploadTracker {

    private final long rootValueId;
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    public UploadTracker(long rootValueId) {
        this.rootValueId = rootValueId;
    }

    public CompletableFuture<Void> getFuture() {
        return future;
    }

    public void increment(int count) {
        pendingCount.addAndGet(count);
    }

    public void decrement() {
        int remaining = pendingCount.decrementAndGet();
        Log.debugf("UploadTracker[%d]: decrement → %d remaining", rootValueId, remaining);
        if (remaining == 0) {
            future.complete(null);
        } else if (remaining < 0) {
            Log.warnf("UploadTracker[%d]: over-decremented to %d — possible accounting bug", rootValueId, remaining);
        }
    }

    public void fail(Throwable t) {
        Log.errorf(t, "UploadTracker[%d]: work failed", rootValueId);
        // Set pending to a negative sentinel so subsequent decrements
        // cannot trigger future.complete(null) — the failure wins
        pendingCount.set(Integer.MIN_VALUE);
        future.completeExceptionally(t);
    }

    @Override
    public String toString() {
        return "UploadTracker[rootValueId=" + rootValueId + ", pending=" + pendingCount.get() + "]";
    }
}
