package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.RecalculationStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Internal tracker for a recalculation operation. Holds mutable progress state
 * and the {@link CompletableFuture} that completes when all work finishes.
 *
 * <p>Not serialized directly — use {@link #toStatus()} to create an immutable
 * {@link RecalculationStatus} snapshot for REST responses.</p>
 */
public class RecalculationTracker {

    private final String id;
    private final String folderName;
    private final long nodeId;
    private final int totalRoots;
    private final AtomicInteger completedRoots = new AtomicInteger(0);
    private final long startedAt;
    private final CompletableFuture<Void> future;
    private volatile RecalculationStatus.State state = RecalculationStatus.State.RUNNING;
    private volatile String error;
    private volatile long completedAt;

    public RecalculationTracker(String id, String folderName, long nodeId, int totalRoots,
                                 CompletableFuture<Void> future) {
        this.id = id;
        this.folderName = folderName;
        this.nodeId = nodeId;
        this.totalRoots = totalRoots;
        this.startedAt = System.currentTimeMillis();
        this.future = future;

        future.whenComplete((v, t) -> {
            completedAt = System.currentTimeMillis();
            if (t != null) {
                state = RecalculationStatus.State.FAILED;
                error = t.getMessage();
            } else {
                state = RecalculationStatus.State.COMPLETED;
            }
        });
    }

    public String getId() { return id; }
    public String getFolderName() { return folderName; }
    public long getNodeId() { return nodeId; }
    public CompletableFuture<Void> getFuture() { return future; }
    public long getStartedAt() { return startedAt; }
    public long getCompletedAt() { return completedAt; }

    public void incrementCompleted() { completedRoots.incrementAndGet(); }

    public RecalculationStatus.State getState() { return state; }

    /**
     * Creates an immutable snapshot of the current progress for REST responses.
     */
    public RecalculationStatus toStatus() {
        return new RecalculationStatus(
                id, folderName, nodeId, totalRoots,
                completedRoots.get(), state, error,
                System.currentTimeMillis() - startedAt);
    }
}
