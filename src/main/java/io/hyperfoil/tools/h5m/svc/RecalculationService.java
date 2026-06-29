package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.RecalculationStatus;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory tracking of recalculation operations.
 * Completed entries are kept for a retention period (default 10 minutes
 * after completion), then lazily evicted on subsequent reads.
 */
@ApplicationScoped
public class RecalculationService {

    private static final long RETENTION_MS = 10 * 60 * 1000; // 10 minutes

    private final ConcurrentHashMap<String, RecalculationTracker> trackers = new ConcurrentHashMap<>();

    /**
     * Creates a new recalculation tracker.
     *
     * @return the created tracker (includes the generated ID)
     */
    public RecalculationTracker create(String folderName, long nodeId, int totalRoots,
                                        CompletableFuture<Void> future) {
        String id = UUID.randomUUID().toString();
        RecalculationTracker tracker = new RecalculationTracker(id, folderName, nodeId, totalRoots, future);
        trackers.put(id, tracker);
        return tracker;
    }

    /**
     * Returns the tracker for the given recalculation ID, or null if not found.
     * Lazily evicts completed/failed entries that have exceeded the retention period
     * since completion.
     */
    public RecalculationTracker get(String id) {
        RecalculationTracker tracker = trackers.get(id);
        if (tracker != null && tracker.getState() != RecalculationStatus.State.RUNNING) {
            long completedAt = tracker.getCompletedAt();
            if (completedAt > 0 && System.currentTimeMillis() - completedAt > RETENTION_MS) {
                trackers.remove(id);
                return null;
            }
        }
        return tracker;
    }
}
