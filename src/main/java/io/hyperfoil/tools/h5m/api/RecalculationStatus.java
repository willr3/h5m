package io.hyperfoil.tools.h5m.api;

/**
 * Immutable snapshot of a recalculation operation's progress.
 * Used as a REST response DTO — no mutable state or internal references.
 */
public record RecalculationStatus(
        /** Opaque identifier for polling progress via {@code GET /api/folder/recalculation/{id}} */
        String id,
        String folderName,
        /** The node being recalculated, or {@code -1} for a full folder recalculation */
        long nodeId,
        int totalRoots,
        int completedRoots,
        State state,
        String error,
        long durationMs
) {
    public enum State { RUNNING, COMPLETED, FAILED }
}
