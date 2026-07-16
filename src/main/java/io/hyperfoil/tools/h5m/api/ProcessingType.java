package io.hyperfoil.tools.h5m.api;

/**
 * Type of processing operation tracked by ProcessingTrackerEntity.
 */
public enum ProcessingType {
    /** An upload of new data being processed through the pipeline */
    UPLOAD,
    /** A selective node recalculation (specific node and its dependents) */
    RECALCULATE_NODE
}
