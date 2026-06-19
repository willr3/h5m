package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.h5m.api.ProcessingType;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * Tracks whether a processing operation (upload, recalculation) has completed.
 * Used for crash recovery: on startup, incomplete operations are re-triggered.
 *
 * The {@code referenceId} field serves different purposes based on {@code type}:
 * <ul>
 *   <li>UPLOAD: the root value ID (for locating the upload to reprocess)</li>
 *   <li>RECALCULATE: -1 (full folder recalculation, no specific reference)</li>
 *   <li>RECALCULATE_NODE: the node ID (for selective recalculation)</li>
 * </ul>
 */
@Entity(name = "processing_tracker")
public class ProcessingTrackerEntity extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false)
    public ProcessingType type;

    @Column(name = "folder_id", nullable = false, updatable = false)
    public long folderId;

    @Column(name = "reference_id", nullable = false, updatable = false)
    public long referenceId;

    @Column(nullable = false)
    public boolean completed = false;

    @CreationTimestamp
    @Column(updatable = false)
    public LocalDateTime createdAt;

    public ProcessingTrackerEntity() {}

    public ProcessingTrackerEntity(ProcessingType type, long folderId, long referenceId) {
        this.type = type;
        this.folderId = folderId;
        this.referenceId = referenceId;
    }
}
