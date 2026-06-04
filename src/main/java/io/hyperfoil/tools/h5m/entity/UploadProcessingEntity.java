package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * Tracks whether all work for a given upload has been processed.
 * Used for crash recovery: on startup, incomplete uploads are re-triggered.
 */
@Entity(name = "upload_processing")
public class UploadProcessingEntity extends PanacheEntity {

    @Column(name = "root_value_id", nullable = false, updatable = false)
    public long rootValueId;

    @Column(name = "folder_name", nullable = false, updatable = false)
    public String folderName;

    @Column(nullable = false)
    public boolean completed = false;

    @CreationTimestamp
    @Column(updatable = false)
    public LocalDateTime createdAt;

    public UploadProcessingEntity() {}

    public UploadProcessingEntity(long rootValueId, String folderName) {
        this.rootValueId = rootValueId;
        this.folderName = folderName;
    }
}
