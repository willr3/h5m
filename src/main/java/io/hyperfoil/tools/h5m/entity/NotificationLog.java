package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * Log of sent notifications for auditing and the web UI notification history page.
 */
@Entity(name = "notification_log")
@Table(indexes = {
    @Index(name = "idx_notification_log_folder", columnList = "folder_id"),
    @Index(name = "idx_notification_log_sent_at", columnList = "sentAt")
})
public class NotificationLog extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "folder_id")
    public FolderEntity folder;

    /** The notification method used (e.g. "webhook", "email") */
    public String method;

    /** Resolved target (URL, email address, channel name) */
    public String destination;

    /** Delivery status: "sent", "failed", "suppressed" */
    public String status;

    /** Error message on failure, null on success */
    @Column(columnDefinition = "TEXT")
    public String errorMessage;

    /** Detection node that triggered this notification */
    public long nodeId;

    /** Name of the detection node */
    public String nodeName;

    /** Number of changes in this notification */
    public int changeCount;

    @CreationTimestamp
    @Column(updatable = false)
    public LocalDateTime sentAt;

    public NotificationLog() {}
}
