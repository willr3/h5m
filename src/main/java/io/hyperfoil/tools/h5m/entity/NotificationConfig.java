package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.h5m.notification.NotificationMethod;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;

/**
 * Configuration for a notification channel on a folder.
 * Each folder can have multiple notification configs (e.g., one for email, one for Slack).
 */
@Entity(name = "notification_config")
public class NotificationConfig extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    /**
     * The folder this notification config belongs to.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "folder_id")
    public FolderEntity folder;

    /**
     * The notification method.
     */
    @Enumerated(EnumType.STRING)
    public NotificationMethod method;

    /**
     * Plugin-specific configuration data as a JSON string.
     * Contains non-sensitive settings like URLs, channel names, email addresses.
     * e.g. {"url": "https://hooks.slack.com/..."} for webhook,
     *      {"to": "team@example.com"} for email,
     *      {"channel": "#alerts"} for Slack.
     */
    @Column(columnDefinition = "TEXT")
    public String data;

    /**
     * Plugin-specific secret data as a JSON string.
     * Contains sensitive values like API tokens, passwords.
     * e.g. {"token": "xoxb-..."} for Slack,
     *      {"token": "ghp_..."} for GitHub.
     * This field is excluded from REST responses via the NotificationConfigResponse DTO.
     */
    @Column(columnDefinition = "TEXT")
    public String secrets;

    /**
     * User-defined message template with placeholders.
     * Available placeholders: {folderName}, {nodeName}, {nodeType},
     * {changeCount}, {changes}, {fingerprint}.
     * <p>
     * Example for Slack: "Regression detected in *{folderName}* by {nodeName}: {changeCount} change(s). cc @perf-team"
     * <p>
     * If null or empty, the plugin uses its default message format.
     */
    @Column(columnDefinition = "TEXT")
    public String template;

    /**
     * Whether this notification config is enabled.
     */
    public boolean enabled = true;

    public NotificationConfig() {}

    public NotificationConfig(FolderEntity folder, NotificationMethod method, String data) {
        this.folder = folder;
        this.method = method;
        this.data = data;
        this.enabled = true;
    }

    public NotificationConfig(FolderEntity folder, NotificationMethod method, String data, String secrets) {
        this(folder, method, data);
        this.secrets = secrets;
    }
}
