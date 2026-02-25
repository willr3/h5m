# Phase 4: Notifications & Alerting

## Overview

Add a notification system that reports detected changes. The primary deliverable is a console summary printed after uploads when changes are detected. A secondary deliverable is webhook support for external integrations (Slack relays, CI systems, etc.).

**Depends on:** Phase 2 (Change entity must exist to report on).

## Console Change Summary

### Integration Point

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/H5m.java`

The `upload` command currently uploads data and returns immediately while the WorkQueue processes in the background. To display a change summary, the upload command needs to wait for work completion.

#### Modified Upload Flow

```java
@CommandLine.Command(name = "upload", description = "upload data to a folder")
public int upload(
        @CommandLine.Parameters(index = "0") String path,
        @CommandLine.Option(names = {"to"}, arity = "1") String folderName,
        @CommandLine.Option(names = {"--no-wait"}, description = "don't wait for change detection")
        boolean noWait
) {
    // ... existing upload logic (read file, call folderService.upload()) ...

    if (!noWait) {
        // Wait for work queue to drain
        workExecutor.getWorkQueue().awaitEmpty(timeout);

        // Query changes linked to this upload
        List<Change> changes = changeService.listChangesByRootValue(rootValue);

        // Print summary
        String summary = notificationService.formatChangeSummary(changes);
        System.out.println(summary);

        // Non-zero exit if unconfirmed changes detected (for CI)
        if (changes.stream().anyMatch(c -> c.confirmed == null)) {
            return 2;  // Distinct from error (1)
        }
    }
    return 0;
}
```

#### WorkQueue.awaitEmpty()

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/queue/WorkQueue.java`

Add a method to block until all pending/active work is complete:

```java
public boolean awaitEmpty(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    takeLock.lock();
    try {
        while (!pendingWork.isEmpty() || !activeWork.isEmpty()) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return false;
            notEmpty.await(remaining, TimeUnit.MILLISECONDS);
        }
        return true;
    } finally {
        takeLock.unlock();
    }
}
```

Note: The WorkQueue already has a `notEmpty` Condition. We may need an additional `empty` Condition that is signaled when work completes. This requires modifying `WorkRunner` to signal when work finishes and the queue becomes empty.

#### Console Output Format

Uses the existing `ListCmd.table()` utility:

```
Changes detected:
┌────────────────┬─────────────┬──────────┬────────┬───────┬───────────┐
│      Node      │ Fingerprint │   Type   │  Value │ Ratio │  Status   │
├────────────────┼─────────────┼──────────┼────────┼───────┼───────────┤
│ throughput-rd   │ abc123      │ rd       │ 750.0  │ -25%  │ unreviewed│
│ latency-ft     │ def456      │ ft:above │ 250.1  │ >200  │ unreviewed│
│ throughput-ed   │ abc123      │ ed       │ 1050.0 │ +400% │ unreviewed│
└────────────────┴─────────────┴──────────┴────────┴───────┴───────────┘
```

If no changes detected:
```
No changes detected.
```

#### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success, no unconfirmed changes |
| 1 | Error (file not found, folder missing, etc.) |
| 2 | Success, but unconfirmed changes detected |

This enables CI pipelines to fail builds on regressions:
```bash
h5m upload results.json to perftest || echo "Regression detected!"
```

## NotificationService

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/svc/NotificationService.java`

```java
@ApplicationScoped
public class NotificationService {

    @Inject
    EntityManager em;

    @Inject
    ChangeService changeService;

    /**
     * Format a console-friendly summary of detected changes.
     */
    public String formatChangeSummary(List<Change> changes) {
        if (changes.isEmpty()) {
            return "No changes detected.";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Changes detected:").append(System.lineSeparator());

        String table = ListCmd.table(120, changes,
            List.of("Node", "Fingerprint", "Type", "Value", "Detail", "Status"),
            List.of(
                c -> c.detectionNode.name,
                c -> extractFingerprint(c.changeValue),
                c -> c.detectionNode.type,
                c -> extractValue(c.changeValue),
                c -> extractDetail(c.changeValue, c.detectionNode.type),
                c -> formatStatus(c.confirmed)
            )
        );
        sb.append(table);
        return sb.toString();
    }

    private String formatStatus(Boolean confirmed) {
        if (confirmed == null) return "unreviewed";
        return confirmed ? "confirmed" : "dismissed";
    }

    private Object extractFingerprint(Value changeValue) {
        return changeValue.data.has("fingerprint")
            ? changeValue.data.get("fingerprint").asText() : "";
    }

    private Object extractValue(Value changeValue) {
        if (changeValue.data.has("value")) return changeValue.data.get("value").asDouble();
        if (changeValue.data.has("afterMean")) return changeValue.data.get("afterMean").asDouble();
        return "";
    }

    private String extractDetail(Value changeValue, String type) {
        return switch (type) {
            case "rd" -> changeValue.data.has("ratio")
                ? String.format("%.1f%%", changeValue.data.get("ratio").asDouble()) : "";
            case "ft" -> changeValue.data.has("direction")
                ? changeValue.data.get("direction").asText() + " "
                  + changeValue.data.get("bound").asDouble() : "";
            case "ed" -> changeValue.data.has("percentChange")
                ? String.format("%.1f%%", changeValue.data.get("percentChange").asDouble()) : "";
            default -> "";
        };
    }

    /**
     * Dispatch notifications to all configured channels for a folder.
     */
    @Transactional
    public void notify(Folder folder, Value rootValue) {
        List<Change> changes = changeService.listChangesByRootValue(rootValue);
        if (changes.isEmpty()) return;

        List<NotificationConfig> configs = NotificationConfig.find(
            "folder = ?1 and enabled = true", folder
        ).list();

        for (NotificationConfig config : configs) {
            switch (config.type) {
                case WEBHOOK -> sendWebhook(config, folder, changes);
            }
        }
    }

    private void sendWebhook(NotificationConfig config, Folder folder, List<Change> changes) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode payload = mapper.createObjectNode();
            payload.put("folder", folder.name);

            ArrayNode changesArray = payload.putArray("changes");
            for (Change change : changes) {
                ObjectNode entry = changesArray.addObject();
                entry.put("node", change.detectionNode.name);
                entry.put("type", change.detectionNode.type);
                entry.set("data", change.changeValue.data);
                entry.put("timestamp", change.timestamp.toString());
            }

            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(config.url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
                .timeout(Duration.ofSeconds(30))
                .build();

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() >= 400) {
                        System.err.println("Webhook failed: " + config.url
                            + " status=" + response.statusCode());
                    }
                })
                .exceptionally(ex -> {
                    System.err.println("Webhook error: " + config.url
                        + " " + ex.getMessage());
                    return null;
                });
        } catch (Exception e) {
            System.err.println("Webhook dispatch error: " + e.getMessage());
        }
    }
}
```

## Entity: NotificationConfig

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/entity/NotificationConfig.java`

```java
@Entity
@Table(name = "notification_config")
public class NotificationConfig extends PanacheEntity {

    public enum NotificationType {
        WEBHOOK
    }

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "folder_id")
    public Folder folder;

    @Enumerated(EnumType.STRING)
    public NotificationType type;

    public String url;

    public boolean enabled = true;
}
```

### Database Schema

| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGINT | PK, auto-generated |
| folder_id | BIGINT | FK -> folder(id) |
| type | VARCHAR | NOT NULL (enum) |
| url | VARCHAR | NOT NULL |
| enabled | BOOLEAN | NOT NULL, default true |

### FreshDb Update

Add cleanup:
```java
stmt.executeUpdate("TRUNCATE TABLE notification_config CASCADE");  // postgresql
stmt.executeUpdate("DELETE from notification_config");              // sqlite
```

## Webhook JSON Payload

```json
{
  "folder": "perftest",
  "changes": [
    {
      "node": "throughput-rd",
      "type": "rd",
      "data": {
        "previous": 1000.0,
        "last": 750.0,
        "value": 750.0,
        "ratio": -25.0,
        "domainvalue": "2024-01-15"
      },
      "timestamp": "2024-01-15T10:30:00"
    },
    {
      "node": "latency-ft",
      "type": "ft",
      "data": {
        "value": 250.1,
        "bound": 200.0,
        "direction": "above",
        "fingerprint": "abc123"
      },
      "timestamp": "2024-01-15T10:30:00"
    }
  ]
}
```

## CLI Commands

### NotifyAdd

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/NotifyAdd.java`

```java
@CommandLine.Command(name = "add", description = "add a notification config")
public class NotifyAdd implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "notification type (webhook)")
    String type;

    @CommandLine.Parameters(index = "1", description = "URL for webhook")
    String url;

    @CommandLine.Option(names = {"to"}, description = "folder name")
    String folderName;
}
```

### NotifyList

```java
@CommandLine.Command(name = "list", description = "list notification configs")
public class NotifyList implements Callable<Integer> {

    @CommandLine.Option(names = {"in"}, description = "folder name")
    String folderName;
}
```

### NotifyRemove

```java
@CommandLine.Command(name = "remove", description = "remove a notification config")
public class NotifyRemove implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "notification config id")
    long configId;
}
```

### Notify Parent Command

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/NotifyCmd.java`

```java
@CommandLine.Command(name = "notify", description = "manage notifications",
    subcommands = {NotifyAdd.class, NotifyList.class, NotifyRemove.class})
public class NotifyCmd implements Callable<Integer> { }
```

**Registration:** Add `NotifyCmd.class` to `H5m`'s `subcommands` array.

**Usage:**
```
h5m notify add webhook https://hooks.slack.com/services/xxx to perftest
h5m notify list in perftest
h5m notify remove 1
```

## Upload Command Changes

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/H5m.java`

The upload command needs to track which root Values were created to query changes afterward. Current code in `upload()`:

```java
// Current: just calls folderService.upload() and returns
folderService.upload(folder, f.getPath(), read);
```

Modified to:
1. Collect root Value references from each upload
2. After all uploads, wait for WorkQueue to drain (unless `--no-wait`)
3. Query changes by root values
4. Print summary and dispatch notifications

### Root Value Access

`FolderService.upload()` currently creates the root Value internally. Two options:
1. **Return the root Value** from `upload()` -- requires changing the return type from void to Value
2. **Query root Values** after upload by finding the most recent root Values in the folder

Option 1 is cleaner. Modify `FolderService.upload()` to return `Value`:

```java
@Transactional
public Value upload(Folder folder, String path, JsonNode data) {
    // ... existing logic ...
    Value newValue = new Value(folder, folder.group.root, data);
    valueService.create(newValue);
    // ... queue work ...
    return newValue;
}
```

## Files Modified/Created

| File | Action |
|------|--------|
| `h5m-core/.../entity/NotificationConfig.java` | **Create** |
| `h5m-core/.../svc/NotificationService.java` | **Create** |
| `h5m-core/.../svc/FolderService.java` | **Modify** -- return Value from upload() |
| `h5m-core/.../queue/WorkQueue.java` | **Modify** -- add awaitEmpty() |
| `h5m/src/.../cli/H5m.java` | **Modify** -- upload waits + prints summary, add NotifyCmd |
| `h5m/src/.../cli/NotifyCmd.java` | **Create** |
| `h5m/src/.../cli/NotifyAdd.java` | **Create** |
| `h5m/src/.../cli/NotifyList.java` | **Create** |
| `h5m/src/.../cli/NotifyRemove.java` | **Create** |
| `h5m-core/src/test/.../FreshDb.java` | **Modify** -- add notification_config cleanup |

## Tests

### Unit Tests

**File:** `h5m-core/src/test/java/io/hyperfoil/tools/h5m/svc/NotificationServiceTest.java`

1. **formatChangeSummary() with changes:** Create Change entities with known data. Call `formatChangeSummary()`. Assert output contains node names, values, and status.

2. **formatChangeSummary() empty:** Pass empty list. Assert output is "No changes detected."

3. **Webhook dispatch:** Mock HTTP server (or verify request construction). Configure a webhook, call `notify()`. Assert POST payload contains correct JSON structure with folder name, change data, and timestamps.

4. **Webhook failure handling:** Configure webhook to unreachable URL. Call `notify()`. Assert no exception is thrown (fire-and-forget). Verify error is logged to stderr.

5. **Disabled webhook:** Set `enabled=false` on NotificationConfig. Call `notify()`. Assert no HTTP request is made.

### Integration Tests

1. **End-to-end upload with summary:** Create folder, add RelativeDifference node, upload data that triggers a regression. Wait for WorkQueue. Verify the upload command would produce a change summary containing the expected node name and ratio.

2. **Exit code for regressions:** Same setup, verify the upload method returns exit code 2 when unconfirmed changes exist.

3. **Exit code for clean upload:** Upload data that does not trigger any changes. Verify exit code 0.

## Recalculation Behavior

This section addresses how notifications interact with the recalculation pipeline (see [#12](https://github.com/Hyperfoil/h5m/issues/12)).

### Should recalculation trigger notifications?

Recalculation (`h5m recalculate`) re-runs all nodes for all existing uploads. This can produce change Values identical to the originals (no effective change) or different change Values (if node config changed or data was modified).

**Recommendation:** Notifications should NOT be triggered during recalculation. Rationale:
- Recalculation is a manual administrative operation, not a new data upload
- Sending notifications for re-detected known regressions would be noisy and confusing
- Users expect notifications only when *new* data reveals *new* regressions

**Implementation:** The `notify()` method in NotificationService is only called from the `upload` command (Phase 4), not from `recalculate`. Since `FolderService.recalculate()` directly queues Work without going through the upload CLI path, notifications are naturally excluded.

### Console summary during recalculation

The `recalculate` CLI command currently has no post-completion output. After Phase 4, it could optionally print a summary of all changes detected across the recalculation. This would be useful for verifying that a node config change had the intended effect.

```java
@CommandLine.Command(name = "recalculate")
public int recalculate(String folderName,
        @CommandLine.Option(names = {"--summary"}) boolean summary) {
    folderService.recalculate(folder);
    if (summary) {
        workExecutor.getWorkQueue().awaitEmpty(timeout);
        List<Change> changes = changeService.listChanges(folder, null);
        System.out.println(notificationService.formatChangeSummary(changes));
    }
    return 0;
}
```

This is a secondary feature, not required for v1.

### Upload notification timing and WorkRunner

The upload command waits for the WorkQueue to drain before querying Changes and sending notifications. The recalculation equality check in WorkRunner (lines 91-95) means:

- If an upload adds data that doesn't trigger any detection node -> no change Values produced -> no Changes created -> notification says "No changes detected"
- If an upload adds data that triggers a detection node -> change Values produced -> Changes created -> notification lists the new regressions
- If a detection node was already triggered by previous data and the recalculation produces the same change Values -> WorkRunner's equality check passes -> Values are not in `newOrUpdated` -> no downstream work -> Changes already exist from the previous run

The key subtlety: even if no *new* Changes are created during this upload, the notification should only report Changes linked to *this upload's root Value*. The `listChangesByRootValue()` query ensures this -- it won't report Changes from previous uploads.

### Tests for recalculation interaction

Add to `NotificationServiceTest.java`:

1. **No duplicate notifications on recalculation:** Upload data that triggers a change. Verify notification is sent. Recalculate. Verify no additional notification is sent (since `notify()` is only called from the upload path).

2. **Notification after recalculation with changed config:** Upload data, then change a threshold. Recalculate. Upload new data. Verify the notification reflects the updated detection results (new threshold), not the old ones.

3. **Upload notification only reports current upload's changes:** Upload two files sequentially. Each triggers a different change. Verify each upload's notification only contains its own changes, not the other upload's.

## Open Questions

1. **Default wait behavior:** Should `upload` wait by default (with `--no-wait` to skip), or should it not wait by default (with `--wait` to enable)? The plan says "default behavior" but the current upload returns immediately. Waiting is more useful for CI but slower for interactive use.

2. **WorkQueue drain detection:** The current WorkQueue uses a `ReentrantLock` with a `notEmpty` condition. Adding `awaitEmpty()` requires signaling when the queue becomes empty. This means `WorkRunner` needs to signal a condition after completing work and finding the queue empty. Need to be careful about race conditions where new work is added between the check and the signal.

3. **Multiple file upload summary:** When uploading a directory, should changes be reported per-file or as a combined summary after all files are uploaded? The current design queries by root Value, so it would naturally be per-file. A combined summary at the end might be more useful.

4. **Webhook retry policy:** Current design is fire-and-forget. Should failed webhooks be retried? If so, how many times, with what backoff? For v1, fire-and-forget with logging is simpler and sufficient.

5. **Webhook authentication:** Should webhooks support custom headers (e.g., `Authorization: Bearer token`)? Not in v1, but the `NotificationConfig` entity could later add a `headers` JSON column.
