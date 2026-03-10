# Phase 4: Notifications & Alerting

## Overview

Add a notification system that reports detected changes after uploads. The primary deliverable is a **console summary** printed after uploads when changes are detected, with a **non-zero exit code** for CI integration. The notification architecture should be extensible to support additional channels in the future.

**No dependency on a separate Change entity** — notifications are built directly on detection Values from the Value DAG (see [Phase 2 revised](phase2-change-entity.md)).

## Architecture

Change notification must not be tied to the CLI upload command. h5m will also run as a service where API endpoints receive uploads from CI jobs. The detection value querying and reporting logic lives in the **service layer**, reusable by both CLI and future REST endpoints.

```
                    ┌─────────────────┐
                    │  FolderService   │
                    │  .upload()       │ ← returns root ValueEntity
                    │  .getDetection   │ ← queries detection values from DAG
                    │   Values()       │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
         CLI upload     REST API       CLI recalculate
         (H5m.java)     (future)       (H5m.java)
              │              │              │
         prints table    returns JSON   prints summary
         exit code 2     dispatches     (same service call)
                         webhooks/etc
```

The service layer is agnostic to the trigger — both `upload` and `recalculate` produce detection values in the DAG. The `getDetectionValues()` method queries the DAG regardless of how the values were produced. Policy decisions (exit codes, external notifications) are left to each caller.

## Service Layer: Detection Value Querying

**File:** `src/main/java/io/hyperfoil/tools/h5m/svc/FolderService.java`

### FolderService.upload() Return Type

`FolderService.upload()` must return the root `ValueEntity` so callers can query detection values by root:

```java
@Transactional
public ValueEntity upload(FolderEntity folder, String path, JsonNode data) {
    // ... existing logic ...
    ValueEntity newValue = new ValueEntity(folder, folder.group.root, data);
    valueService.create(newValue);
    // ... queue work ...
    return newValue;
}
```

### FolderService.getDetectionValues()

Queries detection values from the Value DAG for a given root value. This is reusable by any caller (CLI, REST, notification dispatch):

```java
@Transactional
public List<ValueEntity> getDetectionValues(FolderEntity folder, ValueEntity rootValue) {
    folder = em.createQuery(
        "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources WHERE f.id = :folderId",
        FolderEntity.class
    ).setParameter("folderId", folder.id).getSingleResult();

    List<NodeEntity> detectionNodes = folder.group.sources.stream()
        .filter(n -> n instanceof RelativeDifference || n instanceof FixedThreshold)
        .toList();

    List<ValueEntity> results = new ArrayList<>();
    for (NodeEntity node : detectionNodes) {
        results.addAll(valueService.getDescendantValues(rootValue, node));
    }
    return results;
}
```

This uses the existing `ValueService.getDescendantValues(root, node)` recursive CTE query to walk the DAG.

## WorkQueue Wait Mechanism

**File:** `src/main/java/io/hyperfoil/tools/h5m/queue/WorkQueue.java`

Any caller that needs to wait for work completion (CLI upload, future API endpoint) needs a way to block until the queue is idle.

Add a method to block until all pending and active work is complete:

```java
private final Condition idle = takeLock.newCondition();

public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    takeLock.lock();
    try {
        while (!activeWork.isEmpty() || !runnables.isEmpty()) {
            if (nanos <= 0L) return false;
            nanos = idle.awaitNanos(nanos);
        }
        return true;
    } finally {
        takeLock.unlock();
    }
}
```

The `idle` condition must be signaled when work completes and the queue becomes empty. Modify `decrement()`:

```java
public void decrement(Work work) {
    fullyLock();
    try {
        activeWork.remove(work);
        if (!runnables.isEmpty()) {
            notEmpty.signalAll();
        }
        if (activeWork.isEmpty() && runnables.isEmpty()) {
            idle.signalAll();
        }
    } finally {
        fullyUnlock();
    }
}
```

Note: `decrement()` is called in `WorkService.execute()`'s `finally` block, which runs after any dependent works have been queued via `create()`. So if both `activeWork` and `runnables` are empty at this point, we are truly idle.

## CLI: Console Change Summary

**File:** `src/main/java/io/hyperfoil/tools/h5m/cli/H5m.java`

The CLI upload command is a thin wrapper that calls the service layer and formats output:

```java
@CommandLine.Command(name = "upload", description = "upload data to a folder")
public int upload(
        @CommandLine.Parameters(index = "0") String path,
        @CommandLine.Option(names = {"to"}, arity = "1") String folderName,
        @CommandLine.Option(names = {"--no-wait"}, description = "skip waiting for change detection")
        boolean noWait
) {
    // ... existing file reading logic ...

    List<ValueEntity> rootValues = new ArrayList<>();
    for (File f : todo) {
        JsonNode read = objectMapper.readTree(f);
        ValueEntity rootValue = folderService.upload(folder, f.getPath(), read);
        rootValues.add(rootValue);
    }

    if (!noWait) {
        // Wait for work queue to drain
        workService.awaitIdle(60, TimeUnit.SECONDS);

        // Query detection values via service layer
        List<ValueEntity> detectionValues = new ArrayList<>();
        for (ValueEntity root : rootValues) {
            detectionValues.addAll(folderService.getDetectionValues(folder, root));
        }

        // Print summary
        printChangeSummary(detectionValues);

        if (!detectionValues.isEmpty()) {
            return 2;
        }
    }
    return 0;
}
```

### Console Output Format

Uses the existing `ListCmd.table()` utility:

```
Changes detected:
┌────────────────┬─────────────┬──────┬────────┬────────┐
│      Node      │ Fingerprint │ Type │  Value │ Detail │
├────────────────┼─────────────┼──────┼────────┼────────┤
│ throughput-rd   │ abc123      │ rd   │ 750.0  │ -25.0% │
│ latency-ft     │ def456      │ ft   │ 250.1  │ >200.0 │
└────────────────┴─────────────┴──────┴────────┴────────┘
```

Detail column formatting per detection type:
- **rd**: ratio as percentage (e.g., `-25.0%`)
- **ft**: direction + bound (e.g., `>200.0`, `<10.0`)

If no changes detected:
```
No changes detected.
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success, no changes detected |
| 1 | Error (file not found, folder missing, etc.) |
| 2 | Success, but changes detected |

This enables CI pipelines:
```bash
h5m upload results.json to perftest
if [ $? -eq 2 ]; then
    echo "Performance regression detected"
fi
```

## Files Modified

| File | Action |
|------|--------|
| `src/main/java/io/hyperfoil/tools/h5m/svc/FolderService.java` | **Modify** — return `ValueEntity` from `upload()`, add `getDetectionValues()` |
| `src/main/java/io/hyperfoil/tools/h5m/queue/WorkQueue.java` | **Modify** — add `idle` condition + `awaitIdle()`, signal in `decrement()` |
| `src/main/java/io/hyperfoil/tools/h5m/cli/H5m.java` | **Modify** — upload waits + prints summary + exit code 2 |

## Tests

### CLI Integration Tests

**File:** `src/test/java/io/hyperfoil/tools/h5m/cli/H5mTest.java`

1. **Upload with change summary:** Create folder with RelativeDifference node, upload data that triggers a regression, verify the output contains a change summary table with node name and ratio.

2. **Upload exit code 2:** Same setup, verify the upload command returns exit code 2 when changes are detected.

3. **Upload exit code 0:** Upload data that does not trigger any detection node, verify exit code 0.

4. **Upload --no-wait:** Upload with `--no-wait` flag, verify the command returns immediately with exit code 0 (no summary printed).

5. **Recalculate with summary:** Create folder with detection nodes, upload data that triggers a regression, recalculate, verify the output contains a detection summary. Verify exit code is 0 (not 2).

6. **Recalculate after threshold change:** Upload data within bounds, recalculate (no detections). Lower threshold so data now violates, recalculate again, verify new detections appear in summary.

### Unit Tests

**File:** `src/test/java/io/hyperfoil/tools/h5m/queue/WorkQueueTest.java`

1. **awaitIdle() returns true when queue is empty:** Create empty queue, call `awaitIdle()`, verify it returns true immediately.

2. **awaitIdle() blocks until work completes:** Add work to queue, call `awaitIdle()` from a separate thread, verify it blocks until work is processed and `decrement()` is called.

3. **awaitIdle() respects timeout:** Add work that is never processed, call `awaitIdle(100, MILLISECONDS)`, verify it returns false after timeout.

## Recalculation Behavior

Recalculation re-runs all detection nodes against all existing root values. This can produce new detection values (e.g., lowering a threshold catches more violations) or remove existing ones (e.g., raising a threshold). The service layer handles this identically to uploads — `getDetectionValues()` queries the DAG regardless of how values were produced.

### CLI recalculate with summary

The `recalculate` command should print a detection summary after completion, using the same service method:

```java
@CommandLine.Command(name = "recalculate")
public int recalculate(String folderName) throws InterruptedException {
    FolderEntity folder = folderService.byName(folderName);
    if (folder == null) {
        System.err.println("could not find folder " + folderName);
        return 1;
    }
    folderService.recalculate(folder);

    // Wait for work queue to drain
    workService.awaitIdle(60, TimeUnit.SECONDS);

    // Query detection values across all root values
    List<ValueEntity> rootValues = valueService.getValues(folder.group.root);
    List<ValueEntity> detectionValues = new ArrayList<>();
    for (ValueEntity root : rootValues) {
        detectionValues.addAll(folderService.getDetectionValues(folder, root));
    }

    printChangeSummary(detectionValues);
    return 0;
}
```

Note: `recalculate` does not return exit code 2 — the caller initiated the recalculation intentionally, so the detection results are informational, not an alert.

### External notifications on recalculate

Future notification channels (webhooks, email, etc.) should NOT be dispatched on recalculate. Recalculation is an administrative operation and re-sending alerts for known regressions would be noise. External notification dispatch is a policy decision made by the caller, not the service layer.

## Future Notification Channels

The console summary and exit code cover the immediate need. The architecture should accommodate future notification channels without over-designing them now. Potential future channels include:

- **Webhooks** — HTTP POST to configured endpoints (Slack incoming webhooks, generic HTTP receivers)
- **CI integration** — JUnit XML reports, GitHub Check annotations
- **Email** — via SMTP relay or third-party API
- **GitHub Issues** — automatic issue creation for detected regressions via `gh` CLI or GitHub API

### Extensibility Approach

The service layer (`FolderService.getDetectionValues()`) provides the data. When a second notification channel is needed beyond console output, introduce:

1. **`NotificationConfig` entity** — stores channel type, destination, and folder association
2. **`NotificationService`** — dispatches to configured channels after upload completes, calls `FolderService.getDetectionValues()` for the data
3. **CLI/API commands** — for managing notification configs

Both CLI and REST API consumers call the same service methods. Adding a new channel means:
1. Add a variant to `NotificationConfig.type`
2. Add a dispatch method in `NotificationService`
3. Optionally add CLI/API endpoints for configuration

This is intentionally left as a sketch — detailed design for specific channels should happen when there is a concrete need.

## Open Questions

1. **Default wait behavior:** Should `upload` wait by default (with `--no-wait` to skip), or not wait by default (with `--wait` to enable)? Waiting is more useful for CI but slower for interactive use. Recommendation: wait by default since the primary use case is CI pipelines where detecting regressions is the whole point.

2. **Multiple file upload summary:** When uploading a directory, should changes be reported per-file or as a combined summary? The current design collects all root values and queries detection values across all of them, producing a single combined summary.
