# Phase 2: Change Entity & Tracking

## Overview

Add a dedicated `Change` entity to persistently record detected regressions with a triage workflow (confirm/dismiss). Currently changes are just Values -- this makes them first-class, queryable, and manageable.

## Entity: Change

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/entity/Change.java`

```java
@Entity
@Table(name = "change")
public class Change extends PanacheEntity {

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "detection_node_id")
    public Node detectionNode;       // Which detection node produced this change (rd, ft, ed)

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "change_value_id")
    public Value changeValue;        // The Value containing change details (ratio, bound, etc.)

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "root_value_id")
    public Value rootValue;          // The upload/root Value that triggered detection

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "folder_id")
    public Folder folder;            // For easy querying by folder

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    public LocalDateTime timestamp;

    public Boolean confirmed;        // null=unreviewed, true=confirmed, false=dismissed

    @Column(columnDefinition = "TEXT")
    public String description;       // Human-readable summary or user notes
}
```

### Design Decisions

1. **`confirmed` is `Boolean` (nullable):** Three-state -- `null` means unreviewed, `true` means confirmed regression, `false` means dismissed (false positive). This maps directly to Horreum's triage workflow.

2. **`changeValue` is `@OneToOne`:** Each Change links to exactly one change Value. Multiple change Values from the same upload produce multiple Change entities.

3. **`rootValue` enables batch querying:** After an upload, we can query "all changes from this upload's root value" to produce a summary.

4. **`folder` is denormalized:** While derivable from `rootValue.folder`, having it directly on Change allows efficient queries like "all unconfirmed changes in folder X" without joins.

## Service: ChangeService

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/svc/ChangeService.java`

```java
@ApplicationScoped
public class ChangeService {

    @Inject
    EntityManager em;

    @Transactional
    public Change recordChange(Node detectionNode, Value changeValue, Value rootValue) {
        // Check for duplicate: same detectionNode + changeValue should not produce two Changes
        Change existing = Change.find(
            "detectionNode = ?1 and changeValue = ?2", detectionNode, changeValue
        ).firstResult();
        if (existing != null) {
            return existing;
        }

        Change change = new Change();
        change.detectionNode = detectionNode;
        change.changeValue = changeValue;
        change.rootValue = rootValue;
        change.folder = rootValue.folder;
        change.confirmed = null;  // unreviewed
        change.persist();
        return change;
    }

    @Transactional
    public List<Change> listChanges(Folder folder, Boolean confirmedFilter) {
        if (folder != null && confirmedFilter != null) {
            return Change.find("folder = ?1 and confirmed = ?2", folder, confirmedFilter).list();
        } else if (folder != null) {
            return Change.find("folder = ?1", folder).list();
        } else if (confirmedFilter != null) {
            return Change.find("confirmed = ?1", confirmedFilter).list();
        }
        return Change.listAll();
    }

    @Transactional
    public List<Change> listUnreviewedChanges(Folder folder) {
        if (folder != null) {
            return Change.find("folder = ?1 and confirmed is null", folder).list();
        }
        return Change.find("confirmed is null").list();
    }

    @Transactional
    public List<Change> listChangesByRootValue(Value rootValue) {
        return Change.find("rootValue = ?1", rootValue).list();
    }

    @Transactional
    public Change confirmChange(long changeId, String description) {
        Change change = Change.findById(changeId);
        if (change != null) {
            change.confirmed = true;
            if (description != null) {
                change.description = description;
            }
        }
        return change;
    }

    @Transactional
    public Change dismissChange(long changeId, String description) {
        Change change = Change.findById(changeId);
        if (change != null) {
            change.confirmed = false;
            if (description != null) {
                change.description = description;
            }
        }
        return change;
    }

    @Transactional
    public Change getChange(long changeId) {
        return Change.findById(changeId);
    }
}
```

## Integration with Detection Nodes

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/svc/NodeService.java`

### Injection

```java
@Inject
ChangeService changeService;
```

### Modify calculateRelativeDifferenceValues()

After the existing code that creates a change Value (around line 415-421 in current NodeService.java):

```java
Value changeValue = new Value(root.folder, relDiff, data);
changeValue.idx = startingOrdinal;
// ... existing source linkage ...
rtrn.add(changeValue);

// NEW: Record change for tracking
changeService.recordChange(relDiff, changeValue, root);
```

### Modify calculateFixedThresholdValues() (Phase 1)

Same pattern -- after creating a change Value, call:
```java
changeService.recordChange(ft, changeValue, root);
```

### Timing Consideration

`recordChange()` is called during `calculateValues()`, which runs inside `WorkRunner.run()` in a `@Transactional` context. The Change entity will be persisted in the same transaction as the Value. If the transaction rolls back (e.g., retry), both the Value and Change are rolled back together.

## CLI Commands

### ListChanges

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/ListChanges.java`

```java
@CommandLine.Command(name = "changes", description = "list detected changes",
    mixinStandardHelpOptions = true)
public class ListChanges implements Callable<Integer> {

    @CommandLine.Option(names = {"in"}, description = "folder name")
    String folderName;

    @CommandLine.Option(names = {"--unconfirmed"}, description = "show only unreviewed changes")
    boolean unconfirmed;

    @CommandLine.Option(names = {"--confirmed"}, description = "show only confirmed changes")
    boolean confirmed;

    @CommandLine.Option(names = {"--dismissed"}, description = "show only dismissed changes")
    boolean dismissed;

    @Inject ChangeService changeService;
    @Inject FolderService folderService;
}
```

**Output:** Uses the existing `ListCmd.table()` method to format a table:

```
| id | folder | node        | fingerprint | direction | value  | bound  | confirmed |
|----|--------|-------------|-------------|-----------|--------|--------|-----------|
| 1  | perf   | throughput  | abc123      | below     | 85.2   | 100.0  | null      |
| 2  | perf   | latency     | def456      | above     | 250.1  | 200.0  | true      |
```

Column accessors extract data from `change.changeValue.data` JSON fields and `change.detectionNode.name`.

### ConfirmChange

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/ConfirmChange.java`

```java
@CommandLine.Command(name = "confirm", description = "confirm a detected change",
    mixinStandardHelpOptions = true)
public class ConfirmChange implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "change id")
    long changeId;

    @CommandLine.Option(names = {"--description"}, description = "reason/notes")
    String description;

    @Inject ChangeService changeService;
}
```

### DismissChange

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/DismissChange.java`

```java
@CommandLine.Command(name = "dismiss", description = "dismiss a detected change",
    mixinStandardHelpOptions = true)
public class DismissChange implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "change id")
    long changeId;

    @CommandLine.Option(names = {"--description"}, description = "reason/notes")
    String description;

    @Inject ChangeService changeService;
}
```

### Registration

- Add `ListChanges.class` to `ListCmd`'s `subcommands` array
- Add `ConfirmChange.class` and `DismissChange.class` to `H5m`'s top-level `subcommands` array

**Usage:**
```
h5m list changes in myTest --unconfirmed
h5m confirm 42 --description "verified regression in throughput"
h5m dismiss 43 --description "expected change due to config update"
```

## Database Schema

The `change` table will be auto-created by Hibernate's schema generation (existing `quarkus.hibernate-orm.database.generation` setting). Columns:

| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGINT | PK, auto-generated |
| detection_node_id | BIGINT | FK -> node(id) |
| change_value_id | BIGINT | FK -> value(id), UNIQUE |
| root_value_id | BIGINT | FK -> value(id) |
| folder_id | BIGINT | FK -> folder(id) |
| created_at | TIMESTAMP | NOT NULL, auto-set |
| confirmed | BOOLEAN | NULLABLE |
| description | TEXT | NULLABLE |

### FreshDb Update

Add `change` table cleanup to `FreshDb.dropRows()`:

```java
// Add before the value_edge cleanup:
stmt.executeUpdate("TRUNCATE TABLE change CASCADE");  // postgresql
stmt.executeUpdate("DELETE from change");              // sqlite
```

## Files Modified/Created

| File | Action |
|------|--------|
| `h5m-core/.../entity/Change.java` | **Create** |
| `h5m-core/.../svc/ChangeService.java` | **Create** |
| `h5m-core/.../svc/NodeService.java` | **Modify** -- inject ChangeService, add `recordChange()` calls |
| `h5m/src/.../cli/ListChanges.java` | **Create** |
| `h5m/src/.../cli/ConfirmChange.java` | **Create** |
| `h5m/src/.../cli/DismissChange.java` | **Create** |
| `h5m/src/.../cli/ListCmd.java` | **Modify** -- add ListChanges to subcommands |
| `h5m/src/.../cli/H5m.java` | **Modify** -- add ConfirmChange, DismissChange to subcommands |
| `h5m-core/src/test/.../FreshDb.java` | **Modify** -- add change table cleanup |

## Tests

### Unit Tests

**File:** `h5m-core/src/test/java/io/hyperfoil/tools/h5m/svc/ChangeServiceTest.java`

1. **recordChange() creates entity:** Call `recordChange()`, verify Change is persisted with correct detectionNode, changeValue, rootValue, folder. Verify `confirmed` is null and `timestamp` is set.

2. **recordChange() deduplication:** Call `recordChange()` twice with same detectionNode + changeValue. Assert only one Change exists.

3. **listChanges() by folder:** Create Changes in two folders. `listChanges(folder1, null)` returns only folder1's changes.

4. **listChanges() filter - unconfirmed:** Create 3 changes: one confirmed, one dismissed, one unreviewed. `listUnreviewedChanges()` returns only the unreviewed one.

5. **listChanges() filter - confirmed:** `listChanges(null, true)` returns only the confirmed one.

6. **confirmChange():** Create a Change, call `confirmChange(id, "reason")`. Verify `confirmed=true` and `description="reason"`.

7. **dismissChange():** Create a Change, call `dismissChange(id, "false alarm")`. Verify `confirmed=false` and description is set.

8. **listChangesByRootValue():** Create Changes from two different uploads. Query by one rootValue, verify only that upload's changes are returned.

### Integration Tests

**File:** `h5m-core/src/test/java/io/hyperfoil/tools/h5m/svc/NodeServiceTest.java`

1. **RelativeDifference records Change:** Set up a RelativeDifference scenario that triggers a change. After `calculateRelativeDifferenceValues()`, verify a Change entity exists in the database linked to the correct detection node and root value.

2. **FixedThreshold records Change (if Phase 1 is done):** Same pattern for FixedThreshold.

## Recalculation Behavior

This section addresses how the Change entity interacts with the recalculation pipeline (see [#12](https://github.com/Hyperfoil/h5m/issues/12)). This is the most critical recalculation concern because Change entities carry user triage state (confirmed/dismissed) that should survive recalculation when possible.

### The problem

During recalculation, `WorkRunner.run()` may delete old change Values and create new ones (see WorkRunner lines 81-111). Since `recordChange()` is called inside the calculation methods, the sequence is:

1. `calculateRelativeDifferenceValues()` / `calculateFixedThresholdValues()` produces new change Values
2. `recordChange()` creates new Change entities pointing to the new Values
3. WorkRunner compares new Values against existing Values by path
4. If an existing change Value is matched by path and has different data -> existing Value's data is updated
5. If an existing change Value has no matching new Value -> it is deleted
6. **Orphaned Change entities now reference deleted Values**

### Recommended approach: fingerprint-based Change matching

Instead of linking Changes solely to Values (which are ephemeral during recalculation), use a **composite natural key** to match Changes across recalculations:

```
Change identity = (detectionNode, fingerprint hash, detection type-specific key)
```

Modify `recordChange()` to find and update existing Changes rather than always creating new ones:

```java
@Transactional
public Change recordChange(Node detectionNode, Value changeValue, Value rootValue) {
    // Extract fingerprint from the change Value's data
    String fingerprint = changeValue.data.has("fingerprint")
        ? changeValue.data.get("fingerprint").asText() : null;

    // Look for an existing Change with the same detection node and fingerprint
    Change existing = null;
    if (fingerprint != null) {
        existing = Change.find(
            "detectionNode = ?1 and fingerprint = ?2",
            detectionNode, fingerprint
        ).firstResult();
    }

    if (existing != null) {
        // Update the existing Change's value reference, preserving triage state
        existing.changeValue = changeValue;
        existing.rootValue = rootValue;
        // Do NOT reset confirmed/description -- preserve user triage
        return existing;
    }

    // Create new Change
    Change change = new Change();
    change.detectionNode = detectionNode;
    change.changeValue = changeValue;
    change.rootValue = rootValue;
    change.folder = rootValue.folder;
    change.fingerprint = fingerprint;  // NEW: store fingerprint on Change entity
    change.confirmed = null;
    change.persist();
    return change;
}
```

This requires adding a `fingerprint` column to the Change entity:

```java
@Column(name = "fingerprint")
public String fingerprint;  // Fingerprint hash for matching across recalculations
```

### Handling deleted change Values

When a recalculation produces *fewer* change Values than before (e.g., a regression was fixed, or thresholds were loosened), the orphaned Changes need cleanup. Two approaches:

**Option A: Cleanup after recalculation (recommended)**

Add a `cleanupOrphanedChanges()` method to ChangeService that removes Changes whose `changeValue` has been deleted:

```java
@Transactional
public int cleanupOrphanedChanges(Node detectionNode) {
    return Change.delete("detectionNode = ?1 and changeValue.id not in " +
        "(select v.id from Value v where v.node = ?1)", detectionNode);
}
```

Call this at the end of each detection calculation method, after all new change Values have been produced.

**Option B: CASCADE DELETE on changeValue**

Add `@OnDelete(action = OnDeleteAction.CASCADE)` to the `changeValue` relationship. When WorkRunner deletes stale Values, the associated Changes are automatically deleted. This is simpler but loses triage state unconditionally.

**Recommendation:** Option A for initial implementation, with Option B as a fallback if the orphan query proves too complex. The key advantage of Option A is that it can be combined with the fingerprint-based matching above: a Change that matches by fingerprint gets its `changeValue` updated (preserving triage), while truly orphaned Changes (no matching fingerprint in the new results) get deleted.

### Impact on each detection node type

| Node Type | Recalculation output stability | Change entity impact |
|-----------|-------------------------------|---------------------|
| RelativeDifference | Deterministic for same inputs. Output changes when series data changes. | Fingerprint-matched Changes preserve triage state. |
| FixedThreshold | Fully deterministic. Same inputs + same thresholds = same output. | Fingerprint-matched Changes preserve triage state. Threshold config changes produce new/different Changes. |
| eDivisive | Deterministic with fixed RNG seed. But adding new data points changes detected change points (indices shift). | eDivisive Changes are more volatile. A change point at index 5 may shift to index 6 when new data is added. Fingerprint matching still works, but multiple change points per fingerprint need an additional key (e.g., `changePointIndex` range). |

### Tests for recalculation

Add to `ChangeServiceTest.java`:

1. **Change survives recalculation with same data:** Create a Change via `recordChange()`. Confirm it. Recalculate (call `recordChange()` again with a new Value but same detectionNode and fingerprint). Assert the Change is still confirmed and now points to the new Value.

2. **Change is cleaned up when regression is fixed:** Create a Change for a detected regression. Recalculate with data that no longer triggers the regression (no change Value produced). Call `cleanupOrphanedChanges()`. Assert the Change has been deleted.

3. **New Change created for new fingerprint:** Create a Change for fingerprint "abc". Recalculate and also detect a regression for fingerprint "def". Assert two Changes exist.

4. **Recalculation does not duplicate Changes:** Call `recordChange()` with the same detectionNode and fingerprint multiple times. Assert only one Change entity exists.

## Open Questions

1. **Should Change entities be deleted when their corresponding Value is deleted (cascade)?** Currently the plan says Change is "parallel" to Values. If a recalculation deletes and recreates Values, the Changes should probably be recreated too. Options:
   - Cascade delete Change when changeValue is deleted
   - Clear Changes on recalculation and let them be recreated
   - Keep historical Changes even after recalculation (stale references)

2. **Should there be a unique constraint on `(detectionNode, changeValue)`?** The `recordChange()` method checks for duplicates, but a DB constraint would provide an additional safety net.

3. **Change description auto-generation:** Should `recordChange()` auto-generate a human-readable description from the change Value data (e.g., "throughput dropped 25% from 1000 to 750")? Or should description only be set by users during triage?
