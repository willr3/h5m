# Phase 2: Change Detection Reporting (Revised)

## Overview

Provide the ability to query and report detected changes after uploads. This phase was originally designed around a dedicated `Change` entity, but per review feedback the Value DAG already captures all the necessary information — adding a separate entity would duplicate data that is already derivable from the graph.

### Why no Change entity (yet)

As noted in the [issue #14 discussion](https://github.com/Hyperfoil/h5m/issues/14), the existing Value DAG already records:

- **Detection node** — `value.node` (discriminator `rd`, `ft`, or future `ed`)
- **Change data** — `value.data` (ratio, bound, direction, fingerprint, etc.)
- **Root upload** — traceable via `value.sources` ancestor chain
- **Folder** — `value.folder`
- **Fingerprint** — available from the fingerprint node's value in the ancestor chain

A separate `Change` entity would only add `confirmed` (Boolean) and `description` (String) — triage metadata. That is not enough justification to duplicate the DAG information into another table at this stage of the tool's design.

If triage workflows (confirm/dismiss) become a concrete user need, they can be added later as either:
1. Nullable fields on `ValueEntity` itself (only meaningful for detection node values)
2. A lightweight annotation table keyed by value ID

## Querying Detection Values from the DAG

Detection nodes produce `ValueEntity` instances with discriminator types `rd` (RelativeDifference) and `ft` (FixedThreshold). These values sit in the DAG as descendants of the root upload value.

### Finding changes for a specific upload

Given a root `ValueEntity` from an upload, detection values can be found using the existing `ValueService`:

```java
// For each detection node in the folder's node group:
List<ValueEntity> changes = valueService.getDescendantValues(rootValue, detectionNode);
```

This uses the recursive CTE query over `value_edge` to walk the DAG from the root value down to values produced by the specified detection node.

### Finding all detection nodes in a folder

Detection nodes can be identified by their discriminator type:

```java
List<NodeEntity> detectionNodes = folder.group.sources.stream()
    .filter(n -> n instanceof RelativeDifference || n instanceof FixedThreshold)
    .toList();
```

Or by querying for nodes with detection discriminator values:

```java
List<NodeEntity> detectionNodes = NodeEntity.find(
    "group = ?1 and type in ('rd', 'ft')", folder.group
).list();
```

### Extracting change details from Value data

Each detection node type stores structured JSON in `value.data`:

**RelativeDifference (`rd`):**
```json
{
  "previous": 1000.0,
  "last": 750.0,
  "value": 750.0,
  "ratio": -25.0,
  "fingerprint": {"platform": "x86", "config": "default"}
}
```

**FixedThreshold (`ft`):**
```json
{
  "value": 250.1,
  "bound": 200.0,
  "direction": "ABOVE",
  "fingerprint": {"platform": "x86", "config": "default"}
}
```

These fields can be extracted directly for reporting without any intermediate entity.

## Console Output

Detection values are formatted using the existing `ListCmd.table()` utility:

```
Changes detected:
┌────────────────┬─────────────┬──────┬────────┬────────┐
│      Node      │ Fingerprint │ Type │  Value │ Detail │
├────────────────┼─────────────┼──────┼────────┼────────┤
│ throughput-rd   │ abc123      │ rd   │ 750.0  │ -25.0% │
│ latency-ft     │ def456      │ ft   │ 250.1  │ >200.0 │
└────────────────┴─────────────┴──────┴────────┴────────┘
```

If no detection values exist for the upload:
```
No changes detected.
```

## Returning Root Values from Upload

`FolderService.upload()` currently returns `void`. To enable post-upload change querying, it should return the root `ValueEntity`:

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

## Files Modified

| File | Action |
|------|--------|
| `src/main/java/io/hyperfoil/tools/h5m/svc/FolderService.java` | **Modify** — return `ValueEntity` from `upload()` |

## Future: Triage Workflow

If users need the ability to confirm/dismiss detected changes, the simplest approach would be adding optional fields to `ValueEntity`:

```java
// Only meaningful when node.type is a detection type (rd, ft, ed)
public Boolean confirmed;       // null=unreviewed, true=confirmed, false=dismissed
@Column(columnDefinition = "TEXT")
public String triageNote;       // user annotation
```

This avoids a separate entity while still enabling:
- `h5m list changes in myTest --unconfirmed`
- `h5m confirm <value-id> --description "verified regression"`
- `h5m dismiss <value-id> --description "expected change"`

This is intentionally deferred until there is a concrete user need for triage state.
