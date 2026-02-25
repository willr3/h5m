# Fingerprint Redesign: Structured Fingerprints + Fingerprint Filter

## Context

Horreum stores fingerprints as structured JSON objects (e.g., `{"platform": "x86", "buildType": "release"}`) and provides a `fingerprintFilter` — a JavaScript function on the Test entity that receives a fingerprint object and returns boolean to gate which fingerprints participate in change detection.

h5m's current `FingerprintNode` concatenates all source values into a string and SHA-256 hashes it into an opaque `TextNode`. This prevents:
1. Inspecting individual fingerprint components (the hash is irreversible)
2. Filtering fingerprints before change detection (no filter mechanism exists)

This plan redesigns fingerprints to be structured JSON and adds a jq-based filter expression on detection nodes (`RelativeDifference`), bringing h5m to parity with Horreum's fingerprint capabilities.

---

## Change 1: Structured Fingerprint Values

### What changes
Rewrite `NodeService.calculateFpValues()` to produce a JSON ObjectNode with sorted keys instead of a SHA-256 hash string.

### Current behavior
```java
// NodeService.java:~835
HashFactory hashFactory = new HashFactory();
String glob = node.sources.stream()
    .map(source -> sourceValues.containsKey(source.name) ? sourceValues.get(source.name).data.toString() : "")
    .collect(Collectors.joining(""));
String hash = hashFactory.getStringHash(glob);
newValue.data = new TextNode(hash);
```
Produces: `"a1b2c3d4e5..."` (opaque hash)

### New behavior
```java
ObjectMapper mapper = new ObjectMapper();
ObjectNode fpObject = mapper.createObjectNode();
TreeMap<String, JsonNode> sorted = new TreeMap<>();
for (Node source : node.sources) {
    if (sourceValues.containsKey(source.name)) {
        sorted.put(source.name, sourceValues.get(source.name).data);
    }
}
sorted.forEach(fpObject::set);
newValue.data = fpObject;
```
Produces: `{"buildType": "release", "platform": "x86"}` (structured, inspectable)

### Why TreeMap
- PostgreSQL's `jsonb` type normalizes key order internally, so `{"b":1,"a":2}` equals `{"a":2,"b":1}` — no issue
- SQLite stores JSON as text, so key order matters for `v.data = :fingerprint` equality — TreeMap ensures deterministic alphabetical key ordering
- This means `ValueService.findMatchingFingerprint()` SQL queries work unchanged on both databases

### Files to modify
- `h5m-core/src/main/java/io/hyperfoil/tools/h5m/svc/NodeService.java` — rewrite `calculateFpValues()` (~line 835)
- Remove `HashFactory` import (no longer needed in this method)

### No changes needed
- `FingerprintNode.java` — entity has no special fields, just a discriminator
- `ValueService.java` — `findMatchingFingerprint()` recursive CTEs with `v.data = :fingerprint` work with structured JSON on both PostgreSQL (jsonb normalization) and SQLite (deterministic key order via TreeMap)

---

## Change 2: Fingerprint Filter on Detection Nodes

### What changes
Add a `fingerprintFilter` configuration field to `RelativeDifference` that holds a jq expression. Before processing each fingerprint in change detection, evaluate the filter — skip fingerprints that don't match.

### Design
- **Filter language:** jq expression
- **Semantics:** The jq expression receives the structured fingerprint JSON as input. If it produces a truthy output (`true`, non-null, non-empty), the fingerprint passes. If it produces `false`, `null`, or empty output, the fingerprint is skipped.
- **Examples:**
  - `.platform == "x86"` — only detect changes for x86 platform
  - `.buildType == "release"` — only detect changes for release builds
  - `.platform == "x86" and .buildType == "release"` — compound filter
  - `select(.platform != "experimental")` — exclude experimental platform

### Files to modify

**`h5m-core/.../entity/node/RelativeDifference.java`**
- Add constant: `private static final String FINGERPRINT_FILTER = "fingerprintFilter";`
- Add getter/setter:
```java
public String getFingerprintFilter() {
    return getOperationConfig().has(FINGERPRINT_FILTER)
        ? getOperationConfig().get(FINGERPRINT_FILTER).asText() : null;
}
public void setFingerprintFilter(String filter) {
    ((ObjectNode) getOperationConfig()).put(FINGERPRINT_FILTER, filter);
}
```

**`h5m-core/.../svc/NodeService.java`**
- Add helper method `evaluateFingerprintFilter(String filter, JsonNode fingerprint)`:
  - Evaluates the fingerprint JSON through the jq filter expression using jackson-jq (in-process, no external binary)
  - Returns `true` if the filter produces a truthy output (`true`, non-null, non-empty), `false` otherwise
  - Returns `true` if filter is null/empty (no filter = pass all)
  - Uses the same `compileJq()` caching and `JQ_SCOPE` pattern as `calculateJqValues()`
- Modify `calculateRelativeDifferenceValues()` (~line 287):
  - After getting `fingerprintValues` list, before the loop that processes each fingerprint:
  - Get the filter: `String fpFilter = node.getFingerprintFilter();`
  - Inside the fingerprint loop, add early check:
    ```java
    if (fpFilter != null && !evaluateFingerprintFilter(fpFilter, fingerprintValue.data)) {
        continue; // skip this fingerprint
    }
    ```

**`h5m/src/.../cli/AddRelativeDifference.java`**
- Add CLI option: `@Option(names = {"--fingerprint-filter", "-ff"}, description = "jq filter expression for fingerprints")`
- Pass to RelativeDifference via `rd.setFingerprintFilter(fingerprintFilter)` after node creation

---

## Migration

### Existing data
- Existing fingerprint Values contain SHA-256 hash strings
- After the change, new fingerprint Values will be structured JSON
- Old and new fingerprints are inherently different values, so they won't match each other in `findMatchingFingerprint()` — this is correct behavior: old data uses old fingerprints, new uploads use new fingerprints
- No database migration needed — Value.data is a JSONB column that accepts any JSON type

### Backward compatibility
- Existing RelativeDifference nodes without a `fingerprintFilter` setting work unchanged (filter defaults to null = pass all)
- The `FingerprintNode` entity class is unchanged
- CLI commands without `--fingerprint-filter` work as before

---

## Verification

1. **Unit test:** Create a FingerprintNode with 2 source nodes ("platform", "buildType"). Feed values `{"platform": "x86"}` and `{"buildType": "release"}`. Assert the fingerprint Value is `{"buildType": "release", "platform": "x86"}` (sorted keys).
2. **Unit test:** Test `evaluateFingerprintFilter(".platform == \"x86\"", {"platform": "x86", "buildType": "release"})` returns true.
3. **Unit test:** Test `evaluateFingerprintFilter(".platform == \"arm\"", {"platform": "x86", "buildType": "release"})` returns false.
4. **Integration test:** Create folder with relativedifference node + fingerprint filter. Upload data with multiple fingerprints. Verify change detection only runs for matching fingerprints.
5. **Manual test:** Run existing test suite (`mvn test -pl h5m-core`) to verify no regressions.
