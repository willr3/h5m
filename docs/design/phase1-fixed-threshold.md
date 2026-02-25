# Phase 1: Fixed Threshold Detection Model

## Overview

Add a `FixedThreshold` node type that detects when values exceed static min/max bounds. This is the simplest change detection model and serves as a template for Phase 3 (eDivisive).

## Entity: FixedThreshold

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/entity/node/FixedThreshold.java`

Follows the same single-table inheritance pattern as `RelativeDifference`:

```java
@Entity
@DiscriminatorValue("ft")
public class FixedThreshold extends Node {

    @Transient
    private Json config;  // Parsed from `operation` column on @PostLoad

    // Config fields (stored as JSON in operation):
    // - min (double, default 0.0)
    // - max (double, default 0.0)
    // - minEnabled (boolean, default false)
    // - maxEnabled (boolean, default false)
    // - inclusive (boolean, default true)
}
```

**Config storage** uses the same `operation` column + `Json` config pattern as `RelativeDifference`:
- `@PostLoad` parses `operation` JSON string into transient `Json config`
- Setter methods update both `config` and `operation` string
- Getter methods read from `config` with defaults via `config.getDouble()` / `config.getBoolean()`

**Source nodes** follow the same ordered list convention as `RelativeDifference`:
- `sources[0]` = FingerprintNode (identifies unique series)
- `sources[1]` = GroupBy node (groups values, defaults to root)
- `sources[2]` = Range node (the numeric values to check against thresholds)

Helper methods: `getFingerprintNode()`, `getGroupByNode()`, `getRangeNode()`, `setNodes(fingerprint, groupBy, range)`

## Calculation: calculateFixedThresholdValues()

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/svc/NodeService.java`

### Dispatch Integration

Add `"ft"` case to `calculateValues()` switch statement, following the `"rd"` pattern:

```java
case "ft":
    FixedThreshold fixedThreshold = (FixedThreshold) node;
    for (int rIdx = 0; rIdx < roots.size(); rIdx++) {
        Value root = roots.get(rIdx);
        List<Value> found = calculateFixedThresholdValues(fixedThreshold, root, rtrn.size());
        rtrn.addAll(found);
    }
    break;
```

### Algorithm

```java
@Transactional
public List<Value> calculateFixedThresholdValues(FixedThreshold ft, Value root, int startingOrdinal) {
    // 1. Get fingerprint values from root (same as RelativeDifference)
    List<Value> fingerprintValues = valueService.getDescendantValues(root, ft.getFingerprintNode());

    for (Value fingerprintValue : fingerprintValues) {
        // 2. Get all range values matching this fingerprint
        List<Value> rangeValues = valueService.findMatchingFingerprint(
            ft.getRangeNode(), ft.getGroupByNode(), fingerprintValue, ft.getRangeNode()
        );

        // 3. Check each value against thresholds
        for (Value rangeValue : rangeValues) {
            double value = extractNumeric(rangeValue);

            boolean violation = false;
            String direction = null;
            double bound = 0;

            if (ft.isMinEnabled()) {
                boolean belowMin = ft.isInclusive() ? value < ft.getMin() : value <= ft.getMin();
                if (belowMin) {
                    violation = true;
                    direction = "below";
                    bound = ft.getMin();
                }
            }
            if (ft.isMaxEnabled()) {
                boolean aboveMax = ft.isInclusive() ? value > ft.getMax() : value >= ft.getMax();
                if (aboveMax) {
                    violation = true;
                    direction = "above";
                    bound = ft.getMax();
                }
            }

            if (violation) {
                // 4. Create change Value with violation details
                ObjectNode data = mapper.createObjectNode();
                data.set("value", new DoubleNode(value));
                data.set("bound", new DoubleNode(bound));
                data.set("direction", new TextNode(direction));
                data.set("fingerprint", fingerprintValue.data);

                Value changeValue = new Value(root.folder, ft, data);
                changeValue.idx = startingOrdinal++;
                // Link to groupBy ancestor of fingerprint
                List<Value> parents = valueService.getAncestor(fingerprintValue, ft.getGroupByNode());
                if (parents.size() == 1) {
                    changeValue.sources = parents;
                }
                rtrn.add(changeValue);
            }
        }
    }
}
```

### Key Design Decisions

1. **Cumulative work flag:** FixedThreshold should set `cumulative = true` in `Work` (like RelativeDifference) so that all matching values are checked each time, not just the latest upload. This ensures threshold violations are detected even if data arrives out of order.

2. **Value extraction:** Need a helper to extract a double from a Value's `data` (same pattern as RelativeDifference's inline conversion):
   ```java
   if (obj.data instanceof NumericNode numericNode) {
       return numericNode.asDouble();
   } else if (obj.data.toString().matches("[0-9]+\\.?[0-9]*")) {
       return Double.parseDouble(obj.data.toString());
   }
   ```

3. **Output format:** The change Value contains `{value, bound, direction, fingerprint}` as an ObjectNode, matching the pattern of RelativeDifference's output `{previous, last, value, ratio, domainvalue}`.

## Work Integration

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/entity/Work.java`

The `Work` constructor and `setActiveNode()` need to recognize `FixedThreshold` as cumulative:

```java
if (activeNode instanceof RelativeDifference || activeNode instanceof FixedThreshold) {
    this.cumulative = true;
}
```

## CLI Command: AddFixedThreshold

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/AddFixedThreshold.java`

Follows the same pattern as `AddRelativeDifference`:

```java
@CommandLine.Command(name = "fixedthreshold", separator = " ",
    description = "add a fixed threshold node", mixinStandardHelpOptions = true)
public class AddFixedThreshold implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", arity = "1", description = "node name")
    String name;

    @CommandLine.Option(names = {"to"}, description = "target group / test")
    String groupName;

    @CommandLine.Option(names = {"range"}, arity = "1", description = "node producing values to inspect")
    String rangeName;

    @CommandLine.Option(names = {"by"}, arity = "0..1", description = "grouping node")
    String groupBy;

    @CommandLine.Option(names = {"fingerprint"}, description = "node names to use as fingerprint")
    List<String> fingerprints;

    @CommandLine.Option(names = {"min"}, arity = "0..1", description = "lower bound")
    Double min;

    @CommandLine.Option(names = {"max"}, arity = "0..1", description = "upper bound")
    Double max;

    @CommandLine.Option(names = {"inclusive"}, arity = "0..1",
        description = "whether bounds are inclusive (default true)", defaultValue = "true")
    boolean inclusive;
}
```

**Usage:**
```
h5m add fixedthreshold myThreshold to myTest range throughput by root fingerprint benchmarkName,vmVersion min 100 max 5000
```

**Registration:** Add `AddFixedThreshold.class` to `AddCmd`'s `subcommands` array.

### Node creation logic

Same flow as `AddRelativeDifference.call()`:
1. Validate group exists
2. Resolve range, groupBy, and fingerprint nodes by FQDN
3. Create internal `FingerprintNode` with `_fp-` prefix (e.g., `_fp-myThreshold`)
4. Create `FixedThreshold` with sources `[fingerprintNode, groupByNode, rangeNode]`
5. Set min/max/inclusive config
6. Persist via `nodeService.create()`

Key difference: `min` and `max` are `Double` (nullable) -- if `min` is null, `minEnabled` stays false. If provided, `setMin()` automatically sets `minEnabled = true`.

## Files Modified/Created

| File | Action |
|------|--------|
| `h5m-core/.../entity/node/FixedThreshold.java` | **Create** |
| `h5m-core/.../svc/NodeService.java` | **Modify** -- add `"ft"` case + `calculateFixedThresholdValues()` |
| `h5m-core/.../entity/Work.java` | **Modify** -- recognize FixedThreshold as cumulative |
| `h5m/src/.../cli/AddFixedThreshold.java` | **Create** |
| `h5m/src/.../cli/AddCmd.java` | **Modify** -- add `AddFixedThreshold.class` to subcommands |

## Tests

**File:** `h5m-core/src/test/java/io/hyperfoil/tools/h5m/svc/NodeServiceTest.java`

### Test 1: Basic threshold violation detection
- Create FixedThreshold with min=10, max=100, both enabled
- Set up fingerprint + groupBy + range nodes with values [5, 50, 150]
- Call `calculateFixedThresholdValues()`
- Assert: 2 change Values produced -- value=5 direction="below" bound=10, value=150 direction="above" bound=100
- Assert: no change for value=50

### Test 2: Inclusive flag
- min=10, max=100, inclusive=true: value=10 should NOT trigger (10 >= 10)
- min=10, max=100, inclusive=false: value=10 SHOULD trigger (10 <= 10)

### Test 3: Partial enablement
- Only `minEnabled=true`: values below min trigger, values above any amount do not
- Only `maxEnabled=true`: values above max trigger, values below any amount do not
- Neither enabled: no change Values produced for any input

### Test 4: CLI integration
- Create folder, add nodes, `add fixedthreshold` via CLI
- Upload data with violations
- Verify change Values exist in database via value query

## Recalculation Behavior

This section addresses how FixedThreshold interacts with the recalculation pipeline (see [#12](https://github.com/Hyperfoil/h5m/issues/12)).

### How WorkRunner handles recalculation

When `WorkRunner.run()` executes, it compares newly calculated Values against existing Values by path (`Value.getPath()`). The key logic (WorkRunner lines 81-111):

1. For each source Value, fetch existing descendant Values by path
2. If a new Value's path matches an existing Value:
   - **Same data** (`newValue.data.equals(existingValue.data)`) -> discard the new Value, keep the existing one. No downstream work is triggered.
   - **Different data** -> update `existingValue.data` with the new data. Value is added to `newOrUpdated`, triggering downstream recalculation.
3. If a new Value has no matching path -> persist it as a new Value.
4. If an existing Value has no matching new Value -> delete it (stale result).
5. Downstream Work is only triggered if `newOrUpdated` is non-empty (line 117).

### FixedThreshold output determinism

For a given set of inputs (same range values, same thresholds), `calculateFixedThresholdValues()` produces **deterministic output**:
- The same violations are detected (pure comparison against static bounds)
- The same `idx` values are assigned (determined by iteration order over fingerprint values)
- The same `sources` linkage is established (via `getAncestor()`)

This means the `getPath()` will match, and the `data.equals()` check will pass, so **unchanged inputs produce no downstream recalculation**. This satisfies issue #12's requirement: "a node change that does not change the resulting value does not cause recalculation for dependent nodes."

### When threshold config changes

If a user modifies the FixedThreshold's min/max/inclusive settings:
- Previously non-violating values may now violate (new change Values created)
- Previously violating values may no longer violate (old change Values deleted by WorkRunner step 4)
- The Value `data` content changes (different `bound` value) -> `data.equals()` fails -> downstream work is triggered

This satisfies issue #12's requirement: "a node change that changes the resulting value does trigger recalculation for dependent nodes."

### Multi-value correlation

FixedThreshold can produce multiple change Values (one per violation) from a single root upload. Each Value's path is determined by `node.id + "=" + idx + "," + parent.getPath()`. Since `idx` is assigned sequentially per fingerprint value, and the fingerprint iteration order is stable (determined by the database query), the correlation between old and new Values is correct.

However, if a recalculation changes the *number* of violations (e.g., threshold loosened so fewer values violate), the `idx` assignment may shift. For example:
- Original: violations at values [5, 150] -> idx 0, 1
- After threshold change: only violation at [5] -> idx 0

In this case, WorkRunner will correctly:
- Match idx=0 (value 5) with the existing idx=0 (same path)
- Delete the now-stale idx=1 (value 150) that has no matching new Value

This satisfies issue #12's requirement: "a node change that creates multiple values correctly correlates those values with existing values for change detection."

### Tests for recalculation

Add to `NodeServiceTest.java`:

1. **Recalculation with unchanged thresholds:** Calculate FixedThreshold values, then recalculate with the same inputs. Assert the returned Values have identical `data` to the originals (ensuring `data.equals()` passes in WorkRunner and no downstream work is triggered).

2. **Recalculation with changed thresholds:** Calculate FixedThreshold values with min=10. Change min to 20. Recalculate. Assert the violation set has changed (value 15 is now a violation). Assert the Value `data` differs from the original.

3. **Recalculation with changed input data:** Calculate FixedThreshold values where value=50 is within bounds. Recalculate with value=5 (now below min). Assert a new change Value is produced.

## Open Questions

1. **Should FixedThreshold re-check all historical values on each upload, or only check the newly uploaded value?** The plan says "same pattern as RelativeDifference" which scans all matching fingerprint values. For fixed thresholds, checking only the new value might be more appropriate since the bounds don't depend on historical context. However, using cumulative=true ensures correctness if values are uploaded out of order or recalculated.

2. **Should both min and max violations be reported in a single Value, or should each violation produce its own Value?** The current design produces one Value per violation. If a value is below min AND above max (impossible with sane bounds, but if min > max by misconfiguration), both would be reported separately.
