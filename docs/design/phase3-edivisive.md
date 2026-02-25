# Phase 3: eDivisive (Hunter) Change Point Detection

## Overview

Add statistical change point detection using the eDivisive algorithm. Unlike RelativeDifference (which compares a sliding window against historical mean), eDivisive finds structural breaks in the entire data series without requiring manual threshold tuning. This is a pure Java implementation using commons-math3 (already a project dependency).

## Algorithm Background

The eDivisive algorithm (Matteson & James, 2014) is a non-parametric method for detecting multiple change points in a time series. It works by:

1. Computing pairwise distances between all observations
2. Finding the split point that maximizes the divergence between the two resulting segments (E-statistic)
3. Using a permutation test to assess statistical significance
4. Recursing on each segment to find additional change points

This is the same algorithm used by Horreum's "Hunter" change detection.

## Entity: EDivisive

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/entity/node/EDivisive.java`

```java
@Entity
@DiscriminatorValue("ed")
public class EDivisive extends Node {

    @Transient
    private Json config;  // Parsed from operation on @PostLoad

    // Config fields (stored as JSON in operation):
    // - significance (double, default 0.05) -- p-value threshold
    // - minSize (int, default 5) -- minimum segment size
    // - permutations (int, default 199) -- number of permutations for significance test
}
```

Same `operation`/`Json` config pattern as RelativeDifference and FixedThreshold.

**Source nodes** (same ordered list convention):
- `sources[0]` = FingerprintNode
- `sources[1]` = GroupBy node
- `sources[2]` = Range node

Helper methods: `getFingerprintNode()`, `getGroupByNode()`, `getRangeNode()`, `setNodes(fingerprint, groupBy, range)`

Accessor methods with defaults:
- `getSignificance()` -> default 0.05
- `getMinSize()` -> default 5
- `getPermutations()` -> default 199

## Algorithm Implementation: EDivisiveAlgorithm

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/changedetection/EDivisiveAlgorithm.java`

This is a pure utility class with no JPA/CDI dependencies, making it independently testable.

```java
public class EDivisiveAlgorithm {

    public record ChangePoint(
        int index,           // Position in the series where change occurs
        double beforeMean,   // Mean of values before the change point
        double afterMean,    // Mean of values after the change point
        double percentChange,// Percentage change between segments
        double pValue        // Statistical significance of this change point
    ) {}

    /**
     * Detect change points in the given time series.
     *
     * @param values      The time series data
     * @param significance p-value threshold (e.g., 0.05)
     * @param minSize     Minimum segment size
     * @param permutations Number of permutations for significance testing
     * @return List of detected change points, ordered by index
     */
    public static List<ChangePoint> detectChangePoints(
            double[] values, double significance, int minSize, int permutations) {
        // ...
    }
}
```

### Algorithm Steps

#### Step 1: Compute Pairwise Distance Matrix

For a series of N values, compute an N x N distance matrix where `D[i][j] = |values[i] - values[j]|`. This uses absolute difference as the distance metric (sufficient for univariate data).

```java
private static double[][] computeDistanceMatrix(double[] values) {
    int n = values.length;
    double[][] dist = new double[n][n];
    for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
            dist[i][j] = Math.abs(values[i] - values[j]);
            dist[j][i] = dist[i][j];
        }
    }
    return dist;
}
```

#### Step 2: Compute E-statistic for a Candidate Split

For a candidate split point `tau` dividing the series into segments A (indices 0..tau-1) and B (indices tau..n-1):

```
E(tau) = (|A| * |B|) / (|A| + |B|) * (2*mean(D_AB) - mean(D_AA) - mean(D_BB))
```

Where:
- `D_AB` = distances between points in A and points in B
- `D_AA` = distances between points within A
- `D_BB` = distances between points within B

```java
private static double computeEStatistic(double[][] dist, int start, int tau, int end) {
    int nA = tau - start;
    int nB = end - tau;
    if (nA < 1 || nB < 1) return 0;

    double sumAB = 0, sumAA = 0, sumBB = 0;
    int countAB = 0, countAA = 0, countBB = 0;

    for (int i = start; i < tau; i++) {
        for (int j = tau; j < end; j++) {
            sumAB += dist[i][j];
            countAB++;
        }
        for (int j = i + 1; j < tau; j++) {
            sumAA += dist[i][j];
            countAA++;
        }
    }
    for (int i = tau; i < end; i++) {
        for (int j = i + 1; j < end; j++) {
            sumBB += dist[i][j];
            countBB++;
        }
    }

    double meanAB = countAB > 0 ? sumAB / countAB : 0;
    double meanAA = countAA > 0 ? sumAA / countAA : 0;
    double meanBB = countBB > 0 ? sumBB / countBB : 0;

    return ((double) nA * nB) / (nA + nB) * (2 * meanAB - meanAA - meanBB);
}
```

#### Step 3: Find Best Split Point

Iterate all candidate split points (respecting `minSize`) and find the one that maximizes the E-statistic.

```java
private static int findBestSplit(double[][] dist, int start, int end, int minSize) {
    double maxE = -1;
    int bestTau = -1;
    for (int tau = start + minSize; tau <= end - minSize; tau++) {
        double e = computeEStatistic(dist, start, tau, end);
        if (e > maxE) {
            maxE = e;
            bestTau = tau;
        }
    }
    return bestTau;
}
```

#### Step 4: Permutation Test

To assess significance, shuffle the data within the segment [start, end), recompute the max E-statistic, and count how often the permuted E-statistic exceeds the original.

```java
private static double permutationTest(
        double[] values, double[][] dist, int start, int end,
        int minSize, int permutations, double observedE) {

    Random rng = new Random(42); // deterministic for reproducibility
    int exceedCount = 0;

    for (int p = 0; p < permutations; p++) {
        // Create permuted distance matrix for this segment
        int[] indices = IntStream.range(start, end).toArray();
        shuffle(indices, rng);

        double maxPermE = -1;
        for (int tau = start + minSize; tau <= end - minSize; tau++) {
            double e = computeEStatisticPermuted(dist, indices, start, tau, end);
            maxPermE = Math.max(maxPermE, e);
        }
        if (maxPermE >= observedE) {
            exceedCount++;
        }
    }

    return (double) (exceedCount + 1) / (permutations + 1);
}
```

#### Step 5: Recursive Detection

```java
private static void detectRecursive(
        double[] values, double[][] dist,
        int start, int end,
        double significance, int minSize, int permutations,
        List<ChangePoint> results) {

    if (end - start < 2 * minSize) return;

    int bestTau = findBestSplit(dist, start, end, minSize);
    if (bestTau < 0) return;

    double observedE = computeEStatistic(dist, start, bestTau, end);
    double pValue = permutationTest(values, dist, start, end, minSize, permutations, observedE);

    if (pValue < significance) {
        // Compute segment statistics
        double beforeMean = Arrays.stream(values, start, bestTau).average().orElse(0);
        double afterMean = Arrays.stream(values, bestTau, end).average().orElse(0);
        double percentChange = beforeMean != 0 ? 100.0 * (afterMean - beforeMean) / beforeMean : 0;

        results.add(new ChangePoint(bestTau, beforeMean, afterMean, percentChange, pValue));

        // Recurse on both segments
        detectRecursive(values, dist, start, bestTau, significance, minSize, permutations, results);
        detectRecursive(values, dist, bestTau, end, significance, minSize, permutations, results);
    }
}
```

### Performance Considerations

- **Time complexity:** O(n^2) for the distance matrix, O(n * permutations) for the permutation test per candidate change point. Total: O(n^2 * permutations) per recursive level.
- **Space:** O(n^2) for the distance matrix.
- **Mitigation:** The cumulative Work flag means eDivisive only re-runs when new data arrives. For very long series (>10,000 points), consider downsampling or windowing.
- **commons-math3 usage:** `SummaryStatistics` for mean calculations (already imported in NodeService).

## Calculation: calculateEDivisiveValues()

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/svc/NodeService.java`

### Dispatch Integration

Add `"ed"` case to `calculateValues()`:

```java
case "ed":
    EDivisive edivisive = (EDivisive) node;
    for (int rIdx = 0; rIdx < roots.size(); rIdx++) {
        Value root = roots.get(rIdx);
        List<Value> found = calculateEDivisiveValues(edivisive, root, rtrn.size());
        rtrn.addAll(found);
    }
    break;
```

### Method

```java
@Transactional
public List<Value> calculateEDivisiveValues(EDivisive ed, Value root, int startingOrdinal) {
    List<Value> rtrn = new ArrayList<>();

    List<Value> fingerprintValues = valueService.getDescendantValues(root, ed.getFingerprintNode());

    for (Value fingerprintValue : fingerprintValues) {
        // Get all range values matching this fingerprint, ordered by domain
        List<Value> rangeValues = valueService.findMatchingFingerprint(
            ed.getRangeNode(), ed.getGroupByNode(), fingerprintValue, ed.getRangeNode()
        );

        // Convert to double array
        double[] series = rangeValues.stream()
            .map(v -> extractNumericOrNull(v))
            .filter(Objects::nonNull)
            .mapToDouble(Double::doubleValue)
            .toArray();

        if (series.length < 2 * ed.getMinSize()) {
            continue; // Not enough data
        }

        // Run eDivisive
        List<EDivisiveAlgorithm.ChangePoint> changePoints =
            EDivisiveAlgorithm.detectChangePoints(
                series, ed.getSignificance(), ed.getMinSize(), ed.getPermutations()
            );

        // Create a change Value for each detected change point
        for (EDivisiveAlgorithm.ChangePoint cp : changePoints) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode data = mapper.createObjectNode();
            data.set("changePointIndex", new IntNode(cp.index()));
            data.set("beforeMean", new DoubleNode(cp.beforeMean()));
            data.set("afterMean", new DoubleNode(cp.afterMean()));
            data.set("percentChange", new DoubleNode(cp.percentChange()));
            data.set("pValue", new DoubleNode(cp.pValue()));
            data.set("significance", new DoubleNode(ed.getSignificance()));
            data.set("fingerprint", fingerprintValue.data);

            Value changeValue = new Value(root.folder, ed, data);
            changeValue.idx = startingOrdinal++;
            List<Value> parents = valueService.getAncestor(fingerprintValue, ed.getGroupByNode());
            if (parents.size() == 1) {
                changeValue.sources = parents;
            }
            rtrn.add(changeValue);

            // Record change for tracking (Phase 2)
            changeService.recordChange(ed, changeValue, root);
        }
    }
    return rtrn;
}
```

## Work Integration

**File:** `h5m-core/src/main/java/io/hyperfoil/tools/h5m/entity/Work.java`

EDivisive must be cumulative (it analyzes the full series each time):

```java
if (activeNode instanceof RelativeDifference
    || activeNode instanceof FixedThreshold
    || activeNode instanceof EDivisive) {
    this.cumulative = true;
}
```

## CLI Command: AddEDivisive

**File:** `h5m/src/main/java/io/hyperfoil/tools/h5m/cli/AddEDivisive.java`

```java
@CommandLine.Command(name = "edivisive", separator = " ",
    description = "add an eDivisive change point detection node",
    mixinStandardHelpOptions = true)
public class AddEDivisive implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", arity = "1", description = "node name")
    String name;

    @CommandLine.Option(names = {"to"}, description = "target group / test")
    String groupName;

    @CommandLine.Option(names = {"range"}, arity = "1",
        description = "node producing values to analyze")
    String rangeName;

    @CommandLine.Option(names = {"by"}, arity = "0..1", description = "grouping node")
    String groupBy;

    @CommandLine.Option(names = {"fingerprint"}, description = "node names for fingerprint")
    List<String> fingerprints;

    @CommandLine.Option(names = {"significance"}, arity = "0..1",
        description = "p-value threshold", defaultValue = "0.05")
    double significance;

    @CommandLine.Option(names = {"minSize"}, arity = "0..1",
        description = "minimum segment size", defaultValue = "5")
    int minSize;

    @CommandLine.Option(names = {"permutations"}, arity = "0..1",
        description = "number of permutations", defaultValue = "199")
    int permutations;

    @Inject NodeGroupService nodeGroupService;
    @Inject NodeService nodeService;
}
```

**Usage:**
```
h5m add edivisive cpDetector to myTest range throughput by root fingerprint benchmarkName significance 0.01 minSize 10 permutations 499
```

**Registration:** Add `AddEDivisive.class` to `AddCmd`'s `subcommands` array.

Node creation follows the same pattern as AddRelativeDifference:
1. Validate group, resolve nodes
2. Create FingerprintNode (`_fp-cpDetector`)
3. Create EDivisive with sources [fingerprint, groupBy, range]
4. Set significance, minSize, permutations
5. Persist

## Files Modified/Created

| File | Action |
|------|--------|
| `h5m-core/.../entity/node/EDivisive.java` | **Create** |
| `h5m-core/.../changedetection/EDivisiveAlgorithm.java` | **Create** |
| `h5m-core/.../svc/NodeService.java` | **Modify** -- add `"ed"` case + `calculateEDivisiveValues()` |
| `h5m-core/.../entity/Work.java` | **Modify** -- recognize EDivisive as cumulative |
| `h5m/src/.../cli/AddEDivisive.java` | **Create** |
| `h5m/src/.../cli/AddCmd.java` | **Modify** -- add AddEDivisive to subcommands |

## Tests

### Algorithm Unit Tests

**File:** `h5m-core/src/test/java/io/hyperfoil/tools/h5m/changedetection/EDivisiveAlgorithmTest.java`

These test the pure algorithm without any JPA/CDI dependencies.

1. **Single change point:** Series `[1,1,1,1,1,5,5,5,5,5]`. Assert one change point detected at index 5. Assert beforeMean ~1.0, afterMean ~5.0, percentChange ~400%.

2. **No change point:** Series `[1,1,1,1,1,1,1,1,1,1]`. Assert empty result (no change detected).

3. **Multiple change points:** Series `[1,1,1,1,1,5,5,5,5,5,1,1,1,1,1]`. Assert two change points at indices ~5 and ~10.

4. **minSize enforcement:** Series `[1,1,5,5,5,5,5,5,5,5]` with minSize=5. Change point at index 2 should be rejected (left segment too small). No change detected.

5. **Gradual drift (no change):** Series `[1.0, 1.1, 1.2, 1.3, ..., 2.0]`. Assert no change point (smooth gradient, not a step change).

6. **Noisy data with clear change:** Series of N(0,1) followed by N(5,1). Assert change point detected near the transition.

7. **Significance threshold:** Same series tested with significance=0.001 (stricter) should detect fewer change points than significance=0.1.

### Integration Tests

**File:** `h5m-core/src/test/java/io/hyperfoil/tools/h5m/svc/NodeServiceTest.java`

1. **eDivisive detects change in fingerprinted series:** Set up EDivisive node with fingerprint/groupBy/range. Upload 20 values (10 at level 1, 10 at level 5). Assert change Value is created with changePointIndex ~10.

2. **eDivisive creates Change entity (Phase 2):** Same setup, verify Change entity exists in database after calculation.

## Recalculation Behavior

This section addresses how eDivisive interacts with the recalculation pipeline (see [#12](https://github.com/Hyperfoil/h5m/issues/12)). eDivisive has unique recalculation challenges compared to FixedThreshold and RelativeDifference because its output depends on the *entire series*, not just individual values or a sliding window.

### Output determinism

With a fixed RNG seed (`Random(42)`), `EDivisiveAlgorithm.detectChangePoints()` is **fully deterministic** for the same input array. Same series -> same change points -> same Value `data` -> `data.equals()` passes in WorkRunner -> no unnecessary downstream recalculation.

This satisfies issue #12's requirement: "a node change that does not change the resulting value does not cause recalculation for dependent nodes."

### When new data is added

When a new data point is uploaded, the entire series changes. eDivisive re-analyzes the full series and may:

1. **Detect the same change points at the same indices** -> output unchanged, no downstream work
2. **Detect the same change points at shifted indices** -> output data changes (`changePointIndex` differs), downstream work is triggered
3. **Detect new change points** -> new Values are created
4. **Stop detecting old change points** -> old Values are deleted by WorkRunner

Case 2 is the most interesting. Adding a single data point at the end of the series shouldn't shift existing change points (they're at the same absolute positions). But adding data in the middle (unlikely but possible with out-of-order uploads) could shift indices.

### Multi-value correlation challenge

eDivisive can detect multiple change points per fingerprint, producing multiple Values. The correlation issue from issue #12 ("a node change that creates multiple values correctly correlates those values with existing values") is relevant here.

Consider a series that initially has one change point (producing one Value with idx=0), and after new data is added, has two change points (producing two Values with idx=0 and idx=1):

- WorkRunner matches the new idx=0 with the existing idx=0 by path
- If the data differs (different `changePointIndex`, `beforeMean`, etc.) -> existing Value is updated
- New idx=1 has no matching existing Value -> persisted as new

This works correctly because `idx` assignment order is stable: change points are sorted by index before Value creation.

However, if a change point *disappears* and another *appears*:
- Old: change point at index 5 (idx=0)
- New: change point at index 15 (idx=0)

WorkRunner matches by path (both idx=0), sees different data, and updates. This is functionally correct (old change point is replaced) but semantically misleading (it looks like the index-5 change point moved to index 15 when actually it disappeared and a new one appeared).

**Mitigation:** Include the `changePointIndex` in the Value path computation to enable more precise matching. This would require a custom `getPath()` override or embedding the change point index in the `idx` field. For v1, the simpler approach (sequential idx) is acceptable since the data content accurately describes each change point regardless of matching.

### Interaction with Change entity (Phase 2)

eDivisive's Change entity behavior during recalculation uses the fingerprint-based matching from Phase 2's design. Since eDivisive can produce multiple change points per fingerprint, the matching key needs to include the change point index:

```
Change identity = (detectionNode, fingerprint, changePointIndex)
```

When recalculation produces a change point at a slightly different index (e.g., index 5 shifts to index 6 after adding data), the Change matching has two options:

1. **Exact match:** Treat index 5 and index 6 as different changes. Old Change (index 5) is orphaned and cleaned up. New Change (index 6) is created with `confirmed=null`. User triage is lost.

2. **Fuzzy match:** Consider change points within a tolerance window (e.g., +/- 2 indices) as the same change. Preserves triage state across minor index shifts.

**Recommendation:** Start with exact match (simpler). If users report annoyance at losing triage state when change points shift by 1-2 indices, add fuzzy matching later. The `cleanupOrphanedChanges()` mechanism from Phase 2 handles the orphaned Changes.

### Performance during recalculation

Since eDivisive is O(n^2 * permutations), recalculation is expensive for long series. The cumulative Work flag ensures it only runs when data changes, but `FolderService.recalculate()` forces all nodes to recalculate.

**Mitigation:** During bulk recalculation, the WorkRunner equality check provides a natural optimization. If the eDivisive node's source nodes haven't changed (their Values are equal), the eDivisive input series is the same, producing the same output. The equality check in WorkRunner (line 91) catches this and skips downstream work.

For series that *have* changed, the full O(n^2) cost is unavoidable. This is acceptable because recalculation is a manual operation (`h5m recalculate`), not triggered on every upload.

### Tests for recalculation

Add to `NodeServiceTest.java`:

1. **eDivisive recalculation with same series:** Run eDivisive on a series with a known change point. Re-run on the same series. Assert the output Values have identical `data` (ensuring WorkRunner's equality check passes and no downstream work is triggered).

2. **eDivisive recalculation with extended series:** Run on [1,1,1,1,1,5,5,5,5,5]. Then run on [1,1,1,1,1,5,5,5,5,5,5]. Assert the change point is still detected at index 5 (same position). Assert the Value `data` is identical (same `changePointIndex`, `beforeMean`, `afterMean`).

3. **eDivisive recalculation with regression fixed:** Run on [1,1,1,1,1,5,5,5,5,5] (change point detected). Then run on [1,1,1,1,1,1,1,1,1,1] (no change point). Assert the old change Value is no longer produced, and WorkRunner would delete it.

4. **eDivisive recalculation with new change point appearing:** Run on [1,1,1,1,1,1,1,1,1,1] (no change). Then run on [1,1,1,1,1,5,5,5,5,5] (change at index 5). Assert a new change Value is produced.

## Open Questions

1. **Deterministic permutation tests:** Using a fixed seed (`Random(42)`) ensures reproducible results. Should this be configurable, or is reproducibility always preferred?

2. **Performance for large series:** For series > 5000 points, the O(n^2) distance matrix becomes expensive. Should we:
   - Add a `maxSeries` config to cap the number of data points analyzed (most recent N)?
   - Use a more efficient distance computation (e.g., only computing distances needed for each candidate split)?
   - Log a warning when series exceeds a threshold?

3. **Change point stability:** eDivisive may detect slightly different change points when new data is added (the same historical change point might shift by 1-2 indices). Should we implement change point merging/deduplication in `recordChange()` (e.g., if a change point is within N indices of an existing one, update rather than create)?

4. **Dependency on Phase 2:** The `changeService.recordChange()` call requires Phase 2's Change entity. If Phase 2 isn't complete, should the eDivisive calculation still produce Values but skip Change recording? This can be handled by making `changeService` injection optional or null-safe.
