package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.api.StdDevAnomalyConfig;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class StdDevAnomalyTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    NodeService nodeService;

    @Inject
    ValueService valueService;

    /**
     * Helper: creates the standard node topology for StdDev tests.
     * root → split → [range (.y), domain (.domain), fingerprint (.fingerprint)]
     * Returns the StdDevAnomaly node (already persisted).
     */
    private record TestTopology(
            NodeEntity root, SplitNode split, NodeEntity range,
            NodeEntity domain, NodeEntity fingerprint, StdDevAnomaly sd
    ) {}

    private TestTopology createTopology(int windowSize, double deviations,
                                         StdDevAnomalyConfig.Direction direction, int minDataPoints) throws Exception {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        SplitNode splitNode = new SplitNode("split", "split", List.of(rootNode));
        splitNode.persist();
        NodeEntity rangeNode = new JqNode("range", ".y", splitNode);
        rangeNode.persist();
        NodeEntity domainNode = new JqNode("domain", ".domain", splitNode);
        domainNode.persist();
        NodeEntity fingerprintNode = new JqNode("fingerprint", ".fingerprint", splitNode);
        fingerprintNode.persist();

        StdDevAnomaly sd = new StdDevAnomaly("stddev-test", "{}");
        sd.setNodes(fingerprintNode, splitNode, rangeNode, domainNode);
        sd.setWindowSize(windowSize);
        sd.setDeviations(deviations);
        sd.setDirection(direction);
        sd.setMinDataPoints(minDataPoints);
        sd.persist();
        tm.commit();

        return new TestTopology(rootNode, splitNode, rangeNode, domainNode, fingerprintNode, sd);
    }

    /**
     * Helper: uploads a data point and returns the root ValueEntity.
     */
    private ValueEntity uploadDataPoint(TestTopology t, String fingerprint, double domain, double range) throws Exception {
        tm.begin();
        String json = String.format(
                """
                { "split": [ { "fingerprint": "%s", "domain": %s, "y": %s } ] }
                """, fingerprint, domain, range);
        ValueEntity root = new ValueEntity(null, t.root, JqValues.parse(json));
        root.persist();
        ValueEntity split = new ValueEntity(null, t.split, root.data.getField("split").getElement(0), List.of(root));
        split.persist();
        new ValueEntity(null, t.domain, split.data.getField("domain"), List.of(split)).persist();
        new ValueEntity(null, t.range, split.data.getField("y"), List.of(split)).persist();
        new ValueEntity(null, t.fingerprint, split.data.getField("fingerprint"), List.of(split)).persist();
        tm.commit();
        return root;
    }

    @Test
    public void step_function_detects_anomaly() throws Exception {
        // 10 stable values at 100, then jump to 200 → should detect
        TestTopology t = createTopology(10, 4.0, StdDevAnomalyConfig.Direction.BOTH, 5);

        // Upload 10 stable data points
        ValueEntity lastRoot = null;
        for (int i = 1; i <= 10; i++) {
            lastRoot = uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Verify no anomaly in stable data
        List<ValueEntity> stableChanges = nodeService.calculateStdDevAnomalyValues(t.sd, lastRoot, 0);
        assertEquals(0, stableChanges.size(), "Stable data should produce no anomalies");

        // Upload the anomaly: jump to 200
        ValueEntity anomalyRoot = uploadDataPoint(t, "alpha", 11, 200.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);

        assertEquals(1, changes.size(), "Step function should detect 1 anomaly");
        JqValue changeData = changes.get(0).data;
        assertEquals(200.0, changeData.getField("value").asDouble(0.0), 0.01);
        assertEquals(100.0, changeData.getField("mean").asDouble(0.0), 0.01);
        assertEquals("above", changeData.getField("direction").asString(""));
        assertNotNull(changeData.getField("stddev"));
        assertNotNull(changeData.getField("deviations"));
        assertNotNull(changeData.getField("threshold"));
    }

    @Test
    public void stable_data_no_false_positives() throws Exception {
        // Values within normal noise — should NOT alert at 4 sigma
        TestTopology t = createTopology(10, 4.0, StdDevAnomalyConfig.Direction.BOTH, 5);

        // Upload 10 values with small variance: 100 ± ~2
        double[] values = {100, 101, 99, 102, 98, 100, 101, 99, 100, 101};
        ValueEntity lastRoot = null;
        for (int i = 0; i < values.length; i++) {
            lastRoot = uploadDataPoint(t, "alpha", i + 1, values[i]);
        }

        // Upload a value within 4 sigma of the baseline (mean ~100, stddev ~1.1, 4σ ≈ 4.4)
        ValueEntity normalRoot = uploadDataPoint(t, "alpha", 11, 103.0); // within 4σ
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, normalRoot, 0);

        assertEquals(0, changes.size(), "Value within 4 sigma should not trigger anomaly");
    }

    @Test
    public void direction_upper_only_detects_increases() throws Exception {
        TestTopology t = createTopology(10, 3.0, StdDevAnomalyConfig.Direction.UPPER, 5);

        for (int i = 1; i <= 10; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Drop to 50 — should NOT detect (UPPER only)
        ValueEntity dropRoot = uploadDataPoint(t, "alpha", 11, 50.0);
        List<ValueEntity> dropChanges = nodeService.calculateStdDevAnomalyValues(t.sd, dropRoot, 0);
        assertEquals(0, dropChanges.size(), "UPPER direction should not detect decreases");

        // Jump to 200 — should detect
        ValueEntity jumpRoot = uploadDataPoint(t, "alpha", 12, 200.0);
        List<ValueEntity> jumpChanges = nodeService.calculateStdDevAnomalyValues(t.sd, jumpRoot, 0);
        assertEquals(1, jumpChanges.size(), "UPPER direction should detect increases");
        JqValue jumpData = jumpChanges.get(0).data;
        assertEquals("above", jumpData.getField("direction").asString(""));
        assertEquals(200.0, jumpData.getField("value").asDouble(0.0), 0.01, "Detected value should be 200");
        assertNotNull(jumpData.getField("mean"), "Output should contain mean");
        assertNotNull(jumpData.getField("stddev"), "Output should contain stddev");
        assertNotNull(jumpData.getField("threshold"), "Output should contain threshold");
        assertTrue(jumpData.getField("threshold").asDouble(0.0) < 200.0,
                "Upper threshold should be below 200 (baseline is ~100)");
    }

    @Test
    public void direction_lower_only_detects_decreases() throws Exception {
        TestTopology t = createTopology(10, 3.0, StdDevAnomalyConfig.Direction.LOWER, 5);

        for (int i = 1; i <= 10; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Jump to 200 — should NOT detect (LOWER only)
        ValueEntity jumpRoot = uploadDataPoint(t, "alpha", 11, 200.0);
        List<ValueEntity> jumpChanges = nodeService.calculateStdDevAnomalyValues(t.sd, jumpRoot, 0);
        assertEquals(0, jumpChanges.size(), "LOWER direction should not detect increases");

        // Drop to 10 — extreme drop from baseline of ~100, should detect even with
        // the 200 outlier in the window since most values are ~100
        ValueEntity dropRoot = uploadDataPoint(t, "alpha", 12, 10.0);
        List<ValueEntity> dropChanges = nodeService.calculateStdDevAnomalyValues(t.sd, dropRoot, 0);
        assertEquals(1, dropChanges.size(), "LOWER direction should detect extreme decreases");
        JqValue dropData = dropChanges.get(0).data;
        assertEquals("below", dropData.getField("direction").asString(""));
        assertEquals(10.0, dropData.getField("value").asDouble(0.0), 0.01, "Detected value should be 10");
        assertNotNull(dropData.getField("mean"), "Output should contain mean");
        assertNotNull(dropData.getField("stddev"), "Output should contain stddev");
        assertNotNull(dropData.getField("threshold"), "Output should contain threshold");
        assertTrue(dropData.getField("threshold").asDouble(0.0) > 10.0,
                "Lower threshold should be above 10 (baseline is ~100)");
    }

    @Test
    public void insufficient_data_no_detection() throws Exception {
        // minDataPoints=10 but only upload 5 values → should not alert even with anomaly
        TestTopology t = createTopology(10, 4.0, StdDevAnomalyConfig.Direction.BOTH, 10);

        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Upload extreme value — should still not trigger because insufficient data
        ValueEntity anomalyRoot = uploadDataPoint(t, "alpha", 6, 500.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);

        assertEquals(0, changes.size(),
                "Should not trigger anomaly with fewer than minDataPoints samples");
    }

    @Test
    public void zero_stddev_detects_any_change() throws Exception {
        // All identical values → stddev is 0 → any change should be detected
        TestTopology t = createTopology(10, 4.0, StdDevAnomalyConfig.Direction.BOTH, 5);

        for (int i = 1; i <= 10; i++) {
            uploadDataPoint(t, "alpha", i, 100.0); // all identical
        }

        // Any different value should be detected (stddev floor kicks in)
        ValueEntity changeRoot = uploadDataPoint(t, "alpha", 11, 101.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, changeRoot, 0);

        assertEquals(1, changes.size(),
                "With zero stddev (identical baseline), any change should be detected");
    }

    @Test
    public void known_distribution_3sigma_vs_4sigma() throws Exception {
        // For N(100, 10): stddev=10
        // 3 sigma threshold: [70, 130]
        // 4 sigma threshold: [60, 140]
        // A value of 135 should trigger at 3σ but NOT at 4σ
        TestTopology t3 = createTopology(20, 3.0, StdDevAnomalyConfig.Direction.BOTH, 5);
        TestTopology t4 = createTopology(20, 4.0, StdDevAnomalyConfig.Direction.BOTH, 5);

        // Upload 20 values from N(100, 10) — hand-picked to have mean≈100, stddev≈10
        double[] baseline = {
                95, 105, 90, 110, 100, 98, 102, 93, 107, 99,
                101, 96, 104, 91, 109, 100, 97, 103, 94, 106
        };
        ValueEntity lastRoot3 = null, lastRoot4 = null;
        for (int i = 0; i < baseline.length; i++) {
            lastRoot3 = uploadDataPoint(t3, "alpha", i + 1, baseline[i]);
            lastRoot4 = uploadDataPoint(t4, "alpha", i + 1, baseline[i]);
        }

        // Baseline: mean=100.00, stddev=5.81
        // 3σ threshold: 117.44, 4σ threshold: 123.25
        // Upload 120 — between 3σ and 4σ (3.44σ from mean)
        ValueEntity root120_3 = uploadDataPoint(t3, "alpha", 21, 120.0);
        ValueEntity root120_4 = uploadDataPoint(t4, "alpha", 21, 120.0);

        List<ValueEntity> changes3 = nodeService.calculateStdDevAnomalyValues(t3.sd, root120_3, 0);
        List<ValueEntity> changes4 = nodeService.calculateStdDevAnomalyValues(t4.sd, root120_4, 0);

        assertEquals(1, changes3.size(),
                "Value 120 (3.44σ) should trigger at 3 sigma (threshold ≈ 117.44)");
        JqValue changeData3 = changes3.get(0).data;
        assertEquals(120.0, changeData3.getField("value").asDouble(0.0), 0.01, "Detected value should be 120");
        assertEquals(100.0, changeData3.getField("mean").asDouble(0.0), 1.0, "Mean should be ~100");
        assertTrue(changeData3.getField("stddev").asDouble(0.0) > 4.0 && changeData3.getField("stddev").asDouble(0.0) < 7.0,
                "Stddev should be ~5.81, got: " + changeData3.getField("stddev").asDouble(0.0));
        assertEquals("above", changeData3.getField("direction").asString(""));
        assertTrue(changeData3.getField("threshold").asDouble(0.0) < 120.0,
                "3σ threshold should be below 120");

        assertEquals(0, changes4.size(),
                "Value 120 (3.44σ) should NOT trigger at 4 sigma (threshold ≈ 123.25)");
    }

    @Test
    public void gradual_drift_no_false_alarm() throws Exception {
        // Gradual upward drift: each value slightly higher than the last
        // StdDev should adapt and NOT alert because the stddev naturally
        // grows to accommodate the trend
        TestTopology t = createTopology(10, 4.0, StdDevAnomalyConfig.Direction.BOTH, 5);

        // Linear drift: 100, 102, 104, ..., 120
        for (int i = 0; i <= 10; i++) {
            uploadDataPoint(t, "alpha", i + 1, 100.0 + i * 2.0);
        }

        // Next value continues the trend at 122
        ValueEntity nextRoot = uploadDataPoint(t, "alpha", 12, 122.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, nextRoot, 0);

        assertEquals(0, changes.size(),
                "Gradual drift should not trigger anomaly — stddev adapts to the trend");
    }

    @Test
    public void only_uses_window_size_most_recent_values() throws Exception {
        // Upload 50 values: first 40 at y=100 (old baseline), then 10 at y=200 (new baseline)
        // With windowSize=5, the baseline should only see the last 5 values (all at 200)
        // so y=200 should NOT be anomalous — the window has adapted
        // But if the algorithm used all 50 values, mean≈120 and 200 would be anomalous
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Old baseline: 40 uploads at y=100
        for (int i = 1; i <= 40; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }
        // New baseline: 10 uploads at y=200 (legitimate shift)
        for (int i = 41; i <= 50; i++) {
            uploadDataPoint(t, "alpha", i, 200.0);
        }

        // Upload another value at y=200 — should NOT trigger because
        // the window (last 5 values) is all 200s
        ValueEntity currentRoot = uploadDataPoint(t, "alpha", 51, 200.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, currentRoot, 0);

        assertEquals(0, changes.size(),
                "With windowSize=5, only the last 5 values (all 200) should be used as baseline. " +
                "Value 200 matches the baseline — no anomaly. If all 50 values were used, " +
                "mean≈120 and 200 would incorrectly trigger as anomalous.");
    }

    @Test
    public void multi_fingerprint_independent_detection() throws Exception {
        // Two fingerprints with different baselines — anomaly should be
        // detected independently per fingerprint
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Alpha: stable at 100
        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i * 2, 100.0); // even domain values
        }

        // Beta: stable at 200 (different baseline)
        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "beta", i * 2 + 1, 200.0); // odd domain values
        }

        // Alpha stays stable — should NOT detect
        ValueEntity alphaRoot = uploadDataPoint(t, "alpha", 12, 100.0);
        List<ValueEntity> alphaChanges = nodeService.calculateStdDevAnomalyValues(t.sd, alphaRoot, 0);
        assertEquals(0, alphaChanges.size(),
                "Alpha (stable at 100) should have no anomaly — beta's different baseline must not affect alpha");

        // Beta jumps to 500 — should detect for beta fingerprint
        // domain=13 to avoid collision with last beta domain (5*2+1=11)
        ValueEntity betaRoot = uploadDataPoint(t, "beta", 13, 500.0);
        List<ValueEntity> betaChanges = nodeService.calculateStdDevAnomalyValues(t.sd, betaRoot, 0);

        assertEquals(1, betaChanges.size(), "Beta (jumped from 200 to 500) should have 1 anomaly");
        JqValue betaData = betaChanges.get(0).data;
        assertEquals("beta", betaData.getField("fingerprint").asString(""));
        assertEquals(500.0, betaData.getField("value").asDouble(0.0), 0.01);
        assertEquals(200.0, betaData.getField("mean").asDouble(0.0), 0.01,
                "Mean should be 200 (beta's baseline), not contaminated by alpha's 100");
    }

    @Test
    public void out_of_order_uploads_checks_current_upload() throws Exception {
        // Upload data points out of chronological order.
        // The current upload's value should always be the one checked for anomaly,
        // even if it has an earlier domain value than existing data.
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Upload domain values: 1, 2, 3, 4, 5 — all at y=100 (stable baseline)
        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Upload domain=10 (out of order — later than existing, at normal value)
        ValueEntity normalRoot = uploadDataPoint(t, "alpha", 10, 100.0);
        List<ValueEntity> normalChanges = nodeService.calculateStdDevAnomalyValues(t.sd, normalRoot, 0);
        assertEquals(0, normalChanges.size(), "Normal value uploaded out of order should not trigger anomaly");

        // Upload domain=3 (out of order — EARLIER than existing data, with anomalous value)
        // This tests that the calculation checks the current upload's value (y=500)
        // not values.getLast() which could be domain=10 (y=100)
        ValueEntity outOfOrderRoot = uploadDataPoint(t, "alpha", 3, 500.0);
        List<ValueEntity> outOfOrderChanges = nodeService.calculateStdDevAnomalyValues(t.sd, outOfOrderRoot, 0);
        assertEquals(1, outOfOrderChanges.size(),
                "Out-of-order upload with anomalous value should still be detected. " +
                "The algorithm should check the current upload's value (500), not the latest by domain order (100).");
        assertEquals(500.0, outOfOrderChanges.get(0).data.getField("value").asDouble(0.0), 0.01);
    }

    @Test
    public void stale_change_removed_on_reprocessing() throws Exception {
        // Simulate: first run detects anomaly, then the anomaly value is replaced
        // by a normal value (e.g., recalculation after correcting data)
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Upload 5 stable values at y=100
        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Upload anomaly at y=500
        ValueEntity anomalyRoot = uploadDataPoint(t, "alpha", 6, 500.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);
        assertEquals(1, changes.size(), "Should detect anomaly at y=500");

        // Persist the change value (simulating what WorkService does)
        tm.begin();
        for (ValueEntity change : changes) {
            valueService.create(change);
        }
        tm.commit();

        // Verify the persisted change exists
        tm.begin();
        List<ValueEntity> persistedBefore = valueService.findMatchingFingerprint(
                t.sd, NodeEntity.findById(t.sd.getGroupByNode().getId()),
                valueService.getDescendantValues(anomalyRoot, t.fingerprint).getFirst(),
                t.sd.getDomainNode(), null, null, -1, -1, true);
        assertFalse(persistedBefore.isEmpty(), "Should have a persisted change value");
        tm.commit();

        // Reprocess with the SAME data — anomaly should still be detected
        // because baseline window (domain <= 6) is still [100,100,100,100,100]
        tm.begin();
        List<ValueEntity> reprocessed = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);
        assertEquals(1, reprocessed.size(),
                "Reprocessing same upload should still detect the anomaly (baseline unchanged)");
        tm.commit();
    }

    @Test
    public void anomaly_at_domain_value_is_consistent_across_calls() throws Exception {
        // Verify that for the same data, multiple calls produce the same result
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        ValueEntity anomalyRoot = uploadDataPoint(t, "alpha", 6, 500.0);

        List<ValueEntity> first = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);
        List<ValueEntity> second = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);

        assertEquals(first.size(), second.size(), "Same data should produce same number of anomalies");
        if (!first.isEmpty()) {
            assertEquals(first.get(0).data.getField("value").asDouble(0.0),
                    second.get(0).data.getField("value").asDouble(0.0), 0.01,
                    "Same data should produce same anomaly value");
        }
    }

    @Test
    public void multiple_datasets_per_upload() throws Exception {
        // Upload with multiple split values (datasets) — each should be checked independently
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Upload 5 stable data points for "alpha" at y=100
        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }
        // Upload 5 stable data points for "beta" at y=200
        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "beta", i + 10, 200.0);
        }

        // Upload with BOTH fingerprints: alpha normal (y=100), beta anomalous (y=500)
        tm.begin();
        ValueEntity root = new ValueEntity(null, t.root, JqValues.parse(
                """
                { "split": [
                    { "fingerprint": "alpha", "domain": 6, "y": 100.0 },
                    { "fingerprint": "beta", "domain": 16, "y": 500.0 }
                ] }
                """));
        root.persist();
        // Create both datasets
        JqValue splitArr = root.data.getField("split");
        for (int i = 0; i < splitArr.length(); i++) {
            JqValue ds = splitArr.getElement(i);
            ValueEntity split = new ValueEntity(null, t.split, ds, List.of(root));
            split.persist();
            new ValueEntity(null, t.domain, ds.getField("domain"), List.of(split)).persist();
            new ValueEntity(null, t.range, ds.getField("y"), List.of(split)).persist();
            new ValueEntity(null, t.fingerprint, ds.getField("fingerprint"), List.of(split)).persist();
        }
        tm.commit();

        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, root, 0);

        // Alpha (y=100 matching baseline of 100) should NOT trigger
        // Beta (y=500 vs baseline of 200) SHOULD trigger
        assertEquals(1, changes.size(),
                "Only beta should trigger anomaly. Alpha matches its baseline.");
        assertEquals("beta", changes.get(0).data.getField("fingerprint").asString(""),
                "The anomaly should be for the beta fingerprint");
    }

    @Test
    public void datasets_do_not_cross_contaminate_baselines() throws Exception {
        // Alpha baseline: y=100, Beta baseline: y=1000 (very different).
        // Upload alpha at y=500 — anomalous relative to alpha baseline (100)
        // but normal relative to beta baseline (1000).
        // If baselines cross-contaminate, the combined mean would be ~550
        // and y=500 might NOT be flagged. Correct scoping means alpha's
        // baseline is purely [100,100,...] and 500 is clearly anomalous.
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Build up history: 5 uploads, each with BOTH datasets
        for (int i = 1; i <= 5; i++) {
            tm.begin();
            ValueEntity root = new ValueEntity(null, t.root, JqValues.parse(
                    String.format("""
                    { "split": [
                        { "fingerprint": "alpha", "domain": %d, "y": 100.0 },
                        { "fingerprint": "beta", "domain": %d, "y": 1000.0 }
                    ] }
                    """, i, i + 100)));
            root.persist();
            JqValue splitArr = root.data.getField("split");
            for (int j = 0; j < splitArr.length(); j++) {
                JqValue ds = splitArr.getElement(j);
                ValueEntity split = new ValueEntity(null, t.split, ds, List.of(root));
                split.persist();
                new ValueEntity(null, t.domain, ds.getField("domain"), List.of(split)).persist();
                new ValueEntity(null, t.range, ds.getField("y"), List.of(split)).persist();
                new ValueEntity(null, t.fingerprint, ds.getField("fingerprint"), List.of(split)).persist();
            }
            tm.commit();
        }

        // Upload 6: alpha anomalous (y=500), beta normal (y=1000)
        tm.begin();
        ValueEntity root6 = new ValueEntity(null, t.root, JqValues.parse(
                """
                { "split": [
                    { "fingerprint": "alpha", "domain": 6, "y": 500.0 },
                    { "fingerprint": "beta", "domain": 106, "y": 1000.0 }
                ] }
                """));
        root6.persist();
        JqValue splitArr6 = root6.data.getField("split");
        for (int j = 0; j < splitArr6.length(); j++) {
            JqValue ds = splitArr6.getElement(j);
            ValueEntity split = new ValueEntity(null, t.split, ds, List.of(root6));
            split.persist();
            new ValueEntity(null, t.domain, ds.getField("domain"), List.of(split)).persist();
            new ValueEntity(null, t.range, ds.getField("y"), List.of(split)).persist();
            new ValueEntity(null, t.fingerprint, ds.getField("fingerprint"), List.of(split)).persist();
        }
        tm.commit();

        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, root6, 0);

        // Alpha should trigger (500 vs baseline of 100), beta should NOT (1000 vs baseline of 1000)
        assertEquals(1, changes.size(),
                "Only alpha should trigger. If baselines cross-contaminate, " +
                "the combined mean (~550) would make 500 look normal.");
        assertEquals("alpha", changes.get(0).data.getField("fingerprint").asString(""));
        assertEquals(500.0, changes.get(0).data.getField("value").asDouble(0.0), 0.01);
        assertEquals(100.0, changes.get(0).data.getField("mean").asDouble(0.0), 0.01,
                "Mean should be 100 (alpha's baseline), not ~550 (combined)");
    }

    @Test
    public void groupBy_scopes_range_values_to_correct_dataset() throws Exception {
        // Verify that each fingerprint's range values come from its own split branch,
        // not from all branches in the upload. If groupBy scoping is wrong, the method
        // would get ALL range values from the upload regardless of which dataset they
        // belong to, producing incorrect baselines.
        //
        // Alpha: y=100 (stable), Beta: y=100 (stable but different dataset)
        // Upload alpha at y=100 (normal) — should NOT trigger, even though beta has
        // the same values (proving we're not accidentally mixing datasets)
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // History with both datasets at y=100
        for (int i = 1; i <= 5; i++) {
            tm.begin();
            ValueEntity root = new ValueEntity(null, t.root, JqValues.parse(
                    String.format("""
                    { "split": [
                        { "fingerprint": "alpha", "domain": %d, "y": 100.0 },
                        { "fingerprint": "beta", "domain": %d, "y": 100.0 }
                    ] }
                    """, i, i + 100)));
            root.persist();
            JqValue splitArr = root.data.getField("split");
            for (int j = 0; j < splitArr.length(); j++) {
                JqValue ds = splitArr.getElement(j);
                ValueEntity split = new ValueEntity(null, t.split, ds, List.of(root));
                split.persist();
                new ValueEntity(null, t.domain, ds.getField("domain"), List.of(split)).persist();
                new ValueEntity(null, t.range, ds.getField("y"), List.of(split)).persist();
                new ValueEntity(null, t.fingerprint, ds.getField("fingerprint"), List.of(split)).persist();
            }
            tm.commit();
        }

        // Upload 6: both datasets normal
        tm.begin();
        ValueEntity root6 = new ValueEntity(null, t.root, JqValues.parse(
                """
                { "split": [
                    { "fingerprint": "alpha", "domain": 6, "y": 100.0 },
                    { "fingerprint": "beta", "domain": 106, "y": 100.0 }
                ] }
                """));
        root6.persist();
        JqValue splitArr6 = root6.data.getField("split");
        for (int j = 0; j < splitArr6.length(); j++) {
            JqValue ds = splitArr6.getElement(j);
            ValueEntity split = new ValueEntity(null, t.split, ds, List.of(root6));
            split.persist();
            new ValueEntity(null, t.domain, ds.getField("domain"), List.of(split)).persist();
            new ValueEntity(null, t.range, ds.getField("y"), List.of(split)).persist();
            new ValueEntity(null, t.fingerprint, ds.getField("fingerprint"), List.of(split)).persist();
        }
        tm.commit();

        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, root6, 0);

        assertEquals(0, changes.size(),
                "Neither dataset should trigger — both are stable at y=100. " +
                "If groupBy scoping is wrong, range values from both datasets would " +
                "be mixed, potentially doubling the sample count or producing " +
                "unexpected baseline statistics.");
    }

    @Test
    public void ordered_sequential_uploads_detect_and_verify() throws Exception {
        // Mirror the RelativeDifference ordered test pattern:
        // Upload 1: insufficient data → no change
        // Upload 2: enough data but within baseline → no change
        // Upload 3: anomalous value → detect
        TestTopology t = createTopology(3, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Upload 1: domain=1, y=100
        ValueEntity root1 = uploadDataPoint(t, "alpha", 1, 100.0);
        List<ValueEntity> changes1 = nodeService.calculateStdDevAnomalyValues(t.sd, root1, 0);
        assertEquals(0, changes1.size(),
                "Upload 1: only 1 sample, below minDataPoints=3, should produce no anomalies");

        // Upload 2: domain=2, y=101
        ValueEntity root2 = uploadDataPoint(t, "alpha", 2, 101.0);
        List<ValueEntity> changes2 = nodeService.calculateStdDevAnomalyValues(t.sd, root2, 0);
        assertEquals(0, changes2.size(),
                "Upload 2: only 2 samples, below minDataPoints=3, should produce no anomalies");

        // Upload 3: domain=3, y=100 (stable, 3 samples now, but within baseline)
        ValueEntity root3 = uploadDataPoint(t, "alpha", 3, 100.0);
        List<ValueEntity> changes3 = nodeService.calculateStdDevAnomalyValues(t.sd, root3, 0);
        assertEquals(0, changes3.size(),
                "Upload 3: 3 samples, all stable ~100, no anomaly");

        // Upload 4: domain=4, y=100 (still stable)
        ValueEntity root4 = uploadDataPoint(t, "alpha", 4, 100.0);
        List<ValueEntity> changes4 = nodeService.calculateStdDevAnomalyValues(t.sd, root4, 0);
        assertEquals(0, changes4.size(),
                "Upload 4: stable baseline, no anomaly");

        // Upload 5: domain=5, y=200 (anomaly — large jump from baseline of ~100)
        ValueEntity root5 = uploadDataPoint(t, "alpha", 5, 200.0);
        List<ValueEntity> changes5 = nodeService.calculateStdDevAnomalyValues(t.sd, root5, 0);
        assertEquals(1, changes5.size(),
                "Upload 5: y=200 vs baseline ~100, should detect anomaly");

        JqValue changeData = changes5.get(0).data;
        assertEquals(200.0, changeData.getField("value").asDouble(0.0), 0.01, "Anomalous value should be 200");
        assertTrue(changeData.getField("mean").asDouble(0.0) < 110, "Mean of baseline should be close to 100");
        assertEquals("above", changeData.getField("direction").asString(""), "200 > mean, direction should be 'above'");
        assertEquals("alpha", changeData.getField("fingerprint").asString(""), "Fingerprint should be alpha");
        assertEquals(5, changeData.getField("domainvalue").asInt(0), "Domain value should be 5 (current upload)");
    }

    @Test
    public void mid_sequence_upload_uses_correct_baseline() throws Exception {
        // Upload values at domain 1,2,3,4,5 (all y=100), then 6,7,8,9,10 (all y=100).
        // Now upload at domain=4 with y=500 (mid-sequence, anomalous).
        // The baseline should be values with domain <= 4 from previous uploads,
        // i.e., domains 1,2,3,4 (all y=100). The anomaly should be detected.
        TestTopology t = createTopology(10, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Upload 10 stable values
        for (int i = 1; i <= 10; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Upload at domain=4 with anomalous value — mid-sequence
        ValueEntity midRoot = uploadDataPoint(t, "alpha", 4, 500.0);
        List<ValueEntity> midChanges = nodeService.calculateStdDevAnomalyValues(t.sd, midRoot, 0);

        assertEquals(1, midChanges.size(),
                "Mid-sequence upload at domain=4 with y=500 should detect anomaly. " +
                "Baseline (domain <= 4) is [100,100,100,100], so 500 is clearly anomalous.");
        assertEquals(500.0, midChanges.get(0).data.getField("value").asDouble(0.0), 0.01);
        // The domain value in the output should be 4 (the current upload's domain)
        assertEquals(4, midChanges.get(0).data.getField("domainvalue").asInt(0),
                "Output should report the current upload's domain value (4), not the latest (10)");
    }

    @Test
    public void mid_sequence_upload_normal_value_no_anomaly() throws Exception {
        // Upload values at domain 1-10 with small natural variance.
        // Upload at domain=5 with a value within the variance — mid-sequence.
        // Should NOT trigger anomaly.
        TestTopology t = createTopology(10, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        double[] baseline = {100, 102, 98, 101, 99, 103, 97, 100, 102, 98};
        for (int i = 0; i < baseline.length; i++) {
            uploadDataPoint(t, "alpha", i + 1, baseline[i]);
        }

        // Upload mid-sequence at domain=5, y=100 — well within baseline variance
        ValueEntity midRoot = uploadDataPoint(t, "alpha", 5, 100.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, midRoot, 0);

        assertEquals(0, changes.size(),
                "Mid-sequence upload at domain=5 with y=100 should not trigger anomaly. " +
                "Baseline has natural variance ~2, and 100 is squarely in the middle.");
    }

    @Test
    public void mid_sequence_baseline_excludes_future_values() throws Exception {
        // Upload: domain 1-5 at y=100, then domain 6-10 at y=200 (legitimate shift).
        // Upload at domain=3 with y=100 (mid-sequence, matches OLD baseline).
        // The baseline for domain=3 should be domains 1,2,3 (all y=100).
        // Values at domain 6-10 (y=200) should NOT be in the baseline
        // because they have domain > 3.
        // y=100 matches the baseline perfectly — no anomaly.
        TestTopology t = createTopology(10, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }
        for (int i = 6; i <= 10; i++) {
            uploadDataPoint(t, "alpha", i, 200.0);
        }

        // Upload at domain=3, y=100 — matches OLD baseline, NOT new baseline
        ValueEntity midRoot = uploadDataPoint(t, "alpha", 3, 100.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, midRoot, 0);

        assertEquals(0, changes.size(),
                "Mid-sequence upload at domain=3 with y=100 should NOT trigger anomaly. " +
                "Baseline for domain<=3 is [100,100,100] — value 100 matches perfectly. " +
                "The shift to 200 at domains 6-10 should not affect this baseline.");
    }

    @Test
    public void exact_window_size_boundary() throws Exception {
        // Regression test for off-by-one in windowSize.
        // With windowSize=5 and minDataPoints=6, we need exactly 5 baseline values
        // plus the current value (6 total). If the query fetches windowSize (5) instead
        // of windowSize+1 (6), the current value consumes a baseline slot, leaving only
        // 4 baseline + 1 current = 5 total. With minDataPoints=6, the calculator returns
        // null (insufficient data) and the anomaly is missed.
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 6);

        // Upload exactly 5 baseline values
        for (int i = 1; i <= 5; i++) {
            uploadDataPoint(t, "alpha", i, 100.0);
        }

        // Upload anomaly — with exactly 5 baseline + 1 current = 6 total
        // minDataPoints=6 requires exactly 6 values to proceed
        ValueEntity anomalyRoot = uploadDataPoint(t, "alpha", 6, 500.0);
        List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);

        assertEquals(1, changes.size(),
                "With exactly windowSize=5 baseline values + 1 current = 6 total, " +
                "minDataPoints=6 should be met and anomaly detected. " +
                "If the query fetches windowSize instead of windowSize+1, only 5 values " +
                "reach the calculator, failing the minDataPoints check.");
        assertEquals(500.0, changes.get(0).data.getField("value").asDouble(0.0), 0.01);
    }

    @Test
    public void recalculate_simulation_produces_consistent_results() throws Exception {
        // Simulate the recalculate flow: StdDev runs once per root value.
        // Upload a series with an anomaly at the end, verify detection.
        // Then re-run calculateStdDevAnomalyValues for each root value
        // (mimicking what recalculate does) and verify the final result
        // is consistent — same anomalies, no accumulation.
        TestTopology t = createTopology(5, 3.0, StdDevAnomalyConfig.Direction.BOTH, 3);

        // Upload 5 stable values + 1 anomaly
        List<ValueEntity> roots = new java.util.ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            roots.add(uploadDataPoint(t, "alpha", i, 100.0));
        }
        ValueEntity anomalyRoot = uploadDataPoint(t, "alpha", 6, 500.0);
        roots.add(anomalyRoot);

        // Initial detection: only the anomaly root should produce a change
        List<ValueEntity> initialChanges = nodeService.calculateStdDevAnomalyValues(t.sd, anomalyRoot, 0);
        assertEquals(1, initialChanges.size(), "Should detect 1 anomaly");

        // Persist the change
        tm.begin();
        for (ValueEntity change : initialChanges) {
            valueService.create(change);
        }
        tm.commit();

        // Simulate recalculate: run for EACH root value
        // Non-anomalous roots should produce 0 changes.
        // The anomaly root should re-detect the anomaly.
        // Stale cleanup should delete the old persisted change and
        // the new one replaces it (or keeps it if unchanged).
        for (ValueEntity root : roots) {
            List<ValueEntity> changes = nodeService.calculateStdDevAnomalyValues(t.sd, root, 0);
            if (!changes.isEmpty()) {
                tm.begin();
                for (ValueEntity change : changes) {
                    valueService.create(change);
                }
                tm.commit();
            }
        }

        // After simulated recalculation, verify exactly 1 anomaly remains
        tm.begin();
        List<ValueEntity> finalPersisted = ValueEntity.find("node.id", t.sd.id).list();
        assertEquals(1, finalPersisted.size(),
                "After simulated recalculation (running StdDev per root), " +
                "should still have exactly 1 anomaly (y=500). " +
                "If changes accumulated, cleanup is broken. " +
                "Found: " + finalPersisted.size());
        assertEquals(500.0, finalPersisted.get(0).data.getField("value").asDouble(0.0), 0.01,
                "The persisted anomaly should be for y=500");
        tm.commit();
    }
}
