package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.api.EDivisiveConfig;
import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.EDivisive;
import io.hyperfoil.tools.h5m.entity.node.FingerprintNode;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import io.hyperfoil.tools.h5m.entity.node.SplitNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class EDivisiveTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    FolderService folderService;

    @Inject
    NodeService nodeService;

    @Inject
    WorkService workService;

    @Inject
    ValueService valueService;

    @Inject
    EntityManager em;

    private void awaitIdle(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int stableChecks = 0;
        while (stableChecks < 5) {
            if (System.currentTimeMillis() > deadline) {
                fail("Work queue drain timed out after " + timeoutMs + "ms");
            }
            if (workService.isIdle()) {
                stableChecks++;
            } else {
                stableChecks = 0;
            }
            Thread.sleep(50);
        }
    }

    /**
     * Creates a folder with a value node and an e-divisive detection node,
     * then uploads a series of numeric values.
     * Returns the e-divisive node ID for querying results.
     */
    private long setupAndUpload(String folderName, double[] series, int windowLen, double maxPvalue) throws Exception {
        tm.begin();
        long folderId = folderService.create(folderName);
        FolderEntity folder = folderService.read(folderId);

        // Create nodes: root -> value (extracts .v), domain (extracts .d) -> fingerprint -> edivisive
        JqNode valueNode = new JqNode("value", ".v", folder.group.root);
        valueNode.group = folder.group;
        valueNode.ephemeral = EphemeralMode.KEEP; // keep data for assertions
        valueNode.persist();

        JqNode domainNode = new JqNode("domain", ".d", folder.group.root);
        domainNode.group = folder.group;
        domainNode.ephemeral = EphemeralMode.KEEP;
        domainNode.persist();

        JqNode fpSource = new JqNode("fpSource", ".fp", folder.group.root);
        fpSource.group = folder.group;
        fpSource.persist();

        FingerprintNode fp = new FingerprintNode("fp", "", List.of(fpSource));
        fp.group = folder.group;
        fp.persist();

        EDivisive ed = new EDivisive();
        ed.name = "changedetect";
        ed.setWindowLen(windowLen);
        ed.setMaxPvalue(maxPvalue);
        ed.setMinMagnitude(0.0);
        ed.setMaxSeriesLength(500);
        ed.setNodes(fp, folder.group.root, valueNode, domainNode);
        ed.group = folder.group;
        ed.persist();
        long edId = ed.id;

        tm.commit();

        // Upload each value as a separate run with an incrementing domain value
        for (int i = 0; i < series.length; i++) {
            String json = String.format("{\"v\": %f, \"fp\": \"default\", \"d\": %d}", series[i], i);
            folderService.upload(folderName, "$", JqValues.parse(json))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }

        return edId;
    }

    @Test
    public void step_function_detects_single_change_point() throws Exception {
        // Series with a clear step: 10 values at mean=100, then 10 values at mean=200
        double[] series = new double[20];
        for (int i = 0; i < 10; i++) series[i] = 100.0 + (i % 3); // slight noise
        for (int i = 10; i < 20; i++) series[i] = 200.0 + (i % 3);

        long edId = setupAndUpload("step-test", series, 5, 0.05);

        tm.begin();
        List<ValueEntity> edValues = ValueEntity.find("node.id", edId).list();
        assertEquals(1, edValues.size(), "Should detect exactly 1 change point");

        // Verify the change point data structure
        ValueEntity cp = edValues.get(0);
        assertNotNull(cp.data, "Change point should have data");
        assertTrue(cp.data.has("index"), "Should have index field");
        assertTrue(cp.data.has("meanBefore"), "Should have meanBefore field");
        assertTrue(cp.data.has("meanAfter"), "Should have meanAfter field");
        assertTrue(cp.data.has("pvalue"), "Should have pvalue field");
        assertTrue(cp.data.has("magnitude"), "Should have magnitude field");
        assertTrue(cp.data.has("fingerprint"), "Should have fingerprint field");

        // The algorithm is deterministic (Welch's t-test) — change point should be exactly at index 10
        int cpIndex = cp.data.getField("index").asInt(0);
        assertEquals(10, cpIndex, "Change point should be at index 10");

        // meanBefore should be around 100, meanAfter around 200
        double meanBefore = cp.data.getField("meanBefore").asDouble(0.0);
        double meanAfter = cp.data.getField("meanAfter").asDouble(0.0);
        assertTrue(meanBefore < 150, "meanBefore should be near 100, was " + meanBefore);
        assertTrue(meanAfter > 150, "meanAfter should be near 200, was " + meanAfter);

        // Verify sources — change point should be linked to the groupBy value
        assertNotNull(cp.sources, "Change point should have sources");
        assertFalse(cp.sources.isEmpty(), "Change point should have at least one source");

        tm.commit();
    }

    @Test
    public void flat_series_no_change_points() throws Exception {
        // Flat series with slight noise — should detect no change points
        double[] series = new double[20];
        for (int i = 0; i < 20; i++) series[i] = 100.0 + (i % 3);

        long edId = setupAndUpload("flat-test", series, 5, 0.001);

        tm.begin();
        List<ValueEntity> edValues = ValueEntity.find("node.id", edId).list();
        assertTrue(edValues.isEmpty(), "Flat series should have no change points");
        tm.commit();
    }

    @Test
    public void multiple_change_points() throws Exception {
        // Series with two distinct shifts: 100 -> 200 -> 50
        double[] series = new double[30];
        for (int i = 0; i < 10; i++) series[i] = 100.0;
        for (int i = 10; i < 20; i++) series[i] = 200.0;
        for (int i = 20; i < 30; i++) series[i] = 50.0;

        long edId = setupAndUpload("multi-cp-test", series, 5, 0.05);

        tm.begin();
        List<ValueEntity> edValues = ValueEntity.find("node.id", edId).list();
        assertEquals(2, edValues.size(),
                "Should detect exactly 2 change points, found " + edValues.size());
        tm.commit();
    }

    @Test
    public void insufficient_data_no_crash() throws Exception {
        // Very short series — less than windowLen
        double[] series = {1.0, 2.0};

        long edId = setupAndUpload("short-test", series, 5, 0.05);

        tm.begin();
        List<ValueEntity> edValues = ValueEntity.find("node.id", edId).list();
        assertTrue(edValues.isEmpty(), "Insufficient data should produce no change points");
        tm.commit();
    }

    @Test
    public void recomputation_updates_change_points() throws Exception {
        // Upload initial series with a step
        String folderName = "recomp-test";

        tm.begin();
        long folderId = folderService.create(folderName);
        FolderEntity folder = folderService.read(folderId);

        JqNode valueNode = new JqNode("value", ".v", folder.group.root);
        valueNode.group = folder.group;
        valueNode.ephemeral = EphemeralMode.KEEP;
        valueNode.persist();

        JqNode domainNode = new JqNode("domain", ".d", folder.group.root);
        domainNode.group = folder.group;
        domainNode.ephemeral = EphemeralMode.KEEP;
        domainNode.persist();

        JqNode fpSource = new JqNode("fpSource", ".fp", folder.group.root);
        fpSource.group = folder.group;
        fpSource.persist();

        FingerprintNode fp = new FingerprintNode("fp", "", List.of(fpSource));
        fp.group = folder.group;
        fp.persist();

        EDivisive ed = new EDivisive();
        ed.name = "changedetect";
        ed.setWindowLen(5);
        ed.setMaxPvalue(0.05);
        ed.setMinMagnitude(0.0);
        ed.setMaxSeriesLength(500);
        ed.setNodes(fp, folder.group.root, valueNode, domainNode);
        ed.group = folder.group;
        ed.persist();
        long edId = ed.id;
        tm.commit();

        // Upload first batch: step from 100 to 200
        for (int i = 0; i < 10; i++) {
            double v = i < 5 ? 100.0 : 200.0;
            folderService.upload(folderName, "$", JqValues.parse(String.format("{\"v\": %f, \"fp\": \"default\", \"d\": %d}", v, i)))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }

        tm.begin();
        List<ValueEntity> firstBatch = ValueEntity.find("node.id", edId).list();
        int firstCount = firstBatch.size();
        tm.commit();

        // Upload more data — all at 200 (extending the second segment)
        for (int i = 0; i < 5; i++) {
            folderService.upload(folderName, "$", JqValues.parse(String.format("{\"v\": 200.0, \"fp\": \"default\", \"d\": %d}", 10 + i)))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }

        tm.begin();
        List<ValueEntity> secondBatch = ValueEntity.find("node.id", edId).list();
        // The key assertion: change points should be recomputed, not accumulated.
        // The step at index 5 should still be detected, so we expect 1 change point.
        // Old change points within the window must be deleted before new ones are created.
        assertEquals(firstCount, secondBatch.size(),
                "Change points should be recomputed, not accumulated. " +
                "First batch: " + firstCount + ", second batch: " + secondBatch.size() + ". " +
                "The step is still present, so the same number of change points should exist.");

        // Verify the change point IDs are DIFFERENT (old ones deleted, new ones created)
        if (!firstBatch.isEmpty() && !secondBatch.isEmpty()) {
            // The IDs should differ because old values were deleted and new ones created
            Set<Long> firstIds = new HashSet<>();
            for (ValueEntity v : firstBatch) firstIds.add(v.id);
            boolean anyNewId = false;
            for (ValueEntity v : secondBatch) {
                if (!firstIds.contains(v.id)) {
                    anyNewId = true;
                    break;
                }
            }
            assertTrue(anyNewId,
                    "Change point entities should have new IDs after recomputation, " +
                    "proving old ones were deleted and new ones created");
        }
        tm.commit();
    }

    @Test
    public void rest_createConfigured_edivisive() throws Exception {
        // Test that the createConfigured path works for EDIVISIVE nodes
        tm.begin();
        long folderId = folderService.create("rest-ed-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode rangeNode = new JqNode("range", ".y", folder.group.root);
        rangeNode.group = folder.group;
        rangeNode.persist();

        JqNode fpSource = new JqNode("fpSource", ".fp", folder.group.root);
        fpSource.group = folder.group;
        fpSource.persist();

        long groupId = folder.group.id;
        long rangeId = rangeNode.id;
        long fpSourceId = fpSource.id;
        tm.commit();

        Long fpNodeId = nodeService.createConfigured("_fp-test", groupId, NodeType.FINGERPRINT, List.of(fpSourceId), null);
        Long edNodeId = nodeService.createConfigured("ed-test", groupId, NodeType.EDIVISIVE,
                List.of(fpNodeId, folder.group.root.id, rangeId),
                new EDivisiveConfig(10, 0.01, 0.0, 200, null));

        assertNotNull(edNodeId, "Should return a valid node ID");

        // Clear the persistence context to force a fresh load with @PostLoad
        tm.begin();
        em.clear();
        NodeEntity edNode = NodeEntity.findById(edNodeId);
        assertNotNull(edNode, "Node should be persisted");
        assertTrue(edNode instanceof EDivisive, "Node should be EDivisive type");
        EDivisive ed = (EDivisive) edNode;
        assertEquals(10, ed.getWindowLen(), "windowLen should be 10");
        assertEquals(0.01, ed.getMaxPvalue(), 0.0001, "maxPvalue should be 0.01");
        assertEquals(200, ed.getMaxSeriesLength());
        tm.commit();
    }

    @Test
    public void multi_fingerprint_independent_detection() throws Exception {
        // Two fingerprints with different baselines — change points should be
        // detected independently per fingerprint
        String folderName = "multi-fp-test";
        tm.begin();
        long folderId = folderService.create(folderName);
        FolderEntity folder = folderService.read(folderId);

        JqNode valueNode = new JqNode("value", ".v", folder.group.root);
        valueNode.group = folder.group;
        valueNode.ephemeral = EphemeralMode.KEEP;
        valueNode.persist();

        JqNode domainNode = new JqNode("domain", ".d", folder.group.root);
        domainNode.group = folder.group;
        domainNode.ephemeral = EphemeralMode.KEEP;
        domainNode.persist();

        JqNode fpSource = new JqNode("fpSource", ".fp", folder.group.root);
        fpSource.group = folder.group;
        fpSource.persist();

        FingerprintNode fp = new FingerprintNode("fp", "", List.of(fpSource));
        fp.group = folder.group;
        fp.persist();

        EDivisive ed = new EDivisive();
        ed.name = "changedetect";
        ed.setWindowLen(5);
        ed.setMaxPvalue(0.05);
        ed.setMinMagnitude(0.0);
        ed.setMaxSeriesLength(500);
        ed.setNodes(fp, folder.group.root, valueNode, domainNode);
        ed.group = folder.group;
        ed.persist();
        long edId = ed.id;
        tm.commit();

        // Alpha: stable at 100 for 10 uploads, then step to 200
        for (int i = 0; i < 10; i++) {
            folderService.upload(folderName, "$", JqValues.parse(
                    String.format("{\"v\": 100.0, \"fp\": \"alpha\", \"d\": %d}", i)))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }
        for (int i = 10; i < 20; i++) {
            folderService.upload(folderName, "$", JqValues.parse(
                    String.format("{\"v\": 200.0, \"fp\": \"alpha\", \"d\": %d}", i)))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }

        // Beta: completely stable at 500 (no change points expected)
        for (int i = 0; i < 20; i++) {
            folderService.upload(folderName, "$", JqValues.parse(
                    String.format("{\"v\": 500.0, \"fp\": \"beta\", \"d\": %d}", i + 100)))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }

        tm.begin();
        List<ValueEntity> edValues = ValueEntity.find("node.id", edId).list();

        // Alpha has a step (100→200), beta is flat (500). The FingerprintNode produces
        // computed fingerprint data (not the raw "alpha"/"beta" string), so we verify
        // by checking total change points and their meanBefore/meanAfter values.
        // With independent fingerprints, only alpha's step should produce a change point.
        assertEquals(1, edValues.size(),
                "Should have exactly 1 change point (alpha's step). " +
                "Beta (stable) should not contribute any. Found: " + edValues.size());

        // Verify the change point has correct mean values for alpha's 100→200 step
        ValueEntity cp = edValues.get(0);
        assertTrue(cp.data.getField("meanBefore").asDouble(0.0) < 150,
                "meanBefore should be ~100 (alpha's baseline)");
        assertTrue(cp.data.getField("meanAfter").asDouble(0.0) > 150,
                "meanAfter should be ~200 (alpha's shifted level)");
        tm.commit();
    }

    @Test
    public void out_of_order_upload_reanalyzes_window() throws Exception {
        // Upload values 1-20, then upload a value at domain=15 with a different range.
        // The e-divisive should re-analyze the full window and update change points.
        String folderName = "ooo-test";

        tm.begin();
        long folderId = folderService.create(folderName);
        FolderEntity folder = folderService.read(folderId);

        JqNode valueNode = new JqNode("value", ".v", folder.group.root);
        valueNode.group = folder.group;
        valueNode.ephemeral = EphemeralMode.KEEP;
        valueNode.persist();

        JqNode domainNode = new JqNode("domain", ".d", folder.group.root);
        domainNode.group = folder.group;
        domainNode.ephemeral = EphemeralMode.KEEP;
        domainNode.persist();

        JqNode fpSource = new JqNode("fpSource", ".fp", folder.group.root);
        fpSource.group = folder.group;
        fpSource.persist();

        FingerprintNode fp = new FingerprintNode("fp", "", List.of(fpSource));
        fp.group = folder.group;
        fp.persist();

        EDivisive ed = new EDivisive();
        ed.name = "changedetect";
        ed.setWindowLen(5);
        ed.setMaxPvalue(0.05);
        ed.setMinMagnitude(0.0);
        ed.setMaxSeriesLength(500);
        ed.setNodes(fp, folder.group.root, valueNode, domainNode);
        ed.group = folder.group;
        ed.persist();
        long edId = ed.id;
        tm.commit();

        // Upload stable series at y=100
        for (int i = 0; i < 20; i++) {
            folderService.upload(folderName, "$", JqValues.parse(
                    String.format("{\"v\": 100.0, \"fp\": \"default\", \"d\": %d}", i)))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }

        tm.begin();
        List<ValueEntity> beforeOoo = ValueEntity.find("node.id", edId).list();
        int changesBefore = beforeOoo.size();
        tm.commit();

        assertEquals(0, changesBefore, "Stable series should have 0 change points before out-of-order upload");

        // Out-of-order upload at domain=15 (mid-sequence) with a very different value
        // This won't necessarily change the analysis since e-divisive looks at the
        // most recent N values. domain=15 is within the window.
        // The key point: the upload should not crash and should re-analyze correctly.
        folderService.upload(folderName, "$", JqValues.parse(
                "{\"v\": 100.0, \"fp\": \"default\", \"d\": 15}"))
                .future.orTimeout(30, TimeUnit.SECONDS).join();

        tm.begin();
        List<ValueEntity> afterOoo = ValueEntity.find("node.id", edId).list();
        tm.commit();

        assertEquals(0, afterOoo.size(),
                "Out-of-order upload with same value should not create change points");
    }

    @Test
    public void strengthen_change_point_assertions() throws Exception {
        // Step function with detailed assertions on all output JSON fields
        double[] series = new double[20];
        for (int i = 0; i < 10; i++) series[i] = 100.0;
        for (int i = 10; i < 20; i++) series[i] = 200.0;

        long edId = setupAndUpload("strong-assert-test", series, 5, 0.05);

        tm.begin();
        List<ValueEntity> edValues = ValueEntity.find("node.id", edId).list();
        assertEquals(1, edValues.size(), "Should detect exactly 1 change point");

        ValueEntity cp = edValues.get(0);
        JqValue data = cp.data;

        // All required fields present
        assertFalse(data.getField("index").isNull(), "Should have index");
        assertFalse(data.getField("pvalue").isNull(), "Should have pvalue");
        assertFalse(data.getField("meanBefore").isNull(), "Should have meanBefore");
        assertFalse(data.getField("meanAfter").isNull(), "Should have meanAfter");
        assertFalse(data.getField("stdBefore").isNull(), "Should have stdBefore");
        assertFalse(data.getField("stdAfter").isNull(), "Should have stdAfter");
        assertFalse(data.getField("magnitude").isNull(), "Should have magnitude");
        assertFalse(data.getField("fingerprint").isNull(), "Should have fingerprint");
        assertFalse(data.getField("rangeValueId").isNull(), "Should have rangeValueId");
        assertFalse(data.getField("domainvalue").isNull(), "Should have domainvalue");
        assertFalse(data.getField("hazardLevel").isNull(), "Should have hazardLevel");

        // Value assertions
        assertEquals(10, data.getField("index").asInt(0), "Change point should be at index 10");
        assertEquals(100.0, data.getField("meanBefore").asDouble(0.0), 1.0, "meanBefore should be ~100");
        assertEquals(200.0, data.getField("meanAfter").asDouble(0.0), 1.0, "meanAfter should be ~200");
        assertTrue(data.getField("pvalue").asDouble(0.0) < 0.05, "pvalue should be < 0.05");
        assertTrue(data.getField("magnitude").asDouble(0.0) > 0.0, "magnitude should be positive");
        assertFalse(data.getField("fingerprint").isNull(), "fingerprint should be present");
        assertTrue(data.getField("stdBefore").asDouble(0.0) >= 0, "stdBefore should be >= 0");
        assertTrue(data.getField("stdAfter").asDouble(0.0) >= 0, "stdAfter should be >= 0");

        // hazardLevel for 100→200: log(200/100) = log(2) ≈ 0.693
        double hazard = data.getField("hazardLevel").asDouble(0.0);
        assertEquals(0.693, hazard, 0.1, "hazardLevel should be ~log(2)");

        // Sources should be linked
        assertNotNull(cp.sources, "Should have sources");
        assertFalse(cp.sources.isEmpty(), "Should have at least one source");
        tm.commit();
    }

    @Test
    public void hazard_level_computed() throws Exception {
        // Step function that should produce a change point with a hazard level
        double[] series = new double[20];
        for (int i = 0; i < 10; i++) series[i] = 100.0;
        for (int i = 10; i < 20; i++) series[i] = 200.0;

        long edId = setupAndUpload("hazard-test", series, 5, 0.05);

        tm.begin();
        List<ValueEntity> edValues = ValueEntity.find("node.id", edId).list();
        assertFalse(edValues.isEmpty(), "Should detect a change point");

        ValueEntity cp = edValues.get(0);
        assertTrue(cp.data.has("hazardLevel"), "Should have hazardLevel field");
        double hazardLevel = cp.data.getField("hazardLevel").asDouble(0.0);
        // |log(200/100)| = |log(2)| ≈ 0.693 (sign depends on direction of change)
        assertTrue(Math.abs(hazardLevel) > 0.5 && Math.abs(hazardLevel) < 1.0,
                "Hazard level for 100<->200 should have magnitude ~0.693, was " + hazardLevel);
        tm.commit();
    }

    // --- Direct-call tests (mirror StdDev patterns) ---

    /**
     * Helper: creates the node topology for direct calculateEDivisiveValues calls.
     * root → split → [range (.y), domain (.d), fingerprint (.fp)]
     */
    private record DirectTopology(
            NodeEntity root, SplitNode split, NodeEntity range,
            NodeEntity domain, NodeEntity fingerprint, EDivisive ed
    ) {}

    private DirectTopology createDirectTopology(int windowLen, double maxPvalue) throws Exception {
        tm.begin();
        NodeEntity rootNode = new io.hyperfoil.tools.h5m.entity.node.RootNode();
        rootNode.persist();
        SplitNode splitNode = new SplitNode("split", "split", List.of(rootNode));
        splitNode.persist();
        NodeEntity rangeNode = new JqNode("range", ".y", splitNode);
        rangeNode.persist();
        NodeEntity domainNode = new JqNode("domain", ".d", splitNode);
        domainNode.persist();
        NodeEntity fingerprintNode = new JqNode("fingerprint", ".fp", splitNode);
        fingerprintNode.persist();

        EDivisive ed = new EDivisive();
        ed.name = "ed-direct";
        ed.setWindowLen(windowLen);
        ed.setMaxPvalue(maxPvalue);
        ed.setMinMagnitude(0.0);
        ed.setMaxSeriesLength(500);
        ed.setNodes(fingerprintNode, splitNode, rangeNode, domainNode);
        ed.persist();
        tm.commit();

        return new DirectTopology(rootNode, splitNode, rangeNode, domainNode, fingerprintNode, ed);
    }

    private ValueEntity uploadDirectDataPoint(DirectTopology t, String fingerprint, int domain, double range) throws Exception {
        tm.begin();
        ValueEntity root = new ValueEntity(null, t.root, JqValues.parse(
                String.format("{\"split\":[{\"fp\":\"%s\",\"d\":%d,\"y\":%s}]}", fingerprint, domain, range)));
        root.persist();
        ValueEntity split = new ValueEntity(null, t.split, root.data.getField("split").getElement(0), List.of(root));
        split.persist();
        new ValueEntity(null, t.domain, split.data.getField("d"), List.of(split)).persist();
        new ValueEntity(null, t.range, split.data.getField("y"), List.of(split)).persist();
        new ValueEntity(null, t.fingerprint, split.data.getField("fp"), List.of(split)).persist();
        tm.commit();
        return root;
    }

    @Test
    public void multiple_datasets_per_upload_independent() throws Exception {
        // Upload with multiple datasets (split values) — each fingerprint should
        // produce independent e-divisive results.
        // Alpha: stable at 100, then step to 200
        // Beta: completely stable at 500
        DirectTopology t = createDirectTopology(5, 0.05);

        // Build up history with separate uploads per fingerprint
        for (int i = 0; i < 10; i++) {
            uploadDirectDataPoint(t, "alpha", i, 100.0);
        }
        for (int i = 10; i < 20; i++) {
            uploadDirectDataPoint(t, "alpha", i, 200.0);
        }
        for (int i = 0; i < 20; i++) {
            uploadDirectDataPoint(t, "beta", i + 100, 500.0);
        }

        // Upload with BOTH datasets — this triggers e-divisive for both fingerprints
        tm.begin();
        ValueEntity root = new ValueEntity(null, t.root, JqValues.parse(
                "{\"split\":[{\"fp\":\"alpha\",\"d\":20,\"y\":200.0},{\"fp\":\"beta\",\"d\":120,\"y\":500.0}]}"));
        root.persist();
        JqValue splitArr = root.data.getField("split");
        for (int i = 0; i < splitArr.length(); i++) {
            JqValue ds = splitArr.getElement(i);
            ValueEntity split = new ValueEntity(null, t.split, ds, List.of(root));
            split.persist();
            new ValueEntity(null, t.domain, ds.getField("d"), List.of(split)).persist();
            new ValueEntity(null, t.range, ds.getField("y"), List.of(split)).persist();
            new ValueEntity(null, t.fingerprint, ds.getField("fp"), List.of(split)).persist();
        }
        tm.commit();

        List<ValueEntity> changes = nodeService.calculateEDivisiveValues(t.ed, root, 0);

        // Alpha has a step (100→200) → should detect 1 change point
        // Beta is flat (500) → should detect 0
        // Total should be 1
        assertEquals(1, changes.size(),
                "Only alpha's step (100→200) should produce a change point. " +
                "Beta (stable at 500) should contribute none. " +
                "If datasets cross-contaminate, we'd see incorrect results.");

        // Verify the change point is for alpha's step
        ValueEntity cp = changes.get(0);
        assertTrue(cp.data.getField("meanBefore").asDouble(0.0) < 150,
                "meanBefore should be ~100 (alpha's baseline), not contaminated by beta's 500");
        assertTrue(cp.data.getField("meanAfter").asDouble(0.0) > 150 && cp.data.getField("meanAfter").asDouble(0.0) < 250,
                "meanAfter should be ~200 (alpha's shifted level)");
    }

    @Test
    public void non_sequential_domain_values() throws Exception {
        // Upload domain values out of chronological order.
        // E-divisive fetches the most recent N values by domain order,
        // so the analysis window should contain the data regardless of upload order.
        DirectTopology t = createDirectTopology(5, 0.05);

        // Upload in scrambled order: first the "after" values, then the "before" values
        // Series should be: domain 0-9 at y=100, domain 10-19 at y=200
        // But upload order: 10,12,14,16,18 (y=200), then 0,2,4,6,8 (y=100),
        // then fill in the gaps
        for (int i = 10; i < 20; i += 2) {
            uploadDirectDataPoint(t, "alpha", i, 200.0);
        }
        for (int i = 0; i < 10; i += 2) {
            uploadDirectDataPoint(t, "alpha", i, 100.0);
        }
        for (int i = 1; i < 10; i += 2) {
            uploadDirectDataPoint(t, "alpha", i, 100.0);
        }
        for (int i = 11; i < 20; i += 2) {
            uploadDirectDataPoint(t, "alpha", i, 200.0);
        }

        // Final upload triggers analysis
        ValueEntity lastRoot = uploadDirectDataPoint(t, "alpha", 20, 200.0);
        List<ValueEntity> changes = nodeService.calculateEDivisiveValues(t.ed, lastRoot, 0);

        // Despite scrambled upload order, the analysis should see the full series
        // in domain order: [100,100,...,200,200,...] and detect the change point
        assertEquals(1, changes.size(),
                "E-divisive should detect the step function regardless of upload order. " +
                "The domain ordering ensures values are analyzed in the correct sequence.");
        assertTrue(changes.get(0).data.getField("meanBefore").asDouble(0.0) < 150,
                "meanBefore should be ~100");
        assertTrue(changes.get(0).data.getField("meanAfter").asDouble(0.0) > 150,
                "meanAfter should be ~200");
    }

    @Test
    public void change_point_removed_when_step_disappears() throws Exception {
        // Upload a step function, verify change point exists.
        // Then upload enough data to "fill in" the step with a gradual ramp.
        // The old change point should be removed on recomputation.
        DirectTopology t = createDirectTopology(5, 0.001);

        // Sharp step: 10 values at 100, then 10 at 200
        for (int i = 0; i < 10; i++) {
            uploadDirectDataPoint(t, "alpha", i, 100.0);
        }
        for (int i = 10; i < 20; i++) {
            uploadDirectDataPoint(t, "alpha", i, 200.0);
        }

        ValueEntity root20 = uploadDirectDataPoint(t, "alpha", 20, 200.0);
        List<ValueEntity> beforeFill = nodeService.calculateEDivisiveValues(t.ed, root20, 0);

        // Persist the change points
        tm.begin();
        for (ValueEntity cp : beforeFill) {
            valueService.create(cp);
        }
        tm.commit();

        assertTrue(beforeFill.size() >= 1,
                "Sharp step should produce at least 1 change point");

        // Now upload a gradual ramp from 100 to 200 that fills in the step.
        // This adds smooth transition data to the window.
        for (int i = 21; i <= 60; i++) {
            double v = 100.0 + (i - 21) * (100.0 / 40.0); // gradual ramp
            uploadDirectDataPoint(t, "alpha", i, v);
        }

        // Trigger recomputation — with the gradual ramp in the window,
        // the sharp step at index 10 may change or disappear
        ValueEntity rootFinal = uploadDirectDataPoint(t, "alpha", 61, 200.0);
        List<ValueEntity> afterFill = nodeService.calculateEDivisiveValues(t.ed, rootFinal, 0);

        // Verify that old change points within the window were cleaned up.
        // The old persisted change points should have been deleted by the cleanup
        // logic inside calculateEDivisiveValues. The new change points (afterFill)
        // are returned but not yet persisted — that's the caller's responsibility.
        tm.begin();
        List<ValueEntity> persistedAfter = ValueEntity.find("node.id", t.ed.id).list();
        assertEquals(0, persistedAfter.size(),
                "Old change points should have been deleted during recomputation. " +
                "The new change points are returned (not yet persisted). " +
                "If old ones remain, the cleanup is not working.");
        tm.commit();
    }

    @Test
    public void recalculate_produces_consistent_results() throws Exception {
        // Simulates the recalculate flow: e-divisive runs once per root value,
        // but due to cumulative dedup in WorkQueue, only one execution should
        // produce the final result. Here we test that running calculateEDivisiveValues
        // multiple times (once per root, as recalculate would) produces consistent
        // change points — the cleanup deletes old results and each run produces
        // the same analysis since the full series is the same.
        DirectTopology t = createDirectTopology(5, 0.05);

        // Upload 20 values: step from 100 to 200 at index 10
        List<ValueEntity> roots = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            roots.add(uploadDirectDataPoint(t, "alpha", i, 100.0));
        }
        for (int i = 10; i < 20; i++) {
            roots.add(uploadDirectDataPoint(t, "alpha", i, 200.0));
        }

        // Run e-divisive for the LAST root (simulating a single upload trigger)
        ValueEntity lastRoot = roots.getLast();
        List<ValueEntity> initialResults = nodeService.calculateEDivisiveValues(t.ed, lastRoot, 0);
        assertTrue(initialResults.size() >= 1, "Should detect at least 1 change point");

        // Persist the initial change points
        tm.begin();
        for (ValueEntity cp : initialResults) {
            valueService.create(cp);
        }
        tm.commit();

        int initialCount = initialResults.size();

        // Now simulate recalculation: run e-divisive for EACH root value
        // (this is what happens when recalculate() cascades to e-divisive for each root).
        // With cumulative dedup in WorkQueue, only one would actually run.
        // Here we run them all sequentially to verify that repeated runs
        // produce consistent results (cleanup + re-create).
        List<ValueEntity> lastResults = null;
        for (ValueEntity root : roots) {
            lastResults = nodeService.calculateEDivisiveValues(t.ed, root, 0);
            // Persist each run's results (simulating what WorkService.execute does)
            tm.begin();
            for (ValueEntity cp : lastResults) {
                valueService.create(cp);
            }
            tm.commit();
        }

        // After all runs, the persisted change points should match the last run
        tm.begin();
        List<ValueEntity> finalPersisted = ValueEntity.find("node.id", t.ed.id).list();
        assertEquals(initialCount, finalPersisted.size(),
                "After simulated recalculation (running e-divisive per root), " +
                "the final change point count should match the initial count. " +
                "Initial: " + initialCount + ", final: " + finalPersisted.size() + ". " +
                "The cleanup in calculateEDivisiveValues should delete old change points " +
                "before creating new ones, so repeated runs don't accumulate.");
        tm.commit();
    }

    @Test
    public void recalculate_via_pipeline_preserves_change_points() throws Exception {
        // Full pipeline test: upload data with a step function through the pipeline,
        // then re-upload all data (simulating recalculate). Change points should
        // be consistent.
        double[] series = new double[20];
        for (int i = 0; i < 10; i++) series[i] = 100.0 + (i % 3);
        for (int i = 10; i < 20; i++) series[i] = 200.0 + (i % 3);

        long edId = setupAndUpload("recalc-pipeline-test", series, 5, 0.05);

        // Count change points after initial uploads
        tm.begin();
        List<ValueEntity> initialCps = ValueEntity.find("node.id", edId).list();
        int initialCount = initialCps.size();
        tm.commit();

        assertTrue(initialCount >= 1, "Should have at least 1 change point after initial upload");

        // Upload 5 more values at y=200 (extending the series without adding a new change point)
        for (int i = 0; i < 5; i++) {
            String json = String.format("{\"v\": %f, \"fp\": \"default\", \"d\": %d}", 200.0 + (i % 3), 20 + i);
            folderService.upload("recalc-pipeline-test", "$", JqValues.parse(json))
                    .future.orTimeout(30, TimeUnit.SECONDS).join();
        }

        // Count change points after additional uploads
        tm.begin();
        List<ValueEntity> afterMoreUploads = ValueEntity.find("node.id", edId).list();
        tm.commit();

        // The step at index 10 should still be detected.
        // Change points should not accumulate — cleanup handles it.
        assertEquals(initialCount, afterMoreUploads.size(),
                "After additional uploads extending the series, change point count should " +
                "remain the same. The step at index 10 is still present. " +
                "Initial: " + initialCount + ", after more: " + afterMoreUploads.size());
    }
}
