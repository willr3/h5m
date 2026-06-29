package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.hyperfoil.tools.h5m.api.ProcessingType;
import io.hyperfoil.tools.h5m.api.RecalculationStatus;
import io.hyperfoil.tools.h5m.svc.RecalculationTracker;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ProcessingTrackerEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.event.ChangeDetectedEvent;
import io.hyperfoil.tools.h5m.svc.RecalculationService;
import jakarta.persistence.EntityManager;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class RecalculateTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    FolderService folderService;

    @Inject
    ProcessingService processingService;

    @Inject
    NodeService nodeService;

    @Inject
    WorkService workService;

    @Inject
    ValueService valueService;

    @Inject
    EntityManager em;

    @Inject
    RecalculationService recalculationService;

    @Inject
    ChangeEventObserver eventObserver;

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

    @ApplicationScoped
    public static class ChangeEventObserver {
        private final CopyOnWriteArrayList<ChangeDetectedEvent> events = new CopyOnWriteArrayList<>();

        public void onEvent(@Observes ChangeDetectedEvent event) {
            events.add(event);
        }

        public List<ChangeDetectedEvent> getEvents() {
            return events;
        }

        public void clear() {
            events.clear();
        }
    }

    @Test
    public void recalculate_produces_same_values_as_upload() throws Exception {
        // Set up folder with a chain: root -> extract (.key) -> transform (. + "_done")
        tm.begin();
        long folderId = folderService.create("recalc-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode extract = new JqNode("extract", ".key", folder.group.root);
        extract.group = folder.group;
        extract.persist();
        folder.group.sources.add(extract);

        JqNode transform = new JqNode("transform", ". + \"_done\"", extract);
        transform.group = folder.group;
        transform.persist();
        folder.group.sources.add(transform);

        folder.group.persist();
        long extractId = extract.id;
        long transformId = transform.id;
        tm.commit();

        // Upload data
        folderService.upload("recalc-test", "$", JqValues.parse("{\"key\": \"hello\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify values exist
        tm.begin();
        List<ValueEntity> extractValues = ValueEntity.find("node.id", extractId).list();
        List<ValueEntity> transformValues = ValueEntity.find("node.id", transformId).list();
        assertFalse(extractValues.isEmpty(), "Extract node should have values after upload");
        assertFalse(transformValues.isEmpty(), "Transform node should have values after upload");
        assertEquals("\"hello_done\"", transformValues.get(0).data.toString());
        tm.commit();

        // Recalculate and wait for completion
        folderService.recalculate("recalc-test").getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify values are unchanged (dedup should preserve identical values)
        tm.begin();
        List<ValueEntity> extractAfter = ValueEntity.find("node.id", extractId).list();
        List<ValueEntity> transformAfter = ValueEntity.find("node.id", transformId).list();
        assertEquals(extractValues.size(), extractAfter.size(), "Extract value count should be unchanged");
        assertEquals(transformValues.size(), transformAfter.size(), "Transform value count should be unchanged");
        assertEquals("\"hello_done\"", transformAfter.get(0).data.toString());
        tm.commit();
    }

    @Test
    public void recalculate_suppresses_notifications() throws Exception {
        // Set up folder with a fixed threshold that will detect a violation
        tm.begin();
        long folderId = folderService.create("recalc-notify-test");
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

        // Create fingerprint + fixed threshold nodes
        Long fpNodeId = nodeService.createConfigured("_fp", groupId,
                io.hyperfoil.tools.h5m.api.NodeType.FINGERPRINT, List.of(fpSourceId), null);
        Long ftNodeId = nodeService.createConfigured("ft", groupId,
                io.hyperfoil.tools.h5m.api.NodeType.FIXED_THRESHOLD,
                List.of(fpNodeId, folder.group.root.id, rangeId),
                new io.hyperfoil.tools.h5m.api.FixedThresholdConfig(null, 50.0, false, true, null));

        // Upload a value that exceeds the threshold
        eventObserver.clear();
        folderService.upload("recalc-notify-test", "$", JqValues.parse("{\"y\": 100, \"fp\": \"default\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Upload should dispatch notifications
        List<ChangeDetectedEvent> uploadEvents = new ArrayList<>(eventObserver.getEvents());
        boolean hasDispatchedEvent = uploadEvents.stream().anyMatch(e -> e.dispatch());
        assertTrue(hasDispatchedEvent, "Upload should fire events with dispatch=true");

        // Recalculate — should suppress notifications
        eventObserver.clear();
        folderService.recalculate("recalc-notify-test").getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        List<ChangeDetectedEvent> recalcEvents = eventObserver.getEvents();
        boolean hasNonDispatchedEvent = recalcEvents.stream().anyMatch(e -> !e.dispatch());
        if (!recalcEvents.isEmpty()) {
            // If events were fired during recalculation, they should all have dispatch=false
            assertTrue(recalcEvents.stream().noneMatch(e -> e.dispatch()),
                    "Recalculate events should have dispatch=false");
        }
    }

    @Test
    public void recalculateNode_only_affects_target_and_dependents() throws Exception {
        // Set up: root -> a (.key) -> b (. + "_b") and root -> c (.other)
        // Recalculating 'a' should affect a and b, but not c
        tm.begin();
        long folderId = folderService.create("selective-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        JqNode nodeC = new JqNode("c", ".other", folder.group.root);
        nodeC.group = folder.group;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        long nodeAId = nodeA.id;
        long nodeBId = nodeB.id;
        long nodeCId = nodeC.id;
        tm.commit();

        // Upload
        folderService.upload("selective-test", "$",
                JqValues.parse("{\"key\": \"hello\", \"other\": \"world\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify initial values
        tm.begin();
        assertEquals("hello", ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data.asText());
        assertEquals("\"hello_b\"", ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data.toString());
        assertEquals("world", ValueEntity.find("node.id", nodeCId).<ValueEntity>list().get(0).data.asText());
        tm.commit();

        // Selective recalculate node A
        folderService.recalculateNode(nodeAId).getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Values should be unchanged (same data, dedup preserves them)
        tm.begin();
        assertEquals("hello", ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data.asText());
        assertEquals("\"hello_b\"", ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data.toString());
        assertEquals("world", ValueEntity.find("node.id", nodeCId).<ValueEntity>list().get(0).data.asText());
        tm.commit();
    }

    @Test
    public void recalculate_returns_completable_future() throws Exception {
        // Verify that recalculate returns a future we can wait on
        tm.begin();
        folderService.create("future-test");
        tm.commit();

        // Recalculate empty folder — should complete immediately
        folderService.recalculate("future-test").getFuture()
                .orTimeout(10, TimeUnit.SECONDS).join();
        // No exception = success
    }

    @Test
    public void multiple_uploads_then_recalculate_preserves_change_detection() throws Exception {
        // Upload multiple values, verify change detection fires, then recalculate
        // and verify change detection results are consistent (same count).
        tm.begin();
        long folderId = folderService.create("multi-recalc-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode rangeNode = new JqNode("range", ".y", folder.group.root);
        rangeNode.group = folder.group;
        rangeNode.persist();
        folder.group.sources.add(rangeNode);

        JqNode fpSource = new JqNode("fpSource", ".fp", folder.group.root);
        fpSource.group = folder.group;
        fpSource.persist();
        folder.group.sources.add(fpSource);

        folder.group.persist();

        long groupId = folder.group.id;
        long rangeId = rangeNode.id;
        long fpSourceId = fpSource.id;
        tm.commit();

        // Create fingerprint + fixed threshold (max=50 → y=100 will violate)
        Long fpNodeId = nodeService.createConfigured("_fp", groupId,
                io.hyperfoil.tools.h5m.api.NodeType.FINGERPRINT, List.of(fpSourceId), null);
        Long ftNodeId = nodeService.createConfigured("ft", groupId,
                io.hyperfoil.tools.h5m.api.NodeType.FIXED_THRESHOLD,
                List.of(fpNodeId, folder.group.root.id, rangeId),
                new io.hyperfoil.tools.h5m.api.FixedThresholdConfig(null, 50.0, false, true, null));

        // Upload 3 values that all exceed the threshold
        for (int i = 0; i < 3; i++) {
            folderService.upload("multi-recalc-test", "$",
                    JqValues.parse(String.format("{\"y\": %d, \"fp\": \"default\"}", 100 + i * 10)))
                    .orTimeout(30, TimeUnit.SECONDS).join();
        }

        // Count change detection values after uploads
        tm.begin();
        List<ValueEntity> changesAfterUpload = ValueEntity.find("node.id", ftNodeId).list();
        int uploadChangeCount = changesAfterUpload.size();
        tm.commit();

        assertTrue(uploadChangeCount > 0,
                "Each upload with y > 50 should produce a change detection");

        // Recalculate
        folderService.recalculate("multi-recalc-test").getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify change detection count is the same after recalculation
        tm.begin();
        List<ValueEntity> changesAfterRecalc = ValueEntity.find("node.id", ftNodeId).list();
        tm.commit();

        assertEquals(uploadChangeCount, changesAfterRecalc.size(),
                "Recalculation should produce the same number of change detections. " +
                "After upload: " + uploadChangeCount + ", after recalc: " + changesAfterRecalc.size());
    }

    @Test
    public void recalculate_after_node_update_uses_new_operation() throws Exception {
        // When a node's operation changes, recalculateNode should produce
        // values reflecting the new operation, not the old one.
        tm.begin();
        long folderId = folderService.create("update-recalc-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode extract = new JqNode("extract", ".key", folder.group.root);
        extract.group = folder.group;
        extract.persist();
        folder.group.sources.add(extract);
        folder.group.persist();
        long extractId = extract.id;
        tm.commit();

        // Upload data
        folderService.upload("update-recalc-test", "$",
                JqValues.parse("{\"key\": \"hello\", \"other\": \"world\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify initial value
        tm.begin();
        List<ValueEntity> before = ValueEntity.find("node.id", extractId).list();
        assertEquals(1, before.size());
        assertEquals("hello", before.get(0).data.asText(), "Initial value should be 'hello'");
        tm.commit();

        // Update the node's operation from .key to .other
        tm.begin();
        NodeEntity toUpdate = NodeEntity.findById(extractId);
        toUpdate.operation = ".other";
        nodeService.update(toUpdate);
        tm.commit();

        // Explicitly trigger recalculation AFTER the update transaction committed.
        // This ensures recalculateNode sees the committed operation change.
        // In production, the REST endpoint or CLI would handle this sequencing.
        folderService.recalculateNode(extractId).getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify value changed to reflect new operation
        tm.begin();
        List<ValueEntity> after = ValueEntity.find("node.id", extractId).list();
        assertEquals(1, after.size(), "Should still have 1 value after recalculation");
        assertEquals("world", after.get(0).data.asText(),
                "After changing operation from .key to .other and recalculating, " +
                "value should be 'world'.");
        tm.commit();
    }

    @Test
    public void recalculate_tracks_progress() throws Exception {
        // Upload multiple values, then recalculate and verify progress tracking
        tm.begin();
        long folderId = folderService.create("progress-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode extract = new JqNode("extract", ".key", folder.group.root);
        extract.group = folder.group;
        extract.persist();
        folder.group.sources.add(extract);
        folder.group.persist();
        tm.commit();

        // Upload 3 values
        for (int i = 0; i < 3; i++) {
            folderService.upload("progress-test", "$",
                    JqValues.parse(String.format("{\"key\": \"value_%d\"}", i)))
                    .orTimeout(30, TimeUnit.SECONDS).join();
        }

        // Start recalculation — returns tracker immediately
        RecalculationTracker tracker = folderService.recalculate("progress-test");

        assertNotNull(tracker, "Should return a recalculation tracker");
        assertNotNull(tracker.getId(), "Tracker should have an ID");
        assertEquals("progress-test", tracker.getFolderName());
        assertEquals(-1, tracker.getNodeId(), "Full recalculate should have nodeId=-1");

        // Snapshot before completion
        RecalculationStatus before = tracker.toStatus();
        assertEquals(3, before.totalRoots(), "Should track 3 root values");

        // Wait for completion
        tracker.getFuture().orTimeout(30, TimeUnit.SECONDS).join();

        // After completion, snapshot should show all roots completed
        RecalculationStatus after = tracker.toStatus();
        assertEquals(RecalculationStatus.State.COMPLETED, after.state());
        assertEquals(3, after.completedRoots(), "All 3 roots should be completed");

        // Tracker should be retrievable via the service
        RecalculationTracker retrieved = recalculationService.get(tracker.getId());
        assertNotNull(retrieved, "Tracker should be retrievable by ID");
        assertEquals(RecalculationStatus.State.COMPLETED, retrieved.getState());
        assertTrue(retrieved.toStatus().durationMs() >= 0, "Duration should be non-negative");
    }

    @Test
    public void recalculate_with_split_preserves_datasets() throws Exception {
        // Set up a pipeline where a JQ node produces multiple values (split/dataset pattern).
        // This is how h5m handles datasets: a JQ node with .items[] produces one value
        // per array element, and downstream nodes process each element independently.
        //
        // Pipeline: root → each (.items[]) → value ({each}:.v)
        tm.begin();
        long folderId = folderService.create("split-recalc-test");
        FolderEntity folder = folderService.read(folderId);

        // JQ node that splits the array — produces multiple values
        JqNode eachNode = new JqNode("each", ".items[]", folder.group.root);
        eachNode.group = folder.group;
        eachNode.ephemeral = EphemeralMode.KEEP;
        eachNode.persist();
        folder.group.sources.add(eachNode);

        // Value node: extracts .v from each split element
        JqNode valueNode = new JqNode("value", ".v", eachNode);
        valueNode.group = folder.group;
        valueNode.ephemeral = EphemeralMode.KEEP;
        valueNode.persist();
        folder.group.sources.add(valueNode);

        folder.group.persist();
        long valueNodeId = valueNode.id;
        long eachNodeId = eachNode.id;
        tm.commit();

        // Upload data with 2 items (datasets) per upload
        folderService.upload("split-recalc-test", "$", JqValues.parse(
                "{\"items\": [{\"v\": 10}, {\"v\": 20}]}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify the JQ .items[] produced 2 values
        tm.begin();
        List<ValueEntity> eachValues = ValueEntity.find("node.id", eachNodeId).list();
        List<ValueEntity> valueValues = ValueEntity.find("node.id", valueNodeId).list();
        assertEquals(2, eachValues.size(), "JQ .items[] should produce 2 values (one per item)");
        assertEquals(2, valueValues.size(), "Value node should produce 2 values (one per dataset)");
        List<String> valueDataBefore = valueValues.stream()
                .map(v -> v.data.toString()).sorted().toList();
        tm.commit();

        // Upload a second data point
        folderService.upload("split-recalc-test", "$", JqValues.parse(
                "{\"items\": [{\"v\": 30}, {\"v\": 40}]}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify we now have 4 values (2 per upload × 2 uploads)
        tm.begin();
        List<ValueEntity> valuesBeforeRecalc = ValueEntity.find("node.id", valueNodeId).list();
        assertEquals(4, valuesBeforeRecalc.size(), "Should have 4 values after 2 uploads");
        tm.commit();

        // Recalculate
        folderService.recalculate("split-recalc-test").getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify values are preserved — same count, same data
        tm.begin();
        List<ValueEntity> valuesAfterRecalc = ValueEntity.find("node.id", valueNodeId).list();
        assertEquals(4, valuesAfterRecalc.size(),
                "Recalculate should preserve all 4 values (2 datasets × 2 uploads). " +
                "If multi-value handling is broken, values could be lost or duplicated.");

        List<ValueEntity> eachAfterRecalc = ValueEntity.find("node.id", eachNodeId).list();
        assertEquals(4, eachAfterRecalc.size(),
                "JQ split node should still have 4 values (2 per upload × 2 uploads)");

        // Verify the first upload's values are preserved
        List<String> allValues = valuesAfterRecalc.stream()
                .map(v -> v.data.toString()).sorted().toList();
        assertTrue(allValues.containsAll(valueDataBefore),
                "Original dataset values should be preserved after recalculation");
        tm.commit();
    }

    @Test
    public void recalculate_with_ephemeral_intermediate_nodes() throws Exception {
        // Test that recalculation works correctly when intermediate nodes
        // have ephemeral=AUTO (data nullified after upload).
        // Pipeline: root → extract(.key) [AUTO/ephemeral] → transform(. + "_done") [leaf/KEEP]
        //
        // After upload: extract.data = NULL (nullified), transform.data = "hello_done"
        // During recalculate: extract must be recomputed from root, then transform
        // reads the recomputed extract data, produces the same result.
        // After recalculate: extract.data nullified again, transform.data preserved.
        tm.begin();
        long folderId = folderService.create("ephemeral-recalc-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode extract = new JqNode("extract", ".key", folder.group.root);
        extract.group = folder.group;
        // DISCARD: data will be nullified after upload processing completes.
        // Note: AUTO would need markAutoEphemeral() first (addressed in PR #143).
        extract.ephemeral = EphemeralMode.DISCARD;
        extract.persist();
        folder.group.sources.add(extract);

        JqNode transform = new JqNode("transform", ". + \"_done\"", extract);
        transform.group = folder.group;
        // Leaf node — data is kept
        transform.ephemeral = EphemeralMode.KEEP;
        transform.persist();
        folder.group.sources.add(transform);

        folder.group.persist();
        long extractId = extract.id;
        long transformId = transform.id;
        tm.commit();

        // Upload data
        folderService.upload("ephemeral-recalc-test", "$", JqValues.parse("{\"key\": \"hello\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify: extract data should be NULL (ephemeral), transform data should be present
        tm.begin();
        List<ValueEntity> extractAfterUpload = ValueEntity.find("node.id", extractId).list();
        List<ValueEntity> transformAfterUpload = ValueEntity.find("node.id", transformId).list();
        assertFalse(extractAfterUpload.isEmpty(), "Extract value row should exist");
        assertFalse(transformAfterUpload.isEmpty(), "Transform value should exist");
        // Extract data is nullified by ephemeral cleanup
        assertNull(extractAfterUpload.get(0).data,
                "Extract data should be NULL after ephemeral nullification");
        assertEquals("\"hello_done\"", transformAfterUpload.get(0).data.toString(),
                "Transform (leaf) data should be preserved");
        tm.commit();

        // Recalculate — this must work even though extract.data is NULL.
        // The pipeline should re-extract from root.data → recompute extract → 
        // recompute transform → nullify extract again.
        folderService.recalculate("ephemeral-recalc-test").getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify: transform data should still be correct after recalculation
        tm.begin();
        List<ValueEntity> extractAfterRecalc = ValueEntity.find("node.id", extractId).list();
        List<ValueEntity> transformAfterRecalc = ValueEntity.find("node.id", transformId).list();
        assertFalse(extractAfterRecalc.isEmpty(), "Extract value row should still exist");
        assertFalse(transformAfterRecalc.isEmpty(), "Transform value should still exist");
        assertEquals("\"hello_done\"", transformAfterRecalc.get(0).data.toString(),
                "Transform data should be correct after recalculation, " +
                "even though the intermediate extract node was ephemeral (data=NULL). " +
                "The pipeline must re-extract from root data before transform can run.");
        // Extract data should be nullified again after recalculation
        assertNull(extractAfterRecalc.get(0).data,
                "Extract data should be NULL again after recalculation ephemeral cleanup");
        tm.commit();
    }

    @Test
    public void recalculateNode_with_ephemeral_source_chain() throws Exception {
        // Pipeline: root → A(.key) [DISCARD] → B(. + "_b") [DISCARD] → C(. + "_c") [KEEP]
        // After upload: A.data=NULL, B.data=NULL, C.data="hello_b_c"
        // recalculateNode(C) must walk up past B and A to find the root,
        // then recompute A → B → C via cascade.
        tm.begin();
        long folderId = folderService.create("chain-recalc-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.DISCARD;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.DISCARD;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        JqNode nodeC = new JqNode("c", ". + \"_c\"", nodeB);
        nodeC.group = folder.group;
        nodeC.ephemeral = EphemeralMode.KEEP;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        long nodeAId = nodeA.id;
        long nodeBId = nodeB.id;
        long nodeCId = nodeC.id;
        tm.commit();

        // Upload data
        folderService.upload("chain-recalc-test", "$", JqValues.parse("{\"key\": \"hello\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify after upload: A and B are nullified, C has data
        tm.begin();
        List<ValueEntity> aAfterUpload = ValueEntity.find("node.id", nodeAId).list();
        List<ValueEntity> bAfterUpload = ValueEntity.find("node.id", nodeBId).list();
        List<ValueEntity> cAfterUpload = ValueEntity.find("node.id", nodeCId).list();
        assertEquals(1, aAfterUpload.size(), "A should have 1 value row");
        assertEquals(1, bAfterUpload.size(), "B should have 1 value row");
        assertEquals(1, cAfterUpload.size(), "C should have 1 value row");
        assertNull(aAfterUpload.get(0).data, "A (DISCARD) data should be NULL");
        assertNull(bAfterUpload.get(0).data, "B (DISCARD) data should be NULL");
        assertEquals("\"hello_b_c\"", cAfterUpload.get(0).data.toString(),
                "C (KEEP) should have computed data 'hello_b_c'");
        tm.commit();

        // Verify findRecomputationStartNodes walks up the chain correctly
        Set<NodeEntity> startNodes = processingService.findRecomputationStartNodes(
                NodeEntity.findById(nodeCId), folder.group.root);
        assertEquals(1, startNodes.size(),
                "Should find 1 start node (A, whose source is root)");
        assertTrue(startNodes.stream().anyMatch(n -> n.getId().equals(nodeAId)),
                "Start node should be A (top of ephemeral chain, source is root). " +
                "Start nodes: " + startNodes.stream().map(n -> n.name).toList());

        // Selective recalculate of C — must walk up past B and A
        folderService.recalculateNode(nodeCId).getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify after recalculation: C data should still be correct
        tm.begin();
        List<ValueEntity> cAfterRecalc = ValueEntity.find("node.id", nodeCId).list();
        assertEquals(1, cAfterRecalc.size(), "C should still have 1 value row");
        assertEquals("\"hello_b_c\"", cAfterRecalc.get(0).data.toString(),
                "C should be correctly recomputed via cascade through A → B → C, " +
                "even though A and B had ephemeral NULL data.");
        // A and B should be nullified again after recalculation
        List<ValueEntity> aAfterRecalc = ValueEntity.find("node.id", nodeAId).list();
        List<ValueEntity> bAfterRecalc = ValueEntity.find("node.id", nodeBId).list();
        assertNull(aAfterRecalc.get(0).data, "A should be NULL again after recalculation cleanup");
        assertNull(bAfterRecalc.get(0).data, "B should be NULL again after recalculation cleanup");
        tm.commit();
    }

    @Test
    public void recalculateNode_with_mixed_ephemeral_and_keep_sources() throws Exception {
        // Pipeline: root → A(.key) [KEEP]
        //           root → B(.other) [DISCARD]
        //           A, B → C (null-input jq that combines both)
        // C has two sources: A (KEEP, has data) and B (DISCARD, data=NULL)
        // recalculateNode(C) must walk up B's chain (to root) but not A's.
        tm.begin();
        long folderId = folderService.create("mixed-recalc-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.KEEP;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ".other", folder.group.root);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.DISCARD;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        // C depends on both A and B — uses null-input to combine
        JqNode nodeC = new JqNode("c", "[input, input] | add", List.of(nodeA, nodeB));
        nodeC.group = folder.group;
        nodeC.ephemeral = EphemeralMode.KEEP;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        long nodeAId = nodeA.id;
        long nodeBId = nodeB.id;
        long nodeCId = nodeC.id;
        tm.commit();

        // Upload data
        folderService.upload("mixed-recalc-test", "$",
                JqValues.parse("{\"key\": \"hello\", \"other\": \"world\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify after upload: A has data (KEEP), B is NULL (DISCARD)
        tm.begin();
        List<ValueEntity> aVals = ValueEntity.find("node.id", nodeAId).list();
        List<ValueEntity> bVals = ValueEntity.find("node.id", nodeBId).list();
        List<ValueEntity> cVals = ValueEntity.find("node.id", nodeCId).list();
        assertFalse(aVals.isEmpty(), "A should have value rows");
        assertFalse(bVals.isEmpty(), "B should have value rows");
        assertNotNull(aVals.get(0).data, "A (KEEP) should have data: " + aVals.get(0).data);
        assertNull(bVals.get(0).data, "B (DISCARD) should have NULL data");
        // C may or may not have values depending on the multi-source calculation
        tm.commit();

        // Selective recalculate of C — must walk up B (ephemeral) but not A (KEEP)
        Set<NodeEntity> startNodes = processingService.findRecomputationStartNodes(
                NodeEntity.findById(nodeCId), folder.group.root);
        // A has data → no walk needed. B is DISCARD → walk up to B (source is root).
        // So start nodes should include B (and possibly C if A is fine)
        assertTrue(startNodes.stream().anyMatch(n -> n.getId().equals(nodeBId)),
                "Start nodes should include B (ephemeral source that needs recomputation). " +
                "Start nodes: " + startNodes.stream().map(n -> n.name).toList());

        folderService.recalculateNode(nodeCId).getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify: B should have been recomputed (data restored temporarily, then nullified)
        tm.begin();
        List<ValueEntity> bAfter = ValueEntity.find("node.id", nodeBId).list();
        assertFalse(bAfter.isEmpty(), "B should still have value rows after recalculation");
        assertNull(bAfter.get(0).data,
                "B (DISCARD) should be NULL again after ephemeral cleanup");
        tm.commit();
    }

    @Test
    public void recalculateNode_target_is_top_level_no_walk_needed() throws Exception {
        // Pipeline: root → A(.key) [KEEP]
        // recalculateNode(A) — source is root (always has data), no walk needed.
        tm.begin();
        long folderId = folderService.create("toplevel-recalc-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.KEEP;
        nodeA.persist();
        folder.group.sources.add(nodeA);
        folder.group.persist();
        long nodeAId = nodeA.id;
        tm.commit();

        folderService.upload("toplevel-recalc-test", "$", JqValues.parse("{\"key\": \"hello\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        tm.begin();
        assertEquals("hello", ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data.asText());
        tm.commit();

        folderService.recalculateNode(nodeAId).getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        tm.begin();
        assertEquals("hello", ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data.asText(),
                "Top-level node should be correctly recomputed from root");
        tm.commit();
    }

    @Test
    public void findRecomputationStartNodes_returns_target_when_sources_available() throws Exception {
        // Unit test for the helper method directly
        tm.begin();
        long folderId = folderService.create("start-nodes-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.KEEP;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.KEEP;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        folder.group.persist();
        tm.commit();

        // Both A and B are KEEP — recalculating B should start from B itself
        Set<NodeEntity> startNodes = processingService.findRecomputationStartNodes(nodeB, folder.group.root);
        assertEquals(1, startNodes.size(), "Should start from target node when sources have data");
        assertTrue(startNodes.contains(nodeB), "Start node should be the target node B");
    }

    @Test
    public void findRecomputationStartNodes_walks_up_past_ephemeral() throws Exception {
        // Unit test: A[DISCARD] → B[KEEP], recalculating B should start from A
        tm.begin();
        long folderId = folderService.create("walk-up-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.DISCARD;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.KEEP;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        folder.group.persist();
        tm.commit();

        // A is DISCARD → recalculating B should start from A (walk past A to root)
        Set<NodeEntity> startNodes = processingService.findRecomputationStartNodes(nodeB, folder.group.root);
        assertEquals(1, startNodes.size(), "Should walk up to A (whose source is root)");
        assertTrue(startNodes.contains(nodeA), "Start node should be A (source is root, data available)");
    }

    @Test
    public void findRecomputationStartNodes_walks_up_chain() throws Exception {
        // Unit test: A[DISCARD] → B[DISCARD] → C[KEEP]
        // recalculating C should start from A (walk all the way up)
        tm.begin();
        long folderId = folderService.create("walk-chain-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.DISCARD;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.DISCARD;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        JqNode nodeC = new JqNode("c", ". + \"_c\"", nodeB);
        nodeC.group = folder.group;
        nodeC.ephemeral = EphemeralMode.KEEP;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        tm.commit();

        Set<NodeEntity> startNodes = processingService.findRecomputationStartNodes(nodeC, folder.group.root);
        assertEquals(1, startNodes.size(), "Should walk all the way up to A");
        assertTrue(startNodes.contains(nodeA), "Start node should be A (source is root)");
    }

    // --- Crash recovery tests ---

    @Test
    public void recalculate_creates_tracking_entity() throws Exception {
        // Verify that recalculate() creates a ProcessingTrackerEntity
        // and marks it completed when processing finishes.
        tm.begin();
        long folderId = folderService.create("tracking-test");
        FolderEntity folder = folderService.read(folderId);
        JqNode extract = new JqNode("extract", ".key", folder.group.root);
        extract.group = folder.group;
        extract.persist();
        folder.group.sources.add(extract);
        folder.group.persist();
        tm.commit();

        folderService.upload("tracking-test", "$", JqValues.parse("{\"key\": \"hello\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Recalculate and wait
        folderService.recalculate("tracking-test").getFuture()
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify tracking entity exists and is completed
        tm.begin();
        ProcessingTrackerEntity tracker = ProcessingTrackerEntity.find(
                "type = ?1 and folderId = ?2", ProcessingType.RECALCULATE, folderId).firstResult();
        assertNotNull(tracker, "Recalculation should create a tracking entity");
        assertTrue(tracker.completed, "Tracking entity should be marked completed");
        assertEquals(-1, tracker.referenceId, "Full recalculate should have referenceId=-1");
        tm.commit();
    }

    @Test
    public void recover_incomplete_recalculation() throws Exception {
        // Simulate a crash during recalculation by manually creating an
        // incomplete tracker, then calling recovery.
        tm.begin();
        long folderId = folderService.create("recovery-recalc-test");
        FolderEntity folder = folderService.read(folderId);
        JqNode extract = new JqNode("extract", ".key", folder.group.root);
        extract.group = folder.group;
        extract.ephemeral = EphemeralMode.KEEP;
        extract.persist();
        folder.group.sources.add(extract);
        folder.group.persist();
        long extractId = extract.id;
        tm.commit();

        // Upload data
        folderService.upload("recovery-recalc-test", "$", JqValues.parse("{\"key\": \"hello\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Simulate crash: create incomplete recalculation tracker
        tm.begin();
        ProcessingTrackerEntity tracker = new ProcessingTrackerEntity(
                ProcessingType.RECALCULATE, folderId, -1);
        tracker.persist();
        long trackerId = tracker.id;
        tm.commit();

        // Trigger recovery
        processingService.recoverIncompleteProcessing(null);

        // Wait for any async work to complete
        awaitIdle(30_000);

        // Verify: old tracker should be completed
        tm.begin();
        ProcessingTrackerEntity oldTracker = ProcessingTrackerEntity.findById(trackerId);
        assertNotNull(oldTracker, "Old tracker should still exist");
        assertTrue(oldTracker.completed, "Old tracker should be marked completed by recovery");

        // Verify: values should be correct (recalculation ran)
        List<ValueEntity> values = ValueEntity.find("node.id", extractId).list();
        assertEquals(1, values.size(), "Extract node should have values");
        assertEquals("hello", values.get(0).data.asText(), "Values should be correct after recovery");
        tm.commit();
    }

    @Test
    public void recover_incomplete_selective_recalculation_with_ephemeral_chain() throws Exception {
        // Simulate crash during selective recalculation of a node with
        // ephemeral source chain: root → A[DISCARD] → B[DISCARD] → C[KEEP]
        // Recovery should walk up the ephemeral chain and recompute correctly.
        tm.begin();
        long folderId = folderService.create("recovery-ephemeral-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.DISCARD;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.DISCARD;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        JqNode nodeC = new JqNode("c", ". + \"_c\"", nodeB);
        nodeC.group = folder.group;
        nodeC.ephemeral = EphemeralMode.KEEP;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        long nodeCId = nodeC.id;
        long nodeAId = nodeA.id;
        long nodeBId = nodeB.id;
        tm.commit();

        // Upload data
        folderService.upload("recovery-ephemeral-test", "$", JqValues.parse("{\"key\": \"hello\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify: A and B nullified, C has data
        tm.begin();
        assertNull(ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data, "A should be NULL");
        assertNull(ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data, "B should be NULL");
        assertEquals("\"hello_b_c\"", ValueEntity.find("node.id", nodeCId).<ValueEntity>list().get(0).data.toString());
        tm.commit();

        // Simulate crash: create incomplete selective recalculation tracker for node C
        tm.begin();
        ProcessingTrackerEntity tracker = new ProcessingTrackerEntity(
                ProcessingType.RECALCULATE_NODE, folderId, nodeCId);
        tracker.persist();
        long trackerId = tracker.id;
        tm.commit();

        // Trigger recovery
        processingService.recoverIncompleteProcessing(null);

        // Wait for work to complete
        awaitIdle(30_000);

        // Verify: old tracker completed
        tm.begin();
        ProcessingTrackerEntity oldTracker = ProcessingTrackerEntity.findById(trackerId);
        assertTrue(oldTracker.completed, "Old tracker should be completed by recovery");

        // Verify: C should still have correct data (recovery walked up ephemeral chain)
        List<ValueEntity> cValues = ValueEntity.find("node.id", nodeCId).list();
        assertEquals(1, cValues.size(), "C should have 1 value");
        assertEquals("\"hello_b_c\"", cValues.get(0).data.toString(),
                "C should be correctly recomputed via A→B→C cascade during recovery, " +
                "even though A and B had ephemeral NULL data.");

        // A and B should be nullified again
        assertNull(ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data,
                "A should be NULL again after recovery ephemeral cleanup");
        assertNull(ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data,
                "B should be NULL again after recovery ephemeral cleanup");
        tm.commit();
    }

    @Test
    public void recover_mid_process_recalculation_with_partial_data() throws Exception {
        // Simulate a crash that happened MID-recalculation:
        // Pipeline: root → A(.key) [KEEP] → B(. + "_b") [KEEP] → C(. + "_c") [KEEP]
        //
        // After upload: A="hello", B="hello_b", C="hello_b_c"
        // Simulate mid-process crash: A was recomputed with new operation (.other)
        // producing "world", but B and C still have old values.
        // Recovery should detect the incomplete state and re-process B and C.
        tm.begin();
        long folderId = folderService.create("mid-process-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.KEEP;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.KEEP;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        JqNode nodeC = new JqNode("c", ". + \"_c\"", nodeB);
        nodeC.group = folder.group;
        nodeC.ephemeral = EphemeralMode.KEEP;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        long nodeAId = nodeA.id;
        long nodeBId = nodeB.id;
        long nodeCId = nodeC.id;
        tm.commit();

        // Upload initial data
        folderService.upload("mid-process-test", "$",
                JqValues.parse("{\"key\": \"hello\", \"other\": \"world\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify initial values
        tm.begin();
        assertEquals("hello", ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data.asText());
        assertEquals("\"hello_b\"", ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data.toString());
        assertEquals("\"hello_b_c\"", ValueEntity.find("node.id", nodeCId).<ValueEntity>list().get(0).data.toString());
        tm.commit();

        // Simulate mid-process state: change node A's operation and manually update
        // its value (as if the recalculation computed A but crashed before B and C)
        tm.begin();
        NodeEntity nodeAEntity = NodeEntity.findById(nodeAId);
        nodeAEntity.operation = ".other";
        em.merge(nodeAEntity);
        em.flush();

        // Update A's value data to "world" (simulating partial recalculation)
        ValueEntity aValue = ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0);
        em.createNativeQuery("UPDATE value SET data = cast(:data as jsonb) WHERE id = :id")
                .setParameter("data", "\"world\"")
                .setParameter("id", aValue.id)
                .executeUpdate();

        // Create incomplete tracker (simulating crash before B and C were processed)
        ProcessingTrackerEntity tracker = new ProcessingTrackerEntity(
                ProcessingType.RECALCULATE_NODE, folderId, nodeAId);
        tracker.persist();
        long trackerId = tracker.id;
        tm.commit();

        // At this point: A="world" (new), B="hello_b" (stale), C="hello_b_c" (stale)
        // The tracker says recalculation of A was in progress when crash happened.

        // Trigger recovery
        processingService.recoverIncompleteProcessing(null);
        awaitIdle(30_000);

        // Verify: recovery should have re-processed A and cascaded to B and C
        tm.begin();
        ProcessingTrackerEntity oldTracker = ProcessingTrackerEntity.findById(trackerId);
        assertTrue(oldTracker.completed, "Old tracker should be completed");

        // A should still be "world" (the new operation extracts .other)
        String aData = ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data.asText();
        assertEquals("world", aData, "A should have the new computed value 'world'");

        // B should now be "world_b" (cascaded from new A value)
        String bData = ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data.toString();
        assertEquals("\"world_b\"", bData,
                "B should be recomputed from A's new value. " +
                "If recovery didn't cascade, B would still be 'hello_b' (stale).");

        // C should now be "world_b_c"
        String cData = ValueEntity.find("node.id", nodeCId).<ValueEntity>list().get(0).data.toString();
        assertEquals("\"world_b_c\"", cData,
                "C should be recomputed from B's new value. " +
                "If recovery didn't cascade, C would still be 'hello_b_c' (stale).");
        tm.commit();
    }

    @Test
    public void recover_mid_process_with_ephemeral_intermediate_nodes() throws Exception {
        // Same as above but with ephemeral intermediate nodes:
        // Pipeline: root → A(.key) [DISCARD] → B(. + "_b") [DISCARD] → C(. + "_c") [KEEP]
        //
        // After upload: A=NULL (ephemeral), B=NULL (ephemeral), C="hello_b_c"
        // Simulate: node A's operation changed to .other, but crash before reprocessing.
        // Recovery must walk up the ephemeral chain and recompute everything.
        tm.begin();
        long folderId = folderService.create("mid-ephemeral-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.DISCARD;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ". + \"_b\"", nodeA);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.DISCARD;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        JqNode nodeC = new JqNode("c", ". + \"_c\"", nodeB);
        nodeC.group = folder.group;
        nodeC.ephemeral = EphemeralMode.KEEP;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        long nodeAId = nodeA.id;
        long nodeBId = nodeB.id;
        long nodeCId = nodeC.id;
        tm.commit();

        // Upload initial data
        folderService.upload("mid-ephemeral-test", "$",
                JqValues.parse("{\"key\": \"hello\", \"other\": \"world\"}"))
                .orTimeout(30, TimeUnit.SECONDS).join();

        // Verify: A and B nullified, C has data
        tm.begin();
        assertNull(ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data, "A should be NULL");
        assertNull(ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data, "B should be NULL");
        assertEquals("\"hello_b_c\"", ValueEntity.find("node.id", nodeCId).<ValueEntity>list().get(0).data.toString());
        tm.commit();

        // Change node A's operation (simulating the user action before crash)
        tm.begin();
        NodeEntity nodeAEntity = NodeEntity.findById(nodeAId);
        nodeAEntity.operation = ".other";
        em.merge(nodeAEntity);
        em.flush();

        // Create incomplete tracker for selective recalculation of A
        ProcessingTrackerEntity tracker = new ProcessingTrackerEntity(
                ProcessingType.RECALCULATE_NODE, folderId, nodeAId);
        tracker.persist();
        long trackerId = tracker.id;
        tm.commit();

        // Trigger recovery
        processingService.recoverIncompleteProcessing(null);
        awaitIdle(30_000);

        // Verify: recovery should recompute the chain with the new operation
        tm.begin();
        ProcessingTrackerEntity oldTracker = ProcessingTrackerEntity.findById(trackerId);
        assertTrue(oldTracker.completed, "Old tracker should be completed");

        // C should now reflect the new operation: .other → "world" → "world_b" → "world_b_c"
        String cData = ValueEntity.find("node.id", nodeCId).<ValueEntity>list().get(0).data.toString();
        assertEquals("\"world_b_c\"", cData,
                "C should be recomputed with A's new operation (.other → 'world'). " +
                "Recovery must walk up the ephemeral chain (A=NULL, B=NULL) and " +
                "recompute A→B→C from root data.");

        // A and B should be nullified again after recovery
        assertNull(ValueEntity.find("node.id", nodeAId).<ValueEntity>list().get(0).data,
                "A should be NULL again after recovery ephemeral cleanup");
        assertNull(ValueEntity.find("node.id", nodeBId).<ValueEntity>list().get(0).data,
                "B should be NULL again after recovery ephemeral cleanup");
        tm.commit();
    }

    // --- Edge case and validation tests ---

    @Test
    public void recalculateNode_rejects_invalid_node() throws Exception {
        tm.begin();
        folderService.create("invalid-node-test");
        tm.commit();

        // Non-existent node
        assertThrows(IllegalArgumentException.class,
                () -> folderService.recalculateNode(999999L),
                "Should reject non-existent node ID");
    }

    @Test
    public void recalculateNode_rejects_node_without_group() throws Exception {
        tm.begin();
        long folderId = folderService.create("no-group-test");
        FolderEntity folder = folderService.read(folderId);
        JqNode orphan = new JqNode("orphan", ".key", folder.group.root);
        // Intentionally leave group=null to simulate an orphan node
        orphan.persist();
        long orphanId = orphan.id;
        tm.commit();

        assertThrows(IllegalArgumentException.class,
                () -> folderService.recalculateNode(orphanId),
                "Should reject node that has no group");
    }

    @Test
    public void recalculate_rejects_when_upload_in_progress() throws Exception {
        tm.begin();
        long folderId = folderService.create("upload-guard-test");
        // Simulate in-flight upload
        ProcessingTrackerEntity inFlight = new ProcessingTrackerEntity(
                ProcessingType.UPLOAD, folderId, 1L);
        inFlight.persist();
        tm.commit();

        assertThrows(IllegalStateException.class,
                () -> folderService.recalculate("upload-guard-test"),
                "Should reject recalculation when uploads are in progress");
    }

    @Test
    public void findRecomputationStartNodes_diamond_dag() throws Exception {
        // Diamond: root → A[DISCARD] → C[KEEP]
        //          root → B[KEEP]    → C[KEEP]
        // C has two sources: A (ephemeral) and B (KEEP).
        // Start nodes should include A (needs recompute) but not B.
        tm.begin();
        long folderId = folderService.create("diamond-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode nodeA = new JqNode("a", ".key", folder.group.root);
        nodeA.group = folder.group;
        nodeA.ephemeral = EphemeralMode.DISCARD;
        nodeA.persist();
        folder.group.sources.add(nodeA);

        JqNode nodeB = new JqNode("b", ".other", folder.group.root);
        nodeB.group = folder.group;
        nodeB.ephemeral = EphemeralMode.KEEP;
        nodeB.persist();
        folder.group.sources.add(nodeB);

        // C depends on both A and B
        JqNode nodeC = new JqNode("c", ".", List.of(nodeA, nodeB));
        nodeC.group = folder.group;
        nodeC.ephemeral = EphemeralMode.KEEP;
        nodeC.persist();
        folder.group.sources.add(nodeC);

        folder.group.persist();
        tm.commit();

        Set<NodeEntity> startNodes = processingService.findRecomputationStartNodes(nodeC, folder.group.root);

        // A is DISCARD → walk up to A (source is root, has data).
        // B is KEEP → has data, no walk needed.
        // Since A is ephemeral, allSourcesHaveData is false for C,
        // so C is NOT added. A is added as a start node.
        assertTrue(startNodes.stream().anyMatch(n -> n.name.equals("a")),
                "Start nodes should include A (ephemeral source needs recompute). " +
                "Got: " + startNodes.stream().map(n -> n.name).toList());
        assertFalse(startNodes.stream().anyMatch(n -> n.name.equals("b")),
                "B (KEEP) should NOT be in start nodes — it has data");
        assertFalse(startNodes.stream().anyMatch(n -> n.name.equals("c")),
                "C should NOT be in start nodes — A is ephemeral so not all sources have data");
    }

    @Test
    public void recovery_with_deleted_folder_cleans_up_tracker() throws Exception {
        tm.begin();
        ProcessingTrackerEntity tracker = new ProcessingTrackerEntity(
                ProcessingType.RECALCULATE, 999999L, -1);
        tracker.persist();
        long trackerId = tracker.id;
        tm.commit();

        processingService.recoverIncompleteProcessing(null);

        tm.begin();
        ProcessingTrackerEntity deleted = ProcessingTrackerEntity.findById(trackerId);
        assertNull(deleted, "Tracker for nonexistent folder should be deleted during recovery");
        tm.commit();
    }

    @Test
    public void recovery_with_deleted_node_cleans_up_tracker() throws Exception {
        tm.begin();
        long folderId = folderService.create("recovery-deleted-node-test");
        tm.commit();

        tm.begin();
        ProcessingTrackerEntity tracker = new ProcessingTrackerEntity(
                ProcessingType.RECALCULATE_NODE, folderId, 999999L);
        tracker.persist();
        long trackerId = tracker.id;
        tm.commit();

        processingService.recoverIncompleteProcessing(null);

        tm.begin();
        ProcessingTrackerEntity deleted = ProcessingTrackerEntity.findById(trackerId);
        assertNull(deleted, "Tracker for nonexistent node should be deleted during recovery");
        tm.commit();
    }
}
