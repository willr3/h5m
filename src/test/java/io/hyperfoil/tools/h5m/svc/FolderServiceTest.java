package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.jjq.value.*;
import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ProcessingTrackerEntity;
import io.hyperfoil.tools.h5m.api.ProcessingType;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class FolderServiceTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    FolderService folderService;

    @Inject
    ProcessingService processingService;

    @Inject
    WorkService workService;

    @Inject
    EntityManager em;

    @Inject
    ValueService valueService;

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

    // -- Crash recovery --

    @Test
    public void recovery_reprocesses_incomplete_upload() throws Exception {
        // Set up folder with a JQ node
        tm.begin();
        long folderId = folderService.create("recovery-test");
        FolderEntity folder = folderService.read(folderId);
        JqNode jqNode = new JqNode("extract", ".key", folder.group.root);
        jqNode.group = folder.group;
        jqNode.persist();
        tm.commit();

        // Create a root value manually (simulating a crash mid-upload
        // where the value was persisted but work was never completed)
        tm.begin();
        FolderEntity folder2 = FolderEntity.find("name", "recovery-test").firstResult();
        ValueEntity rootValue = new ValueEntity(folder2, folder2.group.root,
                JqValues.parse("{\"key\": \"recovered\"}"));
        valueService.create(rootValue);

        // Create an incomplete tracking record (simulating crash before completion)
        ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(ProcessingType.UPLOAD, folderId, rootValue.id);
        tracking.persist();
        tm.commit();

        // Verify no computed values yet (the work was never executed)
        tm.begin();
        List<ValueEntity> beforeRecovery = ValueEntity.find("node.id", jqNode.id).list();
        assertEquals(0, beforeRecovery.size(), "No values should exist before recovery");
        tm.commit();

        // Trigger recovery
        processingService.recoverIncompleteProcessing(null);
        awaitIdle(10_000);

        // Verify the value was computed by the recovery
        tm.begin();
        List<ValueEntity> afterRecovery = ValueEntity.find("node.id", jqNode.id).list();
        assertFalse(afterRecovery.isEmpty(), "JQ node should have computed values after recovery");
        assertEquals("recovered", afterRecovery.get(0).data.asText(),
                "Recovered value should match the uploaded data");

        // Verify the tracking record is now completed
        ProcessingTrackerEntity updatedTracking = ProcessingTrackerEntity.find("referenceId", rootValue.id).firstResult();
        assertTrue(updatedTracking.completed, "Tracking record should be marked completed after recovery");
        tm.commit();
    }

    @Test
    public void recovery_skips_missing_root_value() throws Exception {
        // Create an incomplete tracking record pointing to a non-existent root value
        tm.begin();
        ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(ProcessingType.UPLOAD, 999999L, 999999L);
        tracking.persist();
        long trackingId = tracking.id;
        tm.commit();

        // Trigger recovery — should log warning and delete the tracking record
        processingService.recoverIncompleteProcessing(null);

        // Verify the tracking record was deleted
        tm.begin();
        ProcessingTrackerEntity deleted = ProcessingTrackerEntity.findById(trackingId);
        assertNull(deleted, "Tracking record should be deleted when root value is missing");
        tm.commit();
    }

    @Test
    public void recovery_skips_missing_folder() throws Exception {
        // Create a root value in a folder, then delete the folder but leave tracking
        tm.begin();
        long folderId = folderService.create("temp-folder");
        FolderEntity folder = folderService.read(folderId);
        ValueEntity rootValue = new ValueEntity(folder, folder.group.root,
                JqValues.parse("{\"key\": \"orphaned\"}"));
        valueService.create(rootValue);
        long rootValueId = rootValue.id;

        ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(ProcessingType.UPLOAD, 888888L, rootValueId);
        tracking.persist();
        long trackingId = tracking.id;
        tm.commit();

        // Trigger recovery — "deleted-folder" doesn't exist
        processingService.recoverIncompleteProcessing(null);

        // Verify the tracking record was deleted
        tm.begin();
        ProcessingTrackerEntity deleted = ProcessingTrackerEntity.findById(trackingId);
        assertNull(deleted, "Tracking record should be deleted when folder is missing");
        tm.commit();
    }

    @Test
    public void recovery_completes_when_all_values_already_computed() throws Exception {
        // Simulates a crash that happened after all top-level nodes computed
        // their values but before the tracking record was marked completed.
        // Recovery should dedup everything (no new values) and still complete.
        tm.begin();
        long folderId = folderService.create("already-computed");
        FolderEntity folder = folderService.read(folderId);

        JqNode extractKey = new JqNode("extractKey", ".key", folder.group.root);
        extractKey.group = folder.group;
        extractKey.persist();
        folder.group.sources.add(extractKey);

        JqNode extractValue = new JqNode("extractValue", ".value", folder.group.root);
        extractValue.group = folder.group;
        extractValue.persist();
        folder.group.sources.add(extractValue);

        long extractKeyId = extractKey.id;
        long extractValueId = extractValue.id;
        tm.commit();

        // Create root value AND computed values (simulating all work completed)
        tm.begin();
        FolderEntity f = FolderEntity.find("name", "already-computed").firstResult();
        ValueEntity rootValue = new ValueEntity(f, f.group.root,
                JqValues.parse("{\"key\": \"k1\", \"value\": \"v1\"}"));
        valueService.create(rootValue);

        // Both nodes already have their computed values
        ValueEntity keyValue = new ValueEntity(f, extractKey, JqValues.parse("\"k1\""));
        keyValue.sources = List.of(rootValue);
        valueService.create(keyValue);

        ValueEntity valValue = new ValueEntity(f, extractValue, JqValues.parse("\"v1\""));
        valValue.sources = List.of(rootValue);
        valueService.create(valValue);

        // But tracking was never marked completed (crash happened here)
        ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(ProcessingType.UPLOAD, folderId, rootValue.id);
        tracking.persist();
        long rootValueId = rootValue.id;
        tm.commit();

        // Count values before recovery
        tm.begin();
        long valuesBefore = ValueEntity.count();
        tm.commit();

        // Trigger recovery — should dedup everything and complete
        processingService.recoverIncompleteProcessing(null);
        awaitIdle(10_000);

        // Verify no new values were created (everything was deduped)
        tm.begin();
        long valuesAfter = ValueEntity.count();
        assertEquals(valuesBefore, valuesAfter,
                "No new values should be created when all work was already computed");

        // Verify tracking is now completed
        ProcessingTrackerEntity updatedTracking = ProcessingTrackerEntity.find("referenceId", rootValueId).firstResult();
        assertTrue(updatedTracking.completed,
                "Tracking record should be marked completed even when all values were already computed");
        tm.commit();
    }

    // -- Ephemeral value data --

    @Test
    public void ephemeral_discard_data_nulled_after_upload() throws Exception {
        // ephemeral=DISCARD: data should be nulled after upload
        tm.begin();
        long folderId = folderService.create("ephemeral-discard-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode node = new JqNode("extract", ".key", folder.group.root);
        node.group = folder.group;
        node.ephemeral = EphemeralMode.DISCARD;
        node.persist();
        long nodeId = node.id;
        tm.commit();

        folderService.upload("ephemeral-discard-test", "$",
                JqValues.parse("{\"key\": \"k1\"}"))
                .future.orTimeout(30, java.util.concurrent.TimeUnit.SECONDS).join();

        tm.begin();
        List<ValueEntity> values = ValueEntity.find("node.id", nodeId).list();
        assertFalse(values.isEmpty(), "Node should still have value rows");
        assertNull(values.get(0).data, "ephemeral=DISCARD: data should be null after upload");
        tm.commit();
    }

    @Test
    public void ephemeral_keep_data_preserved_after_upload() throws Exception {
        // ephemeral=KEEP: data should always be preserved
        tm.begin();
        long folderId = folderService.create("ephemeral-keep-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode node = new JqNode("extract", ".key", folder.group.root);
        node.group = folder.group;
        node.ephemeral = EphemeralMode.KEEP;
        node.persist();
        long nodeId = node.id;
        tm.commit();

        folderService.upload("ephemeral-keep-test", "$",
                JqValues.parse("{\"key\": \"k1\"}"))
                .future.orTimeout(30, java.util.concurrent.TimeUnit.SECONDS).join();

        tm.begin();
        List<ValueEntity> values = ValueEntity.find("node.id", nodeId).list();
        assertFalse(values.isEmpty(), "Node should have values");
        assertNotNull(values.get(0).data, "ephemeral=KEEP: data should be preserved");
        assertEquals("k1", values.get(0).data.asText());
        tm.commit();
    }

    @Test
    public void ephemeral_auto_intermediate_data_nulled() throws Exception {
        // ephemeral=AUTO: intermediate node (has non-detection child) should be nulled
        // Node graph: root → parent (.key) → child ({parent}:.length)
        // parent is intermediate because child depends on it
        tm.begin();
        long folderId = folderService.create("ephemeral-auto-test");
        FolderEntity folder = folderService.read(folderId);

        // Parent node — ephemeral=AUTO (default), will have a non-detection child
        JqNode parent = new JqNode("parent", ".key", folder.group.root);
        parent.group = folder.group;
        // ephemeral defaults to AUTO
        parent.persist();

        // Child node — depends on parent, making parent intermediate
        // Uses {parent}: prefix to source from parent node
        JqNode child = new JqNode("child", ".", parent);
        child.group = folder.group;
        child.persist();

        long parentId = parent.id;
        long childId = child.id;
        tm.commit();

        // No need to call markAutoEphemeral — the inlined AUTO logic in
        // nullifyEphemeralData resolves AUTO nodes based on graph structure

        folderService.upload("ephemeral-auto-test", "$",
                JqValues.parse("{\"key\": \"k1\"}"))
                .future.orTimeout(30, java.util.concurrent.TimeUnit.SECONDS).join();

        tm.begin();
        // Parent (intermediate, auto) should have data nulled
        List<ValueEntity> parentValues = ValueEntity.find("node.id", parentId).list();
        assertFalse(parentValues.isEmpty(), "Parent should have value rows");
        assertNull(parentValues.get(0).data,
                "ephemeral=AUTO with non-detection child: data should be auto-nulled");

        // Child (leaf, auto) should have data preserved
        List<ValueEntity> childValues = ValueEntity.find("node.id", childId).list();
        assertFalse(childValues.isEmpty(), "Child should have values");
        assertNotNull(childValues.get(0).data, "Leaf node data should be preserved");
        tm.commit();
    }

    @Test
    public void ephemeral_auto_leaf_data_preserved() throws Exception {
        // ephemeral=AUTO: leaf node (no children) should keep data
        tm.begin();
        long folderId = folderService.create("ephemeral-leaf-test");
        FolderEntity folder = folderService.read(folderId);

        JqNode leaf = new JqNode("leaf", ".key", folder.group.root);
        leaf.group = folder.group;
        // ephemeral defaults to AUTO, but no children = leaf
        leaf.persist();
        long leafId = leaf.id;
        tm.commit();

        folderService.upload("ephemeral-leaf-test", "$",
                JqValues.parse("{\"key\": \"k1\"}"))
                .future.orTimeout(30, java.util.concurrent.TimeUnit.SECONDS).join();

        tm.begin();
        List<ValueEntity> values = ValueEntity.find("node.id", leafId).list();
        assertFalse(values.isEmpty(), "Leaf should have values");
        assertNotNull(values.get(0).data,
                "ephemeral=AUTO with no children: leaf data should be preserved");
        tm.commit();
    }

    @Test
    public void recovery_processes_multiple_independent_nodes() throws Exception {
        // Set up folder with two independent top-level nodes extracting different fields.
        // Verifies that recovery processes all nodes, not just the first one.
        tm.begin();
        long folderId = folderService.create("multi-node-test");
        FolderEntity folder = folderService.read(folderId);
        NodeEntity root = folder.group.root;

        JqNode extractKey = new JqNode("extractKey", ".key", root);
        extractKey.group = folder.group;
        extractKey.persist();
        folder.group.sources.add(extractKey);

        JqNode extractValue = new JqNode("extractValue", ".value", root);
        extractValue.group = folder.group;
        extractValue.persist();
        folder.group.sources.add(extractValue);

        long extractKeyId = extractKey.id;
        long extractValueId = extractValue.id;
        tm.commit();

        // Create root value and incomplete tracking (simulating crash)
        tm.begin();
        FolderEntity f = FolderEntity.find("name", "multi-node-test").firstResult();
        ValueEntity rootValue = new ValueEntity(f, f.group.root,
                JqValues.parse("{\"key\": \"k1\", \"value\": \"v1\"}"));
        valueService.create(rootValue);

        ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(ProcessingType.UPLOAD, folderId, rootValue.id);
        tracking.persist();
        long rootValueId = rootValue.id;
        tm.commit();

        // Trigger recovery
        processingService.recoverIncompleteProcessing(null);
        awaitIdle(10_000);

        // Verify both nodes computed values
        tm.begin();
        List<ValueEntity> keyValues = ValueEntity.find("node.id", extractKeyId).list();
        assertFalse(keyValues.isEmpty(), "extractKey should have computed values after recovery");
        assertEquals("k1", keyValues.get(0).data.asText());

        List<ValueEntity> valValues = ValueEntity.find("node.id", extractValueId).list();
        assertFalse(valValues.isEmpty(), "extractValue should have computed values after recovery");
        assertEquals("v1", valValues.get(0).data.asText());

        // Verify tracking completed
        ProcessingTrackerEntity updatedTracking = ProcessingTrackerEntity.find("referenceId", rootValueId).firstResult();
        assertTrue(updatedTracking.completed, "Tracking record should be marked completed after recovery");
        tm.commit();
    }
    
    @Test
    public void upload_id_matches_root_value() throws Exception {
        tm.begin();
        folderService.create("upload-root-match-test");
        tm.commit();

        long uploadId = folderService.upload("upload-root-match-test", null,
                JqValues.parse("{\"cpu\": 95}")).uploadId;

        tm.begin();
        ValueEntity rootValue = ValueEntity.findById(uploadId);
        assertNotNull(rootValue, "Upload ID should correspond to a root ValueEntity");
        assertEquals(uploadId, rootValue.id, "upload should return the root value id");
        tm.commit();
    }

    @Test
    public void upload_creates_processing_tracker() throws Exception {
        tm.begin();
        folderService.create("upload-tracker-svc-test");
        tm.commit();

        long uploadId = folderService.upload("upload-tracker-svc-test", null,
                JqValues.parse("{\"cpu\": 95}")).uploadId;

        tm.begin();
        ProcessingTrackerEntity entity = ProcessingTrackerEntity.find(
                "type = ?1 and referenceId = ?2", ProcessingType.UPLOAD, uploadId).firstResult();
        assertNotNull(entity, "ProcessingTrackerEntity should be created on upload");
        assertEquals(uploadId, entity.referenceId);
        tm.commit();
    }

    @Test
    public void upload_returns_unique_ids_per_upload() throws Exception {
        tm.begin();
        folderService.create("upload-unique-svc-test");
        tm.commit();

        long id1 = folderService.upload("upload-unique-svc-test", null,
                JqValues.parse("{\"cpu\": 95}")).uploadId;
        long id2 = folderService.upload("upload-unique-svc-test", null,
                JqValues.parse("{\"cpu\": 99}")).uploadId;

        assertNotEquals(id1, id2, "Each upload should return a unique ID");
    }
}
