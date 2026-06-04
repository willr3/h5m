package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.UploadProcessingEntity;
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

    private static final ObjectMapper mapper = new ObjectMapper();

    @Inject
    TransactionManager tm;

    @Inject
    FolderService folderService;

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
                mapper.readTree("{\"key\": \"recovered\"}"));
        valueService.create(rootValue);

        // Create an incomplete tracking record (simulating crash before completion)
        UploadProcessingEntity tracking = new UploadProcessingEntity(rootValue.id, "recovery-test");
        tracking.persist();
        tm.commit();

        // Verify no computed values yet (the work was never executed)
        tm.begin();
        List<ValueEntity> beforeRecovery = ValueEntity.find("node.id", jqNode.id).list();
        assertEquals(0, beforeRecovery.size(), "No values should exist before recovery");
        tm.commit();

        // Trigger recovery
        folderService.recoverIncompleteUploads(null);
        awaitIdle(10_000);

        // Verify the value was computed by the recovery
        tm.begin();
        List<ValueEntity> afterRecovery = ValueEntity.find("node.id", jqNode.id).list();
        assertFalse(afterRecovery.isEmpty(), "JQ node should have computed values after recovery");
        assertEquals("recovered", afterRecovery.get(0).data.asText(),
                "Recovered value should match the uploaded data");

        // Verify the tracking record is now completed
        UploadProcessingEntity updatedTracking = UploadProcessingEntity.find("rootValueId", rootValue.id).firstResult();
        assertTrue(updatedTracking.completed, "Tracking record should be marked completed after recovery");
        tm.commit();
    }

    @Test
    public void recovery_skips_missing_root_value() throws Exception {
        // Create an incomplete tracking record pointing to a non-existent root value
        tm.begin();
        UploadProcessingEntity tracking = new UploadProcessingEntity(999999L, "no-such-folder");
        tracking.persist();
        long trackingId = tracking.id;
        tm.commit();

        // Trigger recovery — should log warning and delete the tracking record
        folderService.recoverIncompleteUploads(null);

        // Verify the tracking record was deleted
        tm.begin();
        UploadProcessingEntity deleted = UploadProcessingEntity.findById(trackingId);
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
                mapper.readTree("{\"key\": \"orphaned\"}"));
        valueService.create(rootValue);
        long rootValueId = rootValue.id;

        UploadProcessingEntity tracking = new UploadProcessingEntity(rootValueId, "deleted-folder");
        tracking.persist();
        long trackingId = tracking.id;
        tm.commit();

        // Trigger recovery — "deleted-folder" doesn't exist
        folderService.recoverIncompleteUploads(null);

        // Verify the tracking record was deleted
        tm.begin();
        UploadProcessingEntity deleted = UploadProcessingEntity.findById(trackingId);
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
                mapper.readTree("{\"key\": \"k1\", \"value\": \"v1\"}"));
        valueService.create(rootValue);

        // Both nodes already have their computed values
        ValueEntity keyValue = new ValueEntity(f, extractKey, mapper.readTree("\"k1\""));
        keyValue.sources = List.of(rootValue);
        valueService.create(keyValue);

        ValueEntity valValue = new ValueEntity(f, extractValue, mapper.readTree("\"v1\""));
        valValue.sources = List.of(rootValue);
        valueService.create(valValue);

        // But tracking was never marked completed (crash happened here)
        UploadProcessingEntity tracking = new UploadProcessingEntity(rootValue.id, "already-computed");
        tracking.persist();
        long rootValueId = rootValue.id;
        tm.commit();

        // Count values before recovery
        tm.begin();
        long valuesBefore = ValueEntity.count();
        tm.commit();

        // Trigger recovery — should dedup everything and complete
        folderService.recoverIncompleteUploads(null);
        awaitIdle(10_000);

        // Verify no new values were created (everything was deduped)
        tm.begin();
        long valuesAfter = ValueEntity.count();
        assertEquals(valuesBefore, valuesAfter,
                "No new values should be created when all work was already computed");

        // Verify tracking is now completed
        UploadProcessingEntity updatedTracking = UploadProcessingEntity.find("rootValueId", rootValueId).firstResult();
        assertTrue(updatedTracking.completed,
                "Tracking record should be marked completed even when all values were already computed");
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
                mapper.readTree("{\"key\": \"k1\", \"value\": \"v1\"}"));
        valueService.create(rootValue);

        UploadProcessingEntity tracking = new UploadProcessingEntity(rootValue.id, "multi-node-test");
        tracking.persist();
        long rootValueId = rootValue.id;
        tm.commit();

        // Trigger recovery
        folderService.recoverIncompleteUploads(null);
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
        UploadProcessingEntity updatedTracking = UploadProcessingEntity.find("rootValueId", rootValueId).firstResult();
        assertTrue(updatedTracking.completed, "Tracking record should be marked completed after recovery");
        tm.commit();
    }
}
