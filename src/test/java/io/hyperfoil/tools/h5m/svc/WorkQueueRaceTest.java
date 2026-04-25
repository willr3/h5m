package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduces the work queue race condition where Work items are queued
 * in-memory before the creating transaction commits, causing worker threads
 * to fail with StaleObjectStateException and silently drop values.
 * <p>
 * See: https://github.com/Hyperfoil/h5m/issues/50
 */
@QuarkusTest
public class WorkQueueRaceTest extends FreshDb {

    @Inject
    FolderService folderService;

    @Inject
    WorkService workService;

    @Inject
    TransactionManager tm;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> TEST_FILES = List.of(
            "15248.json", "15763.json", "15764.json", "15765.json", "15766.json");

    /**
     * Uploads multiple JSON files back-to-back without waiting for the work queue
     * to drain between uploads. This maximizes contention — work items from
     * earlier uploads compete with new uploads for DB connections and transactions.
     * <p>
     * Before the fix, the transaction visibility race caused worker threads to
     * pick up Work items before the creating transaction committed. The workers
     * would fail with StaleObjectStateException and silently drop the Work,
     * resulting in missing computed values with no error reported to the user.
     * <p>
     * This test verifies the fix by asserting:
     * - No Work items permanently failed after exhausting retries (race condition
     *   symptom: StaleObjectStateException causing work to exceed retry limit)
     * - No Work items left in DB (race condition symptom: orphaned/stuck work)
     * - All expected values were computed (race condition symptom: missing values)
     */
    @Test
    void rapidUploadsShouldProduceAllValues() throws Exception {
        createThreeNodeTopology("rapid_test");

        // Upload all files back-to-back without draining — maximizes contention.
        // try/finally ensures the queue drains before FreshDb's @AfterEach truncation,
        // even if an upload throws — otherwise background workers can race with cleanup.
        try {
            for (String fileName : TEST_FILES) {
                JsonNode data = loadQvssFile(fileName);
                folderService.upload("rapid_test", "$", data);
            }
        } finally {
            awaitWorkQueue(30_000);
        }

        assertNoRaceConditionSymptoms();
    }

    /**
     * Uploads multiple JSON files sequentially, draining the work queue between
     * each upload. Same race condition assertions as the rapid test, but with
     * sequential uploads to verify correctness under lower contention.
     */
    @Test
    void sequentialUploadsShouldProduceAllValues() throws Exception {
        createThreeNodeTopology("sequential_test");

        try {
            for (String fileName : TEST_FILES) {
                JsonNode data = loadQvssFile(fileName);
                folderService.upload("sequential_test", "$", data);
                awaitWorkQueue(30_000);
            }
        } finally {
            awaitWorkQueue(30_000);
        }

        assertNoRaceConditionSymptoms();
    }

    private void assertNoRaceConditionSymptoms() {
        long remainingWork = Work.count();
        assertEquals(0, remainingWork,
                remainingWork + " Work items stuck in DB — this indicates work was " +
                "queued but never successfully processed. See #50");

        long valueCount = ValueEntity.count();
        assertTrue(valueCount >= 25,
                "Expected at least 25 values from 5 uploads; the 3-node topology uses .results[] paths, " +
                "so each upload can produce multiple values per node. Got " + valueCount +
                ". Missing values indicate the race condition silently dropped work. See #50");
    }

    private void createThreeNodeTopology(String folderName) throws Exception {
        tm.begin();
        try {
            long folderId = folderService.create(folderName);
            FolderEntity folder = folderService.read(folderId);
            NodeGroupEntity group = folder.group;
            NodeEntity root = group.root;

            JqNode n1 = new JqNode("avStartupRss", ".results[].rss.avStartupRss", root);
            n1.group = group;
            n1.persist();

            JqNode n2 = new JqNode("avBuildTime", ".results[].build.avBuildTime", root);
            n2.group = group;
            n2.persist();

            JqNode n3 = new JqNode("avThroughput", ".results[].load.avThroughput", root);
            n3.group = group;
            n3.persist();

            tm.commit();
        } catch (Exception e) {
            tm.rollback();
            throw e;
        }
    }

    private JsonNode loadQvssFile(String fileName) throws Exception {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("qvss/" + fileName)) {
            assertNotNull(is, "Missing test resource: qvss/" + fileName);
            return OBJECT_MAPPER.readTree(is);
        }
    }

    private void awaitWorkQueue(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        // Require both queue-idle AND no persisted Work rows for several consecutive
        // checks. The Work.count() gate catches the afterCompletion gap where the
        // queue is briefly idle between a parent work completing and its cascade
        // children being enqueued after the transaction commits.
        int stableChecks = 0;
        while (stableChecks < 5) {
            if (System.currentTimeMillis() > deadline) {
                fail("Work queue drain timed out after " + timeoutMs + "ms");
            }
            if (workService.isIdle() && Work.count() == 0) {
                stableChecks++;
            } else {
                stableChecks = 0;
            }
            Thread.sleep(50);
        }
    }
}
