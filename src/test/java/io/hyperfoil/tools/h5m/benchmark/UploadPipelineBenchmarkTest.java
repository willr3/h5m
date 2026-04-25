package io.hyperfoil.tools.h5m.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.h5m.svc.ValueService;
import io.hyperfoil.tools.h5m.svc.WorkService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Realistic upload→calculate pipeline benchmark using real qvss test data.
 * Measures end-to-end time from folder setup through upload and value calculation.
 * <p>
 * Run with: mvn test -Dtest=UploadPipelineBenchmarkTest -Dsurefire.excludes=""
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class UploadPipelineBenchmarkTest extends FreshDb {

    private static final int WARMUP = 1;
    private static final int MEASURE = 3;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<BenchmarkTimer.Result> results = new ArrayList<>();
    private static final List<String> reports = new ArrayList<>();

    @Inject
    FolderService folderService;

    @Inject
    NodeService nodeService;

    @Inject
    ValueService valueService;

    @Inject
    TransactionManager tm;

    @Inject
    WorkService workService;

    // qvss files used in the fixedthreshold_qvss_throughput test
    private static final String[] QVSS_9 = {
            "27405.json", "27406.json", "27271.json", "27272.json",
            "26594.json", "26598.json", "27279.json", "27897.json", "84315.json"
    };

    // Larger set: 30 qvss files for heavier workload
    private static final String[] QVSS_30 = {
            "27405.json", "27406.json", "27271.json", "27272.json",
            "26594.json", "26598.json", "26599.json", "26776.json",
            "27279.json", "27897.json", "27940.json", "27941.json",
            "84315.json", "84642.json", "84733.json",
            "7691.json", "7750.json", "6313.json", "6314.json",
            "16328.json", "17333.json", "17334.json",
            "6067.json", "6068.json", "6132.json",
            "15248.json", "15763.json", "15764.json", "15765.json", "15766.json"
    };

    @Override
    public void dropRows() throws SQLException {
        try {
            awaitWorkQueue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.dropRows();
    }

    @BeforeEach
    void rollbackStale() throws Exception {
        if (tm.getTransaction() != null) {
            tm.rollback();
        }
    }

    // ==================== Pipeline: 2 JQ nodes, 9 uploads ====================

    @Test
    @Order(1)
    void pipeline_2nodes_9uploads() throws Exception {
        benchUploadPipeline("pipeline_2n_9u", 2, QVSS_9);
    }

    // ==================== Pipeline: 5 JQ nodes, 9 uploads ====================

    @Test
    @Order(2)
    void pipeline_5nodes_9uploads() throws Exception {
        benchUploadPipeline("pipeline_5n_9u", 5, QVSS_9);
    }

    // ==================== Pipeline: 2 JQ nodes, 30 uploads ====================

    @Test
    @Order(3)
    void pipeline_2nodes_30uploads() throws Exception {
        benchUploadPipeline("pipeline_2n_30u", 2, QVSS_30);
    }

    // ==================== Pipeline: 5 JQ nodes, 30 uploads ====================

    @Test
    @Order(4)
    void pipeline_5nodes_30uploads() throws Exception {
        benchUploadPipeline("pipeline_5n_30u", 5, QVSS_30);
    }

    // ==================== Pipeline: chained nodes (dependent calculation) ====================

    @Test
    @Order(5)
    void pipeline_chained_9uploads() throws Exception {
        benchChainedPipeline("pipeline_chain_9u", QVSS_9);
    }

    @Test
    @Order(6)
    void pipeline_chained_30uploads() throws Exception {
        benchChainedPipeline("pipeline_chain_30u", QVSS_30);
    }

    // ==================== Final report ====================

    @AfterAll
    static void printReport() {
        System.out.println("\n========== UPLOAD PIPELINE BENCHMARK RESULTS ==========");
        System.out.println(BenchmarkTimer.csvHeader());
        for (BenchmarkTimer.Result r : results) {
            System.out.println(BenchmarkTimer.toCsv(r));
        }
        System.out.println("\n========== PIPELINE REPORTS ==========");
        for (String report : reports) {
            System.out.println(report);
        }
        System.out.println("========================================================\n");
    }

    // ==================== Benchmark helpers ====================

    private void benchUploadPipeline(String name, int nodeCount, String[] files) throws Exception {
        JsonNode[] data = loadQvssData(files);

        BenchmarkTimer.Result result = BenchmarkTimer.run(
                name, WARMUP, MEASURE,
                this::cleanDb,
                () -> {
                    setupFlatNodes(name, nodeCount);
                    uploadAndWait(name, data);
                }
        );

        // Final run for reporting
        cleanDb();
        setupFlatNodes(name, nodeCount);
        uploadAndWait(name, data);
        reportCounts(name, nodeCount, files.length);
        results.add(result);
    }

    private void benchChainedPipeline(String name, String[] files) throws Exception {
        JsonNode[] data = loadQvssData(files);

        BenchmarkTimer.Result result = BenchmarkTimer.run(
                name, WARMUP, MEASURE,
                this::cleanDb,
                () -> {
                    setupChainedNodes(name);
                    uploadAndWait(name, data);
                }
        );

        cleanDb();
        setupChainedNodes(name);
        uploadAndWait(name, data);
        reportCounts(name, 3, files.length);
        results.add(result);
    }

    private void setupFlatNodes(String folderName, int nodeCount) throws Exception {
        tm.begin();
        long folderId = folderService.create(folderName);
        FolderEntity folder = folderService.read(folderId);
        NodeEntity root = folder.group.root;

        String[] jqExpressions = {
                ".results.\"quarkus3-jvm\".load.avThroughput",
                ".config.QUARKUS_VERSION",
                ".timing.start",
                ".env.HOST",
                ".results.\"quarkus3-jvm\".load.avLatency"
        };

        for (int i = 0; i < nodeCount; i++) {
            JqNode node = new JqNode("node_" + i, jqExpressions[i % jqExpressions.length], root);
            node.group = folder.group;
            folder.group.sources.add(node);
        }
        folder.group.persist();
        tm.commit();
    }

    private void setupChainedNodes(String folderName) throws Exception {
        tm.begin();
        long folderId = folderService.create(folderName);
        FolderEntity folder = folderService.read(folderId);
        NodeEntity root = folder.group.root;

        // throughput extracts a value from the upload
        JqNode throughput = new JqNode("throughput", ".results.\"quarkus3-jvm\".load.avThroughput", root);
        throughput.group = folder.group;
        folder.group.sources.add(throughput);

        // version extracts another value
        JqNode version = new JqNode("version", ".config.QUARKUS_VERSION", root);
        version.group = folder.group;
        folder.group.sources.add(version);

        // combined depends on throughput (chained calculation)
        JqNode combined = new JqNode("combined", ".", throughput);
        combined.group = folder.group;
        folder.group.sources.add(combined);

        folder.group.persist();
        tm.commit();
    }

    private void uploadAndWait(String folderName, JsonNode[] data) throws Exception {
        for (int i = 0; i < data.length; i++) {
            folderService.upload(folderName, "upload", data[i]);
            // Drain work queue every 10 uploads to prevent connection pool exhaustion
            // when chained nodes create cascading work items
            if ((i + 1) % 10 == 0) {
                awaitWorkQueue();
            }
        }
        awaitWorkQueue();
    }

    private void awaitWorkQueue() throws InterruptedException {
        while (!workService.isIdle()) {
            Thread.sleep(10);
        }
    }

    private JsonNode[] loadQvssData(String[] filenames) throws IOException {
        JsonNode[] data = new JsonNode[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            try (InputStream is = getClass().getClassLoader().getResourceAsStream("qvss/" + filenames[i])) {
                data[i] = OBJECT_MAPPER.readTree(is);
            }
        }
        return data;
    }

    private void cleanDb() throws Exception {
        if (tm.getTransaction() != null) {
            tm.rollback();
        }
        awaitWorkQueue();
        dropRows();
    }

    private void reportCounts(String label, int nodeCount, int uploadCount) throws SQLException {
        long nodes = countRows("node");
        long values = countRows("value");
        long nodeEdges = countRows("node_edge");
        long valueEdges = countRows("value_edge");
        long workItems = countRows("work");
        String report = String.format(
                "[PIPELINE] %s: configured_nodes=%d, uploads=%d, total_nodes=%d, total_values=%d, " +
                        "node_edges=%d, value_edges=%d, pending_work=%d",
                label, nodeCount, uploadCount, nodes, values, nodeEdges, valueEdges, workItems);
        reports.add(report);
    }
}
