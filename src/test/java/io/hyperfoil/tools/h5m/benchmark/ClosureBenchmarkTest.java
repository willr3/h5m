package io.hyperfoil.tools.h5m.benchmark;

import io.hyperfoil.tools.jjq.value.*;
import io.agroal.api.AgroalDataSource;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.WorkService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for comparing main vs closure table branch.
 * Uploads 20 rhivos runs (cycling through 5 run files × 4 times) and measures:
 * - Total upload time
 * - DB table sizes (pre/post VACUUM FULL)
 * - Row counts
 * <p>
 * Run with: mvn test -Dtest=ClosureBenchmarkTest -Dsurefire.excludes=""
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ClosureBenchmarkTest extends FreshDb {

    private static final int TOTAL_UPLOADS = 20;

    private static final String[] RHIVOS_FILES = {
            "/rhivos/40375.json",
            "/rhivos/40376.json",
            "/rhivos/40377.json",
            "/rhivos/46013.json",
            "/rhivos/46014.json"
    };

    @Inject
    FolderService folderService;

    @Inject
    WorkService workService;

    @Inject
    TransactionManager tm;

    @Inject
    AgroalDataSource ds;

    @Test
    @Order(1)
    void upload_20_rhivos_runs() throws Exception {
        // Import the node graph
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        // Pre-load all run data into memory
        JqValue[] runData = new JqValue[RHIVOS_FILES.length];
        for (int i = 0; i < RHIVOS_FILES.length; i++) {
            try (InputStream is = getClass().getResourceAsStream(RHIVOS_FILES[i])) {
                runData[i] = JqValues.parse(is.readAllBytes());
            }
        }

        // Upload 20 runs, cycling through the 5 files
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_UPLOADS; i++) {
            int fileIndex = i % RHIVOS_FILES.length;
            long uploadStart = System.currentTimeMillis();

            folderService.upload("rhivos-perf-comprehensive", "$", runData[fileIndex])
                    .future.orTimeout(120, TimeUnit.SECONDS).join();

            long uploadEnd = System.currentTimeMillis();
            System.out.printf("[BENCHMARK] Upload %d/%d (%s): %d ms%n",
                    i + 1, TOTAL_UPLOADS, RHIVOS_FILES[fileIndex], uploadEnd - uploadStart);
        }

        long totalTime = System.currentTimeMillis() - startTime;
        System.out.printf("[BENCHMARK] Total upload time for %d runs: %d ms (%.1f s)%n",
                TOTAL_UPLOADS, totalTime, totalTime / 1000.0);

        // Report row counts
        reportRowCounts();

        // Report table sizes before vacuum
        System.out.println("\n[BENCHMARK] === TABLE SIZES (before VACUUM FULL) ===");
        reportTableSizes();

        // VACUUM FULL all tables and report again
        vacuumFull();
        System.out.println("\n[BENCHMARK] === TABLE SIZES (after VACUUM FULL) ===");
        reportTableSizes();
    }

    private void reportRowCounts() throws SQLException {
        System.out.println("\n[BENCHMARK] === ROW COUNTS ===");
        String[] tables = {"node", "node_edge", "value", "value_edge"};
        for (String table : tables) {
            long count = countRows(table);
            System.out.printf("[BENCHMARK] %-15s %,d rows%n", table, count);
        }
    }

    private void reportTableSizes() throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            String sql = """
                SELECT
                  t.table_name,
                  pg_size_pretty(pg_total_relation_size(quote_ident(t.table_name))) as total_size,
                  pg_total_relation_size(quote_ident(t.table_name)) as total_bytes,
                  pg_size_pretty(pg_relation_size(quote_ident(t.table_name))) as heap_size,
                  pg_size_pretty(pg_indexes_size(quote_ident(t.table_name))) as index_size,
                  pg_size_pretty(
                    pg_total_relation_size(quote_ident(t.table_name))
                    - pg_relation_size(quote_ident(t.table_name))
                    - pg_indexes_size(quote_ident(t.table_name))
                  ) as toast_size
                FROM information_schema.tables t
                WHERE t.table_schema = 'public'
                  AND t.table_type = 'BASE TABLE'
                ORDER BY pg_total_relation_size(quote_ident(t.table_name)) DESC
                """;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                System.out.printf("[BENCHMARK] %-25s %12s %12s %12s %12s%n",
                        "TABLE", "TOTAL", "HEAP", "INDEX", "TOAST");
                long grandTotal = 0;
                while (rs.next()) {
                    String name = rs.getString("table_name");
                    String total = rs.getString("total_size");
                    long totalBytes = rs.getLong("total_bytes");
                    String heap = rs.getString("heap_size");
                    String index = rs.getString("index_size");
                    String toast = rs.getString("toast_size");
                    grandTotal += totalBytes;
                    System.out.printf("[BENCHMARK] %-25s %12s %12s %12s %12s%n",
                            name, total, heap, index, toast);
                }
                System.out.printf("[BENCHMARK] %-25s %12s%n",
                        "GRAND TOTAL", formatBytes(grandTotal));
            }
        }
    }

    private void vacuumFull() throws SQLException {
        System.out.println("\n[BENCHMARK] Running VACUUM FULL on all tables...");
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                String[] tables = {"value", "value_edge", "node", "node_edge",
                        "folder", "node_group", "upload_processing",
                        "folder_view", "folder_view_component"};
                for (String table : tables) {
                    long start = System.currentTimeMillis();
                    stmt.execute("VACUUM FULL " + table);
                    long elapsed = System.currentTimeMillis() - start;
                    System.out.printf("[BENCHMARK] VACUUM FULL %-25s %d ms%n", table, elapsed);
                }
            }
        }
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f kB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
