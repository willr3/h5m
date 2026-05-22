package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.h5m.svc.ValueService;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import picocli.CommandLine;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "verifyimport", description = "Compare h5m imported data against Horreum source data")
public class VerifyLegacy implements Callable<Integer> {

    @CommandLine.Option(names = {"username"}, description = "legacy db username", defaultValue = "quarkus")
    String username;
    @CommandLine.Option(names = {"password"}, description = "legacy db password", defaultValue = "quarkus")
    String password;
    @CommandLine.Option(names = {"url"}, description = "legacy connection url", defaultValue = "jdbc:postgresql://0.0.0.0:6000/horreum")
    String url;
    @CommandLine.Option(names = {"testId"}, description = "Horreum test ID")
    Long testId;
    @CommandLine.Option(names = {"runId"}, description = "verify a specific run (optional)")
    Long runId;
    @CommandLine.Option(names = {"limit"}, description = "max runs to verify", defaultValue = "5")
    int limit;
    @CommandLine.Option(names = {"verbose"}, description = "show detailed mismatch info", defaultValue = "false")
    boolean verbose;

    @Inject
    EntityManager em;

    @Inject
    FolderService folderService;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    @Transactional
    public Integer call() throws Exception {
        if (testId == null) {
            System.err.println("testId is required");
            return 1;
        }

        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, "1");
        props.put(AgroalPropertiesReader.MIN_SIZE, "1");
        props.put(AgroalPropertiesReader.INITIAL_SIZE, "1");
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.PRINCIPAL, username);
        props.put(AgroalPropertiesReader.CREDENTIAL, password);
        props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME, "org.postgresql.Driver");
        props.put(AgroalPropertiesReader.JDBC_URL, url);
        AgroalDataSource legacyDs = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props).get());

        try (Connection legacyConn = legacyDs.getConnection()) {
            String testName = getTestName(legacyConn, testId);
            if (testName == null) {
                System.err.println("Test not found: " + testId);
                return 1;
            }
            System.out.println("Verifying test: " + testName + " (id=" + testId + ")");

            // Step 1: Compare node structure
            System.out.println("\n=== NODE STRUCTURE ===");
            compareNodeStructure(legacyConn, testName);

            // Step 2: Get runs to verify
            List<Long> runIds;
            if (runId != null) {
                runIds = List.of(runId);
            } else {
                runIds = getRunIds(legacyConn, testId, limit);
            }
            System.out.println("\n=== VERIFYING " + runIds.size() + " RUNS ===");

            int totalMatches = 0;
            int totalMismatches = 0;
            int totalMissing = 0;
            int totalExtra = 0;
            int totalMisaligned = 0;
            Map<String, int[]> perLabel = new LinkedHashMap<>();  // label → [match, mismatch, missing]
            Map<Integer, int[]> perDataset = new TreeMap<>();     // ordinal → [match, mismatch, missing]
            long startTime = System.currentTimeMillis();

            for (Long rid : runIds) {
                System.out.println("\n--- Run " + rid + " ---");
                int[] result = compareRun(legacyConn, testName, rid, perLabel, perDataset);
                totalMatches += result[0];
                totalMismatches += result[1];
                totalMissing += result[2];
                totalExtra += result[3];
                totalMisaligned += result[4];
            }

            long elapsed = System.currentTimeMillis() - startTime;
            int totalComparisons = totalMatches + totalMismatches + totalMissing;
            double matchRate = totalComparisons > 0 ? 100.0 * totalMatches / totalComparisons : 0;

            // Summary
            System.out.println("\n=== SUMMARY ===");
            System.out.println("Runs verified: " + runIds.size());
            System.out.printf("Match rate: %.1f%% (%d/%d)%n", matchRate, totalMatches, totalComparisons);
            System.out.println("  matching:   " + totalMatches);
            System.out.println("  mismatched: " + totalMismatches);
            System.out.println("  missing:    " + totalMissing + " (" + totalMisaligned + " misaligned, " + (totalMissing - totalMisaligned) + " absent)");
            System.out.println("  extra:      " + totalExtra);
            System.out.printf("Time: %.1fs%n", elapsed / 1000.0);

            // Per-label breakdown (only show labels with issues)
            List<String> problemLabels = perLabel.entrySet().stream()
                    .filter(e -> e.getValue()[1] > 0 || e.getValue()[2] > 0)
                    .map(Map.Entry::getKey).toList();
            if (!problemLabels.isEmpty()) {
                System.out.println("\n--- Labels with issues ---");
                System.out.printf("  %-40s %6s %8s %7s %6s%n", "Label", "Match", "Mismatch", "Missing", "Rate");
                for (String label : problemLabels) {
                    int[] s = perLabel.get(label);
                    int total = s[0] + s[1] + s[2];
                    System.out.printf("  %-40s %6d %8d %7d %5.0f%%%n", truncate(label, 40), s[0], s[1], s[2],
                            total > 0 ? 100.0 * s[0] / total : 0);
                }
            }
            List<String> perfectLabels = perLabel.entrySet().stream()
                    .filter(e -> e.getValue()[1] == 0 && e.getValue()[2] == 0 && e.getValue()[0] > 0)
                    .map(Map.Entry::getKey).toList();
            if (!perfectLabels.isEmpty()) {
                System.out.println("\n--- Labels with 100% match (" + perfectLabels.size() + ") ---");
                System.out.println("  " + String.join(", ", perfectLabels));
            }

            // Per-dataset breakdown (only show datasets with issues)
            List<Integer> problemDatasets = perDataset.entrySet().stream()
                    .filter(e -> e.getValue()[1] > 0 || e.getValue()[2] > 0)
                    .map(Map.Entry::getKey).toList();
            if (!problemDatasets.isEmpty()) {
                System.out.println("\n--- Datasets with issues ---");
                System.out.printf("  %-12s %6s %8s %7s%n", "Dataset", "Match", "Mismatch", "Missing");
                for (int ds : problemDatasets) {
                    int[] s = perDataset.get(ds);
                    System.out.printf("  %-12d %6d %8d %7d%n", ds, s[0], s[1], s[2]);
                }
            }

            if (totalMismatches == 0 && totalMissing == 0) {
                System.out.println("\nRESULT: PASS");
                return 0;
            } else {
                System.out.println("\nRESULT: DIFFERENCES FOUND");
                return 1;
            }
        } finally {
            legacyDs.close();
        }
    }

    private String getTestName(Connection conn, long testId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM test WHERE id = ?")) {
            ps.setLong(1, testId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private List<Long> getRunIds(Connection conn, long testId, int limit) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM run WHERE testid = ? AND trashed = false ORDER BY id DESC LIMIT ?")) {
            ps.setLong(1, testId);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) ids.add(rs.getLong(1));
            }
        }
        return ids;
    }

    private void compareNodeStructure(Connection legacyConn, String testName) throws SQLException {
        // Horreum: count labels for this test's target schema
        int horreumLabelCount = 0;
        try (PreparedStatement ps = legacyConn.prepareStatement("""
                SELECT count(DISTINCT l.id) FROM label l
                JOIN transformer t ON l.schema_id = t.schema_id
                JOIN test_transformers tt ON tt.transformer_id = t.id
                WHERE tt.test_id = ?
                """)) {
            ps.setLong(1, testId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) horreumLabelCount = rs.getInt(1);
            }
        }

        // Also count labels from run schemas (no-transform path)
        int horreumSchemaLabelCount = 0;
        try (PreparedStatement ps = legacyConn.prepareStatement("""
                SELECT count(DISTINCT l.id) FROM label l
                JOIN schema s ON l.schema_id = s.id
                WHERE s.uri IN (SELECT DISTINCT schema::text FROM run_schema_paths WHERE testid = ?)
                """)) {
            ps.setLong(1, testId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) horreumSchemaLabelCount = rs.getInt(1);
            }
        }

        // h5m: count nodes by type
        @SuppressWarnings("unchecked")
        List<Object[]> h5mNodes = em.createNativeQuery("""
                SELECT n.type, count(*) FROM node n
                JOIN node_group ng ON n.group_id = ng.id
                JOIN folder f ON f.group_id = ng.id
                WHERE f.name = ?
                GROUP BY n.type ORDER BY n.type
                """)
                .setParameter(1, testName)
                .getResultList();

        System.out.println("Horreum labels (transformer schema): " + horreumLabelCount);
        System.out.println("Horreum labels (run schemas): " + horreumSchemaLabelCount);
        System.out.println("h5m nodes:");
        int totalNodes = 0;
        for (Object[] row : h5mNodes) {
            System.out.println("  " + row[0] + ": " + row[1]);
            totalNodes += ((Number) row[1]).intValue();
        }
        System.out.println("  total: " + totalNodes);

        // Count change detection
        int horreumChangeDetections = 0;
        try (PreparedStatement ps = legacyConn.prepareStatement("""
                SELECT count(*) FROM changedetection
                WHERE variable_id IN (SELECT id FROM variable WHERE testid = ?)
                """)) {
            ps.setLong(1, testId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) horreumChangeDetections = rs.getInt(1);
            }
        }

        long h5mChangeDetections = 0;
        try {
            h5mChangeDetections = ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM node n
                    JOIN node_group ng ON n.group_id = ng.id
                    JOIN folder f ON f.group_id = ng.id
                    WHERE f.name = ? AND n.type IN ('rd', 'ft')
                    """)
                    .setParameter(1, testName)
                    .getSingleResult()).longValue();
        } catch (Exception e) {
            // folder may not exist
        }

        System.out.println("Change detection nodes: Horreum=" + horreumChangeDetections + " h5m=" + h5mChangeDetections);
    }

    private int[] compareRun(Connection legacyConn, String testName, long runId,
                             Map<String, int[]> perLabel, Map<Integer, int[]> perDataset) throws SQLException {
        int matches = 0, mismatches = 0, missing = 0, extra = 0, misaligned = 0;

        // Get Horreum label values for this run
        Map<String, Map<Integer, String>> horreumValues = new LinkedHashMap<>();
        try (PreparedStatement ps = legacyConn.prepareStatement("""
                SELECT ds.ordinal, l.name, lv.value::text
                FROM label_values lv
                JOIN label l ON l.id = lv.label_id
                JOIN dataset ds ON ds.id = lv.dataset_id
                WHERE ds.runid = ? AND ds.testid = ?
                ORDER BY ds.ordinal, l.name
                """)) {
            ps.setLong(1, runId);
            ps.setLong(2, testId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int ordinal = rs.getInt(1);
                    String labelName = rs.getString(2);
                    String value = rs.getString(3);
                    horreumValues.computeIfAbsent(labelName, k -> new TreeMap<>()).put(ordinal, value);
                }
            }
        }

        int datasetCount = 0;
        try (PreparedStatement ps = legacyConn.prepareStatement(
                "SELECT count(*) FROM dataset WHERE runid = ? AND testid = ?")) {
            ps.setLong(1, runId);
            ps.setLong(2, testId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) datasetCount = rs.getInt(1);
            }
        }

        // Get h5m values per label, ordered by idx (which maps to dataset ordinal)
        @SuppressWarnings("unchecked")
        List<Object[]> h5mValues = em.createNativeQuery("""
                SELECT n.name, v.idx, v.data::text
                FROM value v
                JOIN node n ON v.node_id = n.id
                JOIN node_group ng ON n.group_id = ng.id
                JOIN folder f ON f.group_id = ng.id
                WHERE f.name = ?
                AND n.type NOT IN ('root')
                ORDER BY n.name, v.idx
                """)
                .setParameter(1, testName)
                .getResultList();

        Map<String, Map<Integer, String>> h5mByLabel = new LinkedHashMap<>();
        for (Object[] row : h5mValues) {
            String name = (String) row[0];
            int idx = ((Number) row[1]).intValue();
            String value = (String) row[2];
            h5mByLabel.computeIfAbsent(name, k -> new TreeMap<>()).put(idx, value);
        }

        System.out.println("  Horreum: " + datasetCount + " datasets, " + horreumValues.size() + " labels");
        System.out.println("  h5m: " + h5mByLabel.size() + " label nodes with values");

        // Compare labels that exist in Horreum, checking each dataset ordinal
        for (String labelName : horreumValues.keySet()) {
            Map<Integer, String> horreumOrdinals = horreumValues.get(labelName);
            Map<Integer, String> h5mOrdinals = h5mByLabel.getOrDefault(labelName, Map.of());

            for (Map.Entry<Integer, String> entry : horreumOrdinals.entrySet()) {
                int ordinal = entry.getKey();
                String horreumValue = entry.getValue();
                if ("null".equals(horreumValue)) continue;

                // h5m idx starts at 1 (idx 0 = root), Horreum ordinal starts at 0
                // Find the h5m value at the matching position
                String h5mValue = null;
                int h5mIdx = ordinal + 1;
                if (h5mOrdinals.containsKey(h5mIdx)) {
                    h5mValue = h5mOrdinals.get(h5mIdx);
                } else if (!h5mOrdinals.isEmpty()) {
                    // Try matching by position in the ordered map
                    List<String> h5mList = new ArrayList<>(h5mOrdinals.values());
                    if (ordinal < h5mList.size()) {
                        h5mValue = h5mList.get(ordinal);
                    }
                }

                if (h5mValue != null) {
                    String hNorm = normalizeValue(horreumValue);
                    String h5mNorm = normalizeValue(h5mValue);
                    if (hNorm.equals(h5mNorm)) {
                        matches++;
                        track(perLabel, labelName, 0);
                        track(perDataset, ordinal, 0);
                        if (verbose) {
                            System.out.println("  OK       " + labelName + "[" + ordinal + "] = " + truncate(horreumValue, 80));
                        }
                    } else {
                        mismatches++;
                        track(perLabel, labelName, 1);
                        track(perDataset, ordinal, 1);
                        System.out.println("  MISMATCH " + labelName + " (dataset " + ordinal + "):");
                        System.out.println("           horreum = " + truncate(horreumValue, 120));
                        System.out.println("           h5m     = " + truncate(h5mValue, 120));
                        if (verbose) {
                            System.out.println("           h5m idx = " + h5mIdx);
                            System.out.println("           h5m has " + h5mOrdinals.size() + " values at indices: " + h5mOrdinals.keySet());
                        }
                    }
                } else {
                    missing++;
                    track(perLabel, labelName, 2);
                    track(perDataset, ordinal, 2);
                    // Check if the value exists at a different idx (wrong position) or not at all
                    String hNorm = normalizeValue(horreumValue);
                    String foundAt = null;
                    if (!h5mOrdinals.isEmpty()) {
                        for (Map.Entry<Integer, String> h5mEntry : h5mOrdinals.entrySet()) {
                            if (hNorm.equals(normalizeValue(h5mEntry.getValue()))) {
                                foundAt = "idx=" + h5mEntry.getKey();
                                break;
                            }
                        }
                    }
                    if (foundAt != null) {
                        misaligned++;
                        System.out.println("  MISALIGN " + labelName + " (dataset " + ordinal + "): value exists at " + foundAt);
                    } else if (!h5mOrdinals.isEmpty()) {
                        System.out.println("  MISMATCH " + labelName + " (dataset " + ordinal + ", h5m idx " + h5mIdx + "): node has values but none match");
                    } else {
                        System.out.println("  MISSING  " + labelName + " (dataset " + ordinal + ", h5m idx " + h5mIdx + "): no values in h5m");
                    }
                    if (verbose && !h5mOrdinals.isEmpty()) {
                        System.out.println("           h5m has values at indices: " + h5mOrdinals.keySet());
                    }
                }
            }
        }

        // Check for extra h5m labels not in Horreum
        for (String name : h5mByLabel.keySet()) {
            if (!horreumValues.containsKey(name) && isLabelName(name)) {
                extra++;
            }
        }
        if (extra > 0) {
            System.out.println("  EXTRA    " + extra + " labels in h5m not in Horreum");
        }

        System.out.println("  Result: " + matches + " match, " + mismatches + " mismatch, " + missing + " missing (" + misaligned + " misaligned), " + extra + " extra");
        return new int[]{matches, mismatches, missing, extra, misaligned};
    }

    private static <K> void track(Map<K, int[]> map, K key, int idx) {
        map.computeIfAbsent(key, k -> new int[3])[idx]++;
    }

    private static String normalizeValue(String value) {
        if (value == null) return "null";
        // Remove quotes for string comparison
        if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        }
        // Truncate long values (objects/arrays) to a hash for comparison
        if (value.length() > 200) {
            return "HASH:" + value.hashCode();
        }
        return value.trim();
    }

    private static String truncate(String s, int len) {
        if (s == null) return "null";
        return s.length() > len ? s.substring(0, len) + "..." : s;
    }

    private static boolean isLabelName(String name) {
        // Heuristic: label names typically start with uppercase or are known patterns
        return name.matches("^[A-Z].*") || name.contains(" ");
    }
}
