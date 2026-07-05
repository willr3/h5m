package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.WorkService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

@CommandLine.Command(name="load-legacy-runs")
public class LoadLegacyRuns implements Callable<Integer> {

    @Inject
    FolderService folderService;

    @Inject
    WorkService workService;

    @CommandLine.Option(names = {"username"}, description = "legacy db username", defaultValue = "quarkus") String username;
    @CommandLine.Option(names = {"password"}, description = "legacy db password", defaultValue = "quarkus") String password;
    @CommandLine.Option(names = {"url"}, description = "legacy connection url",defaultValue = "jdbc:postgresql://0.0.0.0:") String url;
    @CommandLine.Option(names = {"testId"}, description = "specify which test(s) to load. Comma-separated for multiple. Loads all if unspecified", split = ",") List<Long> testId;
    @CommandLine.Option(names = {"limit"}, description = "max runs to load", defaultValue = "-1") int limit;
    @CommandLine.Option(names = {"offset"}, description = "how many runs to skip ", defaultValue = "-1") int offset;
    @CommandLine.Option(names = {"batch"}, description = "max runs to batch at once", defaultValue = "-1") int batch;
    @CommandLine.Option(names = {"pause"}, description = "pause for user input after every batch", defaultValue = "false") boolean pause;

    @Override
    public Integer call() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, "1");
        props.put(AgroalPropertiesReader.MIN_SIZE, "1");
        props.put(AgroalPropertiesReader.INITIAL_SIZE, "1");
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.PRINCIPAL,username); //username
        props.put(AgroalPropertiesReader.CREDENTIAL,password);//password
        props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.postgresql.Driver");
        props.put(AgroalPropertiesReader.JDBC_URL, url );
        AgroalDataSource ds  = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props)
                .get());

        Map<Long,String> tests = new LinkedHashMap<>();
        try(Connection connection = ds.getConnection()){
            if(testId!=null && !testId.isEmpty()){
                for (Long id : testId) {
                    try(PreparedStatement statement = connection.prepareStatement("select name from test where id = ?")){
                        statement.setLong(1, id);
                        try (ResultSet rs = statement.executeQuery()){
                            while(rs.next()){
                                tests.put(id, rs.getString("name"));
                            }
                        }
                    }
                }
            }else {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet rs = statement.executeQuery("select id,name from test")) {
                        while (rs.next()) {
                            tests.put(rs.getLong(1), rs.getString(2));
                        }
                    }
                }
            }
            System.out.println("loaded "+tests.size()+" legacy tests");
            for(Long testId : tests.keySet()){
                String name = tests.get(testId);
                Folder folder = folderService.byName(name);
                if(folder == null){
                    System.out.println("Failed to find Folder for test "+name+" id="+testId);
                    continue;
                }
                // Phase 1: Fetch run IDs (lightweight, no large data transfer)
                String idQuery = "select id from run where testid = ? and trashed = false order by id desc";
                if (limit > 0) idQuery += " limit ?";
                if (offset > 0) idQuery += " offset ?";
                List<Long> runIds = new ArrayList<>();
                try (PreparedStatement ps = connection.prepareStatement(idQuery)) {
                    ps.setLong(1, testId);
                    int paramIdx = 2;
                    if (limit > 0) ps.setInt(paramIdx++, limit);
                    if (offset > 0) ps.setInt(paramIdx, offset);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            runIds.add(rs.getLong(1));
                        }
                    }
                }
                System.out.println("loading " + runIds.size() + " uploads to " + name);

                // Phase 2: Process in batches, fetching data per batch
                int batchSize = batch > 0 ? batch : runIds.size();
                int count = 0;
                Scanner scanner = new Scanner(System.in);
                for (int batchStart = 0; batchStart < runIds.size(); batchStart += batchSize) {
                    if (Thread.interrupted()) throw new InterruptedException("Import interrupted");

                    int batchEnd = Math.min(batchStart + batchSize, runIds.size());
                    List<Long> batchIds = runIds.subList(batchStart, batchEnd);

                    // Fetch data for this batch only (short-lived query, no long cursor)
                    String placeholders = String.join(",", batchIds.stream().map(id -> "?").toList());
                    List<CompletableFuture<Void>> batchFutures = new ArrayList<>();
                    try (PreparedStatement ps = connection.prepareStatement(
                            "select id, data from run where id in (" + placeholders + ")")) {
                        for (int i = 0; i < batchIds.size(); i++) {
                            ps.setLong(i + 1, batchIds.get(i));
                        }
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                long id = rs.getLong(1);
                                System.out.println(name + " " + id);
                                // Parse directly from bytes — avoids UTF-8→char decoding,
                                // StringBuilder doubling, and String copy that the previous
                                // getCharacterStream() path required.
                                byte[] bytes = rs.getBytes("data");
                                JqValue data = JqValues.parse(bytes);
                                batchFutures.add(folderService.upload(folder.name(), null, data).future);
                                count++;
                            }
                        }
                    }

                    // Wait for this batch to complete before fetching next
                    if (!batchFutures.isEmpty()) {
                        System.out.println("waiting for batch of " + batchFutures.size() + " to complete");
                        CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]))
                                .orTimeout(10, TimeUnit.MINUTES)
                                .join();
                        System.out.println("batch complete");
                    }

                    if (pause) {
                        scanner.nextLine();
                    }
                }
                System.out.println("loaded " + count + " runs");
            }
        } finally {
            ds.close();
        }
        // Wait for the work queue to drain — async cascade workers may still be
        // processing after the upload futures complete. Without this, the CLI
        // process exits and CDI context is destroyed while workers are active.
        System.out.println("waiting for work queue to drain...");
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        int stableChecks = 0;
        while (stableChecks < 5 && System.currentTimeMillis() < deadline) {
            if (workService.isIdle()) {
                stableChecks++;
            } else {
                stableChecks = 0;
            }
            try { Thread.sleep(200); } catch (InterruptedException e) { break; }
        }
        System.out.println("work queue drained");
        return 0;
    }
}
