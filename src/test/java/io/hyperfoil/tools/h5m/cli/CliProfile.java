package io.hyperfoil.tools.h5m.cli;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class CliProfile implements QuarkusTestProfile {

    public static final String TEST_DB_PATH;

    static {
        try {
            Path dir = Files.createTempDirectory("h5m-test-");
            Path db = dir.resolve("h5m.db");

            dir.toFile().deleteOnExit();
            db.toFile().deleteOnExit();
            dir.resolve("h5m.db-shm").toFile().deleteOnExit();
            dir.resolve("h5m.db-wal").toFile().deleteOnExit();

            TEST_DB_PATH = db.toString();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public String getConfigProfile() {
        return "cli";
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("h5m.jdbc.url", "jdbc:sqlite:" + TEST_DB_PATH);
    }
}
