package io.hyperfoil.tools.h5m.cli;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class CliProfile implements QuarkusTestProfile {

    public static final String TEST_DB_PATH = "/tmp/h5m-test.db";

    @Override
    public String getConfigProfile() {
        return "cli";
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("quarkus.datasource.jdbc.url", "jdbc:sqlite:" + TEST_DB_PATH);
    }
}
