package io.hyperfoil.tools.h5m.svc;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class SecurityEnabledProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("h5m.security.enabled", "true");
    }
}
