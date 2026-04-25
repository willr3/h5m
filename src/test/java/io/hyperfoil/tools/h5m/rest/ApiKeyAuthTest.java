package io.hyperfoil.tools.h5m.rest;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.svc.ApiKeyService;
import io.hyperfoil.tools.h5m.svc.SecurityEnabledProfile;
import io.hyperfoil.tools.h5m.svc.UserService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;

@QuarkusTest
@TestProfile(SecurityEnabledProfile.class)
public class ApiKeyAuthTest extends FreshDb {

    @Inject
    UserService userService;

    @Inject
    ApiKeyService apiKeyService;

    @Test
    void write_endpoint_returns_401_without_auth() {
        given()
                .contentType(MediaType.APPLICATION_JSON)
                .when().post("/api/folder/test")
                .then()
                .statusCode(401);
    }

    @Test
    void read_endpoint_returns_200_without_auth() {
        given()
                .when().get("/api/folder")
                .then()
                .statusCode(200);
    }

    @Test
    void write_endpoint_succeeds_with_valid_api_key() {
        userService.create("writer", Role.USER);
        String key = apiKeyService.create("writer", "write key");

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + key)
                .when().post("/api/folder/auth-test")
                .then()
                .statusCode(200);
    }

    @Test
    void write_endpoint_returns_401_with_invalid_key() {
        given()
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer H5M_INVALID_KEY_12345678")
                .when().post("/api/folder/test")
                .then()
                .statusCode(401);
    }

    @Test
    void write_endpoint_returns_401_with_revoked_key() {
        userService.create("revoked-user", Role.USER);
        String key = apiKeyService.create("revoked-user", "revoked key");
        var keys = apiKeyService.listByUser("revoked-user");
        apiKeyService.revoke(keys.get(0).id);

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + key)
                .when().post("/api/folder/test")
                .then()
                .statusCode(401);
    }

    @Test
    void purge_endpoint_returns_403_for_non_admin() {
        userService.create("regular", Role.USER);
        String key = apiKeyService.create("regular", "user key");

        given()
                .header("Authorization", "Bearer " + key)
                .when().delete("/api/value")
                .then()
                .statusCode(403);
    }

    @Test
    void purge_endpoint_succeeds_for_admin() {
        userService.create("admin", Role.ADMIN);
        String key = apiKeyService.create("admin", "admin key");

        given()
                .header("Authorization", "Bearer " + key)
                .when().delete("/api/value")
                .then()
                .statusCode(204);
    }
}
