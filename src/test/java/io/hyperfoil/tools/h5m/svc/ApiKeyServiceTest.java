package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.ApiKey;
import io.hyperfoil.tools.h5m.entity.Role;
import io.hyperfoil.tools.h5m.entity.User;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(SecurityEnabledProfile.class)
public class ApiKeyServiceTest extends FreshDb {

    @Inject
    ApiKeyService apiKeyService;

    @Inject
    UserService userService;

    @Test
    void create_key_returns_prefixed_string() {
        userService.create("alice", Role.USER);
        String key = apiKeyService.create("alice", "test key");
        assertNotNull(key);
        assertTrue(key.startsWith("H5M_"));
    }

    @Test
    void create_key_stores_hash_not_raw() {
        userService.create("bob", Role.USER);
        String rawKey = apiKeyService.create("bob", "my key");
        List<ApiKey> keys = apiKeyService.listByUser("bob");
        assertEquals(1, keys.size());
        assertNotEquals(rawKey, keys.get(0).keyHash);
    }

    @Test
    void validate_key_returns_user() {
        userService.create("carol", Role.USER);
        String rawKey = apiKeyService.create("carol", "valid key");
        User user = apiKeyService.validateKey(rawKey);
        assertNotNull(user);
        assertEquals("carol", user.username);
    }

    @Test
    void validate_key_returns_null_for_unknown() {
        assertNull(apiKeyService.validateKey("H5M_UNKNOWN_KEY_VALUE_HERE"));
    }

    @Test
    void validate_key_returns_null_for_invalid_prefix() {
        assertNull(apiKeyService.validateKey("INVALID_KEY"));
    }

    @Test
    void validate_key_returns_null_for_revoked() {
        userService.create("dave", Role.USER);
        String rawKey = apiKeyService.create("dave", "revoke me");
        List<ApiKey> keys = apiKeyService.listByUser("dave");
        apiKeyService.revoke(keys.get(0).id);
        assertNull(apiKeyService.validateKey(rawKey));
    }

    @Test
    void validate_key_updates_lastUsedAt() {
        userService.create("eve", Role.USER);
        String rawKey = apiKeyService.create("eve", "track access");
        List<ApiKey> before = apiKeyService.listByUser("eve");
        assertNull(before.get(0).lastUsedAt);

        apiKeyService.validateKey(rawKey);

        List<ApiKey> after = apiKeyService.listByUser("eve");
        assertNotNull(after.get(0).lastUsedAt);
    }

    @Test
    void list_keys_by_user() {
        userService.create("frank", Role.USER);
        apiKeyService.create("frank", "key one");
        apiKeyService.create("frank", "key two");
        List<ApiKey> keys = apiKeyService.listByUser("frank");
        assertEquals(2, keys.size());
    }

    @Test
    void revoke_key() {
        userService.create("grace", Role.USER);
        apiKeyService.create("grace", "to revoke");
        List<ApiKey> keys = apiKeyService.listByUser("grace");
        assertFalse(keys.get(0).revoked);

        apiKeyService.revoke(keys.get(0).id);

        keys = apiKeyService.listByUser("grace");
        assertTrue(keys.get(0).revoked);
    }

    @Test
    void create_key_throws_for_unknown_user() {
        assertThrows(IllegalArgumentException.class,
                () -> apiKeyService.create("nonexistent", "key"));
    }
}
