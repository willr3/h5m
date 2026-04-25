package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.svc.ApiKeyServiceInterface;
import io.hyperfoil.tools.h5m.entity.ApiKey;
import io.hyperfoil.tools.h5m.entity.User;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class ApiKeyService implements ApiKeyServiceInterface {

    @ConfigProperty(name = "h5m.api-key.expiration-days", defaultValue = "365")
    long expirationDays;

    @Inject
    UserService userService;

    @Override
    @Transactional
    public String create(String username, String description) {
        User user = userService.byUsername(username);
        if (user == null) {
            throw new IllegalArgumentException("User not found: " + username);
        }
        String rawKey = "H5M_" + UUID.randomUUID().toString().replace("-", "_").toUpperCase();
        ApiKey apiKey = new ApiKey();
        apiKey.keyHash = hashKey(rawKey);
        apiKey.user = user;
        apiKey.description = description;
        apiKey.createdAt = Instant.now();
        apiKey.activeDays = expirationDays;
        apiKey.revoked = false;
        apiKey.persist();
        return rawKey;
    }

    @Override
    @Transactional
    public List<ApiKey> listByUser(String username) {
        return ApiKey.find("user.username", username).list();
    }

    @Override
    @Transactional
    public void revoke(long keyId) {
        ApiKey key = ApiKey.findById(keyId);
        if (key != null) {
            key.revoked = true;
        }
    }

    @Transactional
    public User validateKey(String rawKey) {
        if (rawKey == null || !rawKey.startsWith("H5M_")) {
            return null;
        }
        String hash = hashKey(rawKey);
        ApiKey apiKey = ApiKey.find("keyHash", hash).firstResult();
        if (apiKey == null || apiKey.revoked || apiKey.isExpired(Instant.now())) {
            return null;
        }
        apiKey.recordAccess();
        return apiKey.user;
    }

    static String hashKey(String rawKey) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(rawKey.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
