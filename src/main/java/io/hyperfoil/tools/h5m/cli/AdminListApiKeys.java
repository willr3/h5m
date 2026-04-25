package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.ApiKeyServiceInterface;
import io.hyperfoil.tools.h5m.entity.ApiKey;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.time.Instant;
import java.util.List;

@CommandLine.Command(name = "list-api-keys", description = "list API keys for a user", mixinStandardHelpOptions = true)
public class AdminListApiKeys implements Runnable {

    @Inject
    ApiKeyServiceInterface apiKeyService;

    @CommandLine.Parameters(index = "0", description = "username")
    public String username;

    @Override
    public void run() {
        List<ApiKey> keys = apiKeyService.listByUser(username);
        Instant now = Instant.now();
        System.out.println(ListCmd.table(100, keys,
                List.of("id", "description", "created", "last_used", "revoked", "expired"),
                List.of(k -> String.valueOf(k.id),
                        k -> k.description != null ? k.description : "",
                        k -> k.createdAt != null ? k.createdAt.toString() : "",
                        k -> k.lastUsedAt != null ? k.lastUsedAt.toString() : "never",
                        k -> String.valueOf(k.revoked),
                        k -> String.valueOf(k.isExpired(now)))));
    }
}
