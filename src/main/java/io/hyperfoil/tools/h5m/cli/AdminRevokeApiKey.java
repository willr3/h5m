package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.ApiKeyServiceInterface;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "revoke-api-key", description = "revoke an API key", mixinStandardHelpOptions = true)
public class AdminRevokeApiKey implements Callable<Integer> {

    @Inject
    ApiKeyServiceInterface apiKeyService;

    @CommandLine.Parameters(index = "0", description = "API key ID")
    public long keyId;

    @Override
    public Integer call() {
        apiKeyService.revoke(keyId);
        System.out.println("API key " + keyId + " revoked.");
        return 0;
    }
}
