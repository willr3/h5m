package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.ApiKeyServiceInterface;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "create-api-key", description = "create an API key for a user", mixinStandardHelpOptions = true)
public class AdminCreateApiKey implements Callable<Integer> {

    @Inject
    ApiKeyServiceInterface apiKeyService;

    @CommandLine.Parameters(index = "0", description = "username")
    public String username;

    @CommandLine.Option(names = {"--description"}, description = "key description", defaultValue = "")
    public String description;

    @Override
    public Integer call() {
        String rawKey = apiKeyService.create(username, description);
        System.out.println("API key created for user: " + username);
        System.out.println("Key: " + rawKey);
        System.out.println("WARNING: This key cannot be retrieved again. Store it securely.");
        return 0;
    }
}
