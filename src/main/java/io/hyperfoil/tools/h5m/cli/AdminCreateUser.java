package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.UserServiceInterface;
import io.hyperfoil.tools.h5m.entity.Role;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "create-user", description = "create a new user", mixinStandardHelpOptions = true)
public class AdminCreateUser implements Callable<Integer> {

    @Inject
    UserServiceInterface userService;

    @CommandLine.Parameters(index = "0", description = "username")
    public String username;

    @CommandLine.Option(names = {"--role"}, description = "role (ADMIN or USER)", defaultValue = "USER")
    public Role role;

    @Override
    public Integer call() {
        long id = userService.create(username, role);
        System.out.println("Created user: " + username + " (id=" + id + ", role=" + role + ")");
        return 0;
    }
}
