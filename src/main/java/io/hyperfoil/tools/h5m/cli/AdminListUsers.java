package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.UserServiceInterface;
import io.hyperfoil.tools.h5m.entity.User;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.List;

@CommandLine.Command(name = "list-users", description = "list all users", mixinStandardHelpOptions = true)
public class AdminListUsers implements Runnable {

    @Inject
    UserServiceInterface userService;

    @Override
    public void run() {
        List<User> users = userService.list();
        System.out.println(ListCmd.table(80, users,
                List.of("username", "role"),
                List.of(u -> u.username, u -> u.role.name())));
    }
}
