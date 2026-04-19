package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.TeamServiceInterface;
import io.hyperfoil.tools.h5m.api.svc.UserServiceInterface;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.User;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "add-member", description = "add a user to a team", mixinStandardHelpOptions = true)
public class AdminAddMember implements Callable<Integer> {

    @Inject
    TeamServiceInterface teamService;

    @Inject
    UserServiceInterface userService;

    @CommandLine.Parameters(index = "0", description = "username")
    public String username;

    @CommandLine.Parameters(index = "1", description = "team name")
    public String teamName;

    @Override
    public Integer call() {
        User user = userService.byUsername(username);
        if (user == null) {
            System.err.println("User not found: " + username);
            return 1;
        }
        Team team = teamService.byName(teamName);
        if (team == null) {
            System.err.println("Team not found: " + teamName);
            return 1;
        }
        teamService.addMember(team.id, user.id);
        System.out.println("Added " + username + " to team " + teamName);
        return 0;
    }
}
