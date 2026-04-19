package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.TeamServiceInterface;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "create-team", description = "create a new team", mixinStandardHelpOptions = true)
public class AdminCreateTeam implements Callable<Integer> {

    @Inject
    TeamServiceInterface teamService;

    @CommandLine.Parameters(index = "0", description = "team name")
    public String name;

    @Override
    public Integer call() {
        long id = teamService.create(name);
        System.out.println("Created team: " + name + " (id=" + id + ")");
        return 0;
    }
}
