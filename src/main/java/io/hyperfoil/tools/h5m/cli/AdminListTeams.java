package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.svc.TeamServiceInterface;
import io.hyperfoil.tools.h5m.entity.Team;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.List;

@CommandLine.Command(name = "list-teams", description = "list all teams", mixinStandardHelpOptions = true)
public class AdminListTeams implements Runnable {

    @Inject
    TeamServiceInterface teamService;

    @Override
    public void run() {
        List<Team> teams = teamService.list();
        System.out.println(ListCmd.table(80, teams,
                List.of("name", "members"),
                List.of(t -> t.name, t -> t.members.size())));
    }
}
