package io.hyperfoil.tools.h5m.cli;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "admin",
        description = "admin operations",
        mixinStandardHelpOptions = true,
        subcommands = {
                AdminCreateTeam.class,
                AdminCreateUser.class,
                AdminListTeams.class,
                AdminListUsers.class,
                AdminAddMember.class,
        }
)
public class AdminCmd implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        CommandLine cmd = new CommandLine(this);
        cmd.usage(System.out);
        return 0;
    }
}
