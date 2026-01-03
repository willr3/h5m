package exp.command;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
    name="add",
    description = "add entity",
    mixinStandardHelpOptions = true,
    subcommands = {
        AddFolder.class,
        AddJq.class,
        AddJs.class,
        AddJsonata.class,
        AddSqlJsonpath.class,
        AddRelativeDifference.class,
    }
)
public class AddCmd implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        CommandLine cmd = new CommandLine(this);
        cmd.usage(System.out);
        return 0;
    }
}
