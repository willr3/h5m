package exp.command;

import exp.entity.Folder;
import exp.entity.Node;
import exp.entity.NodeGroup;
import exp.entity.node.JqNode;
import exp.queue.WorkQueueExecutor;
import exp.svc.*;
import io.hyperfoil.tools.yaup.AsciiArt;
import io.hyperfoil.tools.yaup.json.Json;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.transaction.Transactional;
import org.hibernate.jdbc.WorkExecutor;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@QuarkusMain
@TopCommand
//@Command(name = "", mixinStandardHelpOptions = true)
@CommandLine.Command(name="", mixinStandardHelpOptions = true, subcommands={CommandLine.HelpCommand.class, AutoComplete.GenerateCompletion.class, ListCmd.class, AddCmd.class, RemoveCmd.class})
public class H5m implements QuarkusApplication {

    //@Inject
    FolderService folderService;

    //@Inject
    NodeService nodeService;

    //@Inject
    NodeGroupService nodeGroupService;

    //@Inject
    ValueService valueService;

    //@Inject
    WorkService workService;

    //@Inject
    @Named("workExecutor")
    WorkQueueExecutor workExecutor;

    public static boolean consoleAttached(){
        return System.console() != null;
    }

    @CommandLine.Command(name="sleep",description = "keep the process idle for x seconds")
    public int sleep(int seconds) throws InterruptedException {
        Thread.sleep(Duration.ofSeconds(seconds).toMillis());
        return 0;
    }

    @CommandLine.Command(name="purge-values", description = "remove all values (to re-scan)")
    public int purgeValues(){
        valueService.purgeValues();
        return 0;
    }

    @CommandLine.Command(name="structure",description = "use yaup to compute the structure of a folder",aliases = {"shape"}, mixinStandardHelpOptions = true)
    public int structure(String folderName){
        Folder found = folderService.byName(folderName);
        if(found == null){
            System.err.println("could not find folder "+folderName);
            return 1;
        }
        Json structure = folderService.structure(found);
        System.out.println(structure.toString(2));
        return 0;
    }

    @CommandLine.Command(name="scan",description = "scan folder for new files and compute values")
    public int scan(String folderName) throws InterruptedException {
        Folder folder = folderService.byName(folderName);
        if(folder == null){
            System.err.println("could not find folder "+folderName);
            return 1;
        }
        folderService.scan(folder);
        workExecutor.shutdown();//will wait for idle
        workExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        return 0;
    }

    @Inject
    CommandLine.IFactory factory;

    @Override
    public int run(String... args) throws Exception {
        //becuase no @Inject if we implement QuarkusApplication :(
        this.folderService = CDI.current().select(FolderService.class).get();
        this.nodeGroupService = CDI.current().select(NodeGroupService.class).get();
        this.nodeService = CDI.current().select(NodeService.class).get();
        this.valueService = CDI.current().select(ValueService.class).get();
        this.workService = CDI.current().select(WorkService.class).get();
        this.workExecutor = CDI.current().select(WorkQueueExecutor.class).get();
        System.setProperty("polyglotimpl.DisableClassPathIsolation", "true");
        CommandLine cmd = new CommandLine(this,factory);
        CommandLine gen = cmd.getSubcommands().get("generate-completion");
        gen.getCommandSpec().usageMessage().hidden(true);
        int returnCode = cmd.execute(args);
        workExecutor.shutdown();
        return returnCode;
    }



}
