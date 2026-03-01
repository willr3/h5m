package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.h5m.entity.Folder;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.hyperfoil.tools.h5m.svc.*;
import io.hyperfoil.tools.yaup.json.Json;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import picocli.AutoComplete;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@QuarkusMain
@TopCommand
@CommandLine.Command(name="", mixinStandardHelpOptions = true,separator = " ", subcommands={CommandLine.HelpCommand.class, AutoComplete.GenerateCompletion.class, ListCmd.class, AddCmd.class, RemoveCmd.class, LoadLegacyTests.class, LoadLegacyRuns.class})
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

    @CommandLine.Command(name="purge-values", description = "remove all values (to re-upload)")
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
    @CommandLine.Command(name="recalculate",description = "recalculate values for all entries in folder")
    public int recalculate(String folderName) throws InterruptedException {
        Folder folder = folderService.byName(folderName);
        if(folder == null){
            System.err.println("could not find folder "+folderName);
            return 1;
        }
        folderService.recalculate(folder);
        return 0;
    }
    @CommandLine.Command(name="upload",description = "")
    public int upload(
            @CommandLine.Parameters(index="0")
            String path,
            @CommandLine.Option(names = {"to"},description = "grouping node" ,arity = "1")
            String folderName
    ){
        Folder folder = folderService.byName(folderName);
        if(folder == null){
            System.err.println("could not find folder "+folderName);
            return 1;
        }
        File pathFile  = new File(path);
        if(!pathFile.exists()){
            System.err.println("upload path does not exist: "+path);
            return 1;
        }
        List<File> todo = pathFile.isDirectory() ? List.of(pathFile.listFiles(s->s.toString().endsWith(".json") && !s.getName().startsWith("."))): List.of(pathFile);
        ObjectMapper objectMapper = new ObjectMapper();
        for( File f : todo){
            try {
                if( todo.size()>1) {
                    System.out.println(f.getName());
                }
                JsonNode read = objectMapper.readTree(f);
                if(read!=null){
                    folderService.upload(folder,f.getPath(),read);
                }else{
                    System.err.println(f.getPath()+" could not be loaded as json");
                }
            } catch (IOException e) {
                System.err.println("failure trying to read "+f.getPath()+"\n"+e.getMessage());
                return 1;
            }
        }
        return 0;
    }

    @Inject
    CommandLine.IFactory factory;

    @Override
    public int run(String... args) throws Exception {
        //because no @Inject if we implement QuarkusApplication :(
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
        workExecutor.awaitTermination(1,TimeUnit.HOURS);
        return returnCode;
    }



}
