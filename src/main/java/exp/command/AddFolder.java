package exp.command;

import exp.entity.Folder;
import exp.entity.NodeGroup;
import exp.svc.FolderService;
import exp.svc.NodeGroupService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.Callable;

@CommandLine.Command(name="folder", description = "add a folder", mixinStandardHelpOptions = true)
public class AddFolder implements Callable<Integer> {

    @Inject
    FolderService folderService;

    @Inject
    NodeGroupService nodeGroupService;

    @CommandLine.Parameters(index="0",arity="0..1")
    public String name;

    @Override
    public Integer call() throws Exception {
        if(name == null && H5m.consoleAttached()){
            Scanner sc = new Scanner(System.in);
            System.out.printf("Enter name: ");
            name = sc.nextLine();
        }
        NodeGroup existingGroup =  nodeGroupService.byName(name);
        if(existingGroup != null){
            System.err.println(name+" conflicts with an existing node group");
            return 1;
        }
        Folder newFolder = new Folder(name);
        folderService.create(newFolder);
        return 0;
    }
}
