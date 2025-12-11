package exp.command;

import exp.svc.FolderService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CommandLine.Command(name="folder", aliases={"folders"}, description = "list folders", mixinStandardHelpOptions = true)
public class ListFolder implements Runnable {

    @CommandLine.ParentCommand
    ListCmd listCmd;

    @Inject
    FolderService folderService;

    @Override
    public void run() {
        Map<String,Integer> folderCounts = folderService.getFolderUploadCount();
        List<String> names = new ArrayList<>(folderCounts.keySet());
        names.sort(String.CASE_INSENSITIVE_ORDER);
        System.out.println(ListCmd.table(80,names,List.of("name","uploads"), List.of(Object::toString, folderCounts::get)));
        //folderService.list().forEach(System.out::println);
    }
}
