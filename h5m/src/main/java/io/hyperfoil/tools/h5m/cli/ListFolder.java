package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.svc.FolderService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.ArrayList;
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
    }
}
