package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.svc.FolderService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name="folder",description = "remove a folder", mixinStandardHelpOptions = true)
public class RemoveFolder implements Callable<Integer> {

    @Inject
    FolderService folderService;

    @CommandLine.Parameters
    String name;

    @Override
    public Integer call() throws Exception {
        FolderEntity found = folderService.byName(name);
        if(found == null){
            System.err.println("FolderEntity "+name+" not found");
        }else{
            folderService.delete(found);
        }
        return 0;
    }
}
