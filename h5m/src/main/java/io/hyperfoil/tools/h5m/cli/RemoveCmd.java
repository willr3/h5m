package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.entity.Folder;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.NodeGroup;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeGroupService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;


@CommandLine.Command(name="remove", description = "remove entity",aliases = {"rm","del","delete"}, mixinStandardHelpOptions = true, subcommands = {RemoveFolder.class, RemoveNode.class})
public class RemoveCmd implements Callable<Integer> {

    @Inject
    FolderService folderService;
    @Inject
    NodeGroupService nodeGroupService;
    @Inject
    NodeService nodeService;

    @CommandLine.Parameters(index="0",arity="0..1")
    public String name;

    @Override
    public Integer call() throws Exception {
        if(name == null) {
            CommandLine cmd = new CommandLine(this);
            cmd.usage(System.out);
            return 0;
        }
        Folder folder = folderService.byName(name);
        NodeGroup nodeGroup = nodeGroupService.byName(name);
        List<Node> nodes = nodeService.findNodeByFqdn(name);

        if (folder != null) {
            if(!nodes.isEmpty()) {
                System.err.println("Cannot delete, matched folder and nodes");
                System.err.println("  folder = "+folder.name);
                nodes.forEach(n-> System.err.println("  node = "+n.getFqdn()));
            }else{
                System.out.println("deleting "+name+" folder");
                folderService.delete(folder);
            }
        } else if (nodeGroup != null) {
            if(!nodes.isEmpty()) {
                System.err.println("Cannot delete, matched node group and nodes");
                System.err.println("  group = "+nodeGroup.name);
                nodes.forEach(n-> System.err.println("  node = "+n.getFqdn()));
            }else{
                System.out.println("deleted "+name+" node group");
                nodeGroupService.delete(nodeGroup);
            }
        } else if (!nodes.isEmpty()) {
            if(nodes.size() != 1){
                System.err.println("Cannot delete, matched multiple and nodes");
                nodes.forEach(n-> System.out.println("  node = "+n.getFqdn()));
            }else{
                System.out.println("deleting "+nodes.get(0).getFqdn()+" node");
                nodeService.delete(nodes.get(0));
            }
        } else {
            System.err.println("failed to match folder, nodegroup, or node with name "+name);
        }


        return 0;
    }
}
