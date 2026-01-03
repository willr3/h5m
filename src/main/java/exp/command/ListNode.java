package exp.command;

import exp.entity.Node;
import exp.entity.NodeGroup;
import exp.svc.NodeGroupService;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name="nodes", separator = " ", description = "list nodes", mixinStandardHelpOptions = true)
public class ListNode implements Callable<Integer> {

    @CommandLine.ParentCommand
    ListCmd parent;

    @Inject
    NodeGroupService nodeGroupService;

    @CommandLine.Option(names = {"from"},description = "group name", arity="0..1") String groupName;

    @Override
    @Transactional
    public Integer call() throws Exception {
        groupName = groupName==null ? parent.name : groupName;
        if(groupName == null){
            CommandLine cmd = new CommandLine(this);
            cmd.usage(System.err);
            return 1;
        }
        NodeGroup nodeGroup = nodeGroupService.byName(groupName);
        if(nodeGroup == null){
            System.err.println("Node group "+groupName+" not found");
            return 1;
        }
        System.out.println(
            ListCmd.table(80,nodeGroup.sources,List.of("name","type","fqdn","operation"),
                List.of(n->n.name,
                n->n.type,
                Node::getFqdn,
                n->n.operation
                )
            )
        );
        return 0;
    }



}
