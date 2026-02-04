package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.svc.NodeService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name="node",separator = " ", description = "remove a node", mixinStandardHelpOptions = true)
public class RemoveNode implements Callable<Integer> {

    @Inject
    NodeService nodeService;

    @CommandLine.Parameters(index="0",arity="1",description = "node name") String name;

    @CommandLine.Option(names = {"from"},description = "target group / test",arity = "0..1") String groupName;

    @Override
    public Integer call() throws Exception {

        if(groupName != null){
            name = groupName+ Node.FQDN_SEPARATOR+name;
        }
        List<Node> found = nodeService.findNodeByFqdn(name);
        if(found==null || found.isEmpty()) {
            System.err.println("could not find " + name);
            return 1;
        }else if (found.size()>1){
            System.err.println("found too many matching nodes");
            found.forEach(System.out::println);
            return 1;
        }else{
            nodeService.delete(found.get(0));
        }
        return 0;
    }
}
