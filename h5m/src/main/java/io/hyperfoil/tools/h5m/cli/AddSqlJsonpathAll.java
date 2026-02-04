package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.NodeGroup;
import io.hyperfoil.tools.h5m.entity.node.SqlJsonpathAllNode;
import io.hyperfoil.tools.h5m.svc.NodeGroupService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name="sqlpathall", separator = " ", description = "add sql jsonpath node", mixinStandardHelpOptions = true)
public class AddSqlJsonpathAll implements Callable<Integer> {

    @CommandLine.Option(names = {"to"},description = "target group / test" ) String groupName;
    @CommandLine.Parameters(index="0",arity="1",description = "node name") String name;
    @CommandLine.Parameters(index="1",arity="1",description = "jsonpath") String jsonpath;

    @Inject
    NodeGroupService nodeGroupService;

    @Inject
    NodeService nodeService;

    @Override
    public Integer call() throws Exception {

        if(name == null){
            System.err.println("missing node name");
            return 1;
        }
        if(groupName == null){
            System.err.println("missing group name");
            return 1;
        }
        NodeGroup foundGroup = nodeGroupService.byName(groupName);
        if(foundGroup == null){
            System.err.println("could not find target group/test "+groupName);
            return 1;
        }
        if(jsonpath == null){
            System.err.println("missing jsonpath");
            return 1;
        }
        Node node = SqlJsonpathAllNode.parse(name,jsonpath, n->nodeService.findNodeByFqdn(n,foundGroup.id));
        if(node == null){
            System.err.println("could not create node from "+jsonpath);
            return 1;
        }
        node.group = foundGroup;
        if(node.sources.isEmpty()){
            node.sources.add(foundGroup.root);
        }
        nodeService.create(node);
        return 0;
    }

}
