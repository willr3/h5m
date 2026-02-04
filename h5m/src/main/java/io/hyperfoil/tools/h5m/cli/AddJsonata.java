package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.NodeGroup;
import io.hyperfoil.tools.h5m.entity.node.JsonataNode;
import io.hyperfoil.tools.h5m.svc.NodeGroupService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;

@CommandLine.Command(name="jsonata", separator = " ", description = "add jsonata node", mixinStandardHelpOptions = true)
public class AddJsonata implements Callable<Integer> {

    @CommandLine.Option(names = {"to"},description = "target group / test" ) String groupName;
    @CommandLine.Parameters(index="0",arity="1",description = "node name") String name;
    @CommandLine.Parameters(index="1",arity="1",description = "jq filter") String jsonata;

    @Inject
    NodeGroupService nodeGroupService;

    @Inject
    NodeService nodeService;

    @Override
    public Integer call() throws IOException {

        if(name == null || name.isBlank()){
            System.err.println("missing jsonata node name");
            return 1;
        }
        if(name.matches("\\d+")){
            System.err.println("nodes names cannot be numbers");
            return 1;
        }
        if("-".equals(jsonata)){
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                    sb.append(System.lineSeparator());
                }
            }
            if(sb.length()>0){
                jsonata = sb.toString().trim();
            }else{
                System.err.println("unable to read function from input");
                return 1;
            }
        }
        if(jsonata == null || jsonata.isEmpty()){
            System.err.println("missing jsonata operation");
            return 1;
        }
        if(groupName == null){
            System.err.println("missing group name");
            return 1;
        }
        NodeGroup foundGroup =  nodeGroupService.byName(groupName);
        if(foundGroup == null){
            System.err.println("group not found");
            return 1;
        }

        Node node = JsonataNode.parse(name,jsonata, n->nodeService.findNodeByFqdn(n, foundGroup.id));
        if(node == null){
            System.err.println("cannot create node from jsonata\n"+jsonata);
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
