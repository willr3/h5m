package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.NodeGroup;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.svc.NodeGroupService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.concurrent.Callable;

@CommandLine.Command(name="jq", separator = " ", description = "add jq node", mixinStandardHelpOptions = true)
public class AddJq implements Callable<Integer> {

    @CommandLine.Option(names = {"to"},description = "target group / test" ) String groupName;
    @CommandLine.Parameters(index="0",arity="0..1",description = "node name") String name;
    @CommandLine.Parameters(index="1",arity="0..1",description = "jq filter") String jq;

    @Inject
    NodeGroupService nodeGroupService;

    @Inject
    NodeService nodeService;

    @Override
    public Integer call() throws Exception {
        Scanner sc = new Scanner(System.in);
        if(name == null && H5m.consoleAttached()){
            System.out.printf("Enter name: ");
            name = sc.nextLine();
        }
        NodeGroup foundGroup = null;
        do{
            if(groupName == null && H5m.consoleAttached()){
                System.out.printf("Enter target group / folder name: ");
                groupName = sc.nextLine();
            }
            foundGroup =  nodeGroupService.byName(groupName);
            if(foundGroup == null){
                System.err.println("could not find "+groupName);
                groupName = null;
            }
        }while(groupName == null && H5m.consoleAttached());

        if("-".equals(jq)){
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                    sb.append(System.lineSeparator());
                }
            }
            if(sb.length()>0){
                jq = sb.toString().trim();
            }else{
                System.err.println("unable to read function from input");
                return 1;
            }
        }
        if(jq == null && H5m.consoleAttached()){
            System.out.printf("Enter jq filter: ");
            jq = sc.nextLine();
        }
        NodeGroup staticFoundGroup = foundGroup;
        Node node = JqNode.parse(name,jq, n->nodeService.findNodeByFqdn(n,staticFoundGroup.id));
        if(node == null){
            System.err.println("cannot create node from jq="+jq);
            return 1;
        }
        node.group=foundGroup;
        if(node.sources.isEmpty()){
            node.sources.add(foundGroup.root);
        }
        nodeService.create(node);
        return 0;
    }
}
