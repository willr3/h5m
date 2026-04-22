package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.api.NodeGroup;
import io.hyperfoil.tools.h5m.api.svc.NodeGroupServiceInterface;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.aesh.util.graph.GraphStyle;
import picocli.CommandLine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.aesh.util.graph.Graph;
import org.aesh.util.graph.GraphNode;

@CommandLine.Command(name="nodes", separator = " ", description = "list nodes", mixinStandardHelpOptions = true)
public class ListNode implements Callable<Integer> {

    @CommandLine.ParentCommand
    ListCmd parent;

    @Inject
    NodeGroupServiceInterface nodeGroupService;

    public static enum Render {Table, Graph};

    @CommandLine.Option(names = {"from"},description = "group name", arity="0..1") String groupName;

    @CommandLine.Option(names = {"as"}, description = "Valid values: ${COMPLETION-CANDIDATES}\")", defaultValue = "Table") Render render;

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
            System.err.println("NodeEntity group "+groupName+" not found");
            return 1;
        }
        if(render.equals(Render.Graph)){
            GraphNode rootNode = GraphNode.of("root");
            Map<Node,GraphNode> nodes = new HashMap<>();
            nodes.put(nodeGroup.root(), rootNode);
                    for(Node source: nodeGroup.sources()){
                    walk(source,nodes);
                }
            System.out.println(Graph.render(rootNode, GraphStyle.ROUNDED));
        }else {
            System.out.println(
                ListCmd.table(80,nodeGroup.sources(),List.of("name","type","fqdn","operation"),
                    List.of(Node::name,
                        n->n.type().display(),
                        Node::fqdn,
                        Node::operation
                    )
                )
            );
        }

        return 0;
    }

    public GraphNode walk(Node node, Map<Node,GraphNode> nodes){
        if(nodes.containsKey(node)){
            return nodes.get(node);
        }else{
            GraphNode rtrn = GraphNode.of(node.name());
            nodes.put(node, rtrn);
            for(Node source: node.sources()){
                    GraphNode fromSource = walk(source,nodes);
                    fromSource.child(rtrn);
                }
            return rtrn;
        }
    }

}
