package exp.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import exp.entity.Node;
import exp.entity.NodeGroup;
import exp.entity.Value;
import exp.svc.NodeGroupService;
import exp.svc.NodeService;
import exp.svc.ValueService;
import io.hyperfoil.tools.yaup.json.Json;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import picocli.CommandLine;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CommandLine.Command(name="value", aliases = {"values"}, separator = " ", description = "list values", sortOptions = false, mixinStandardHelpOptions = true)
public class ListValue implements Callable<Integer> {

    @CommandLine.ParentCommand
    ListCmd parent;

    @Inject
    NodeService nodeService;

    @Inject
    NodeGroupService nodeGroupService;

    @Inject
    ValueService valueService;

    public enum Format { raw, table }

    @CommandLine.Option(names = {"as"},description = "presentation option: ${COMPLETION-CANDIDATES}", arity = "0..1", order=3,defaultValue = "raw")
    Format format;

    @CommandLine.Option(names = {"by"},description = "grouping node" ,arity = "0..1", order = 2)
    public String groupBy;

    @CommandLine.Option(names = {"from"},description = "group name" ,arity = "0..1", order = 1)
    public String groupName;


    public ListValue() {}
    public ListValue(String groupName,String groupBy) {
        this.groupName = groupName;
        this.groupBy = groupBy;
    }

    @Override
    @Transactional
    public Integer call() throws Exception {
        groupName = groupName==null ? parent.name: groupName;
        if(groupName==null){
            CommandLine cmd = new CommandLine(this);
            cmd.usage(System.err);
            return 1;
        }
        NodeGroup nodeGroup = nodeGroupService.byName(groupName);
        if(nodeGroup == null){
            System.err.println("Node group "+groupName+" not found");
            return 1;
        }

        if(groupBy!=null){
            List<Node> foundNodes = nodeService.findNodeByFqdn(groupBy,nodeGroup.id);
            if(foundNodes.isEmpty()){
                System.err.println(groupBy+" not found");
                return 1;
            }else if (foundNodes.size()>1){
                System.err.println(groupBy+" is ambiguous, matched the following nodes:");
                for(int i=0;i<foundNodes.size();i++){
                    System.err.printf("%3d %s",i,foundNodes.get(i).name);
                }
                return 1;
            }else{
                Node foundNode = foundNodes.get(0);
                List<JsonNode> jsons = valueService.getGroupedValues(foundNode);
                if(Format.raw.equals(format)){
                    System.out.println(ListCmd.table(80, jsons,
                            List.of("data"),
                            List.of(JsonNode::toString)));
                }else{
                    Set<String> keys = new HashSet<>();
                    for(JsonNode json : jsons){
                        if(json.isObject()){
                            ObjectNode object = (ObjectNode) json;
                            for(Iterator<String> iter = object.fieldNames(); iter.hasNext();){
                                String key = iter.next();
                                if(!key.startsWith("_")){
                                    keys.add(iter.next());
                                }
                            }
                        }
                    }
                    List<String> keyList = new ArrayList<>(keys);
                    keyList.sort(String.CASE_INSENSITIVE_ORDER);
                    List<Function<JsonNode,Object>> accessors = keyList.stream().map(name-> (Function<JsonNode, Object>) json -> json.get(name).toString()).toList();
                    System.out.println(ListCmd.table(80, jsons, keyList, accessors));
                }

            }
        }else {

            List<Value> values = valueService.getDescendantValues(nodeGroup.root);
            System.out.println("Count: " + values.size());
            System.out.println(ListCmd.table(80, values,
                    List.of("id", "data", "node.id"),
                    List.of(v -> v.id, v -> v.data, v -> v.node.id)));
        }
        return 0;
    }
}
