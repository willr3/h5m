package exp.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import exp.entity.Node;
import exp.entity.NodeGroup;
import exp.entity.node.FingerprintNode;
import exp.entity.node.RelativeDifference;
import exp.svc.NodeGroupService;
import exp.svc.NodeService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name="relativedifference", separator = " ", description = "add a relative difference node", mixinStandardHelpOptions = true)
public class AddRelativeDifference implements Callable<Integer> {

    @CommandLine.Option(names = {"to"},description = "target group / test" ) String groupName;

    @CommandLine.Option(names={"range"}, arity="1",description = "node that produces the value to inspect")
    String rangeName;
    @CommandLine.Option(names={"domain"}, arity="0..1", description = "node used to sort the rang values")
    String domainName;
    @CommandLine.Option(names={"threshold"}, arity="0..1", description = "Maximum difference between the aggregated value of last <window> datapoints and the mean of preceding values.", defaultValue = ""+RelativeDifference.DEFAULT_THRESHOLD)
    double threshold;
    @CommandLine.Option(names={"window"}, arity="0..1",description = "Number of most recent datapoints used for aggregating the value for comparison.", defaultValue = ""+RelativeDifference.DEFAULT_WINDOW)
    int window;
    @CommandLine.Option(names={"minPrevious"}, arity = "0..1", description = "Number of datapoints preceding the aggregation window.", defaultValue = ""+RelativeDifference.DEFAULT_MIN_PREVIOUS)
    int minPrevious;
    @CommandLine.Option(names={"filter"}, arity = "0..1",description = "Function used to aggregate datapoints from the floating window.", defaultValue = RelativeDifference.DEFAULT_FILTER)
    String filter;
    @CommandLine.Option(names={"fingerprint"}, description = "node names to use as fingerprint")
    List<String> fingerprints;

    @CommandLine.Option(names = {"by"},description = "grouping node" ,arity = "0..1")
    public String groupBy;

    @CommandLine.Parameters(index="0",arity="1",description = "node name") String name;

    @Inject
    NodeGroupService nodeGroupService;

    @Inject
    NodeService nodeService;

    @Override
    public Integer call() throws Exception {
        if(name==null || name.isEmpty()){
            System.err.println("missing node name");
            return 1;
        }
        if(groupName==null || groupName.isEmpty()){
            System.err.println("missing group name");
            return 1;
        }
        NodeGroup foundGroup = nodeGroupService.byName(groupName);
        if(foundGroup==null){
            System.err.println("node group with name "+groupName+" does not exist");
            return 1;
        }

        List<Node> foundNodes = nodeService.findNodeByFqdn(name,foundGroup.id);
        if(!foundNodes.isEmpty()){
            System.err.println(groupName+" already has "+name+" node(s)\n  "+foundNodes.stream().map(Node::getFqdn).collect(Collectors.joining("\n  ")));
        }

        if(rangeName==null || rangeName.isEmpty()){
            System.err.println("Missing range");
            return 1;
        }
        foundNodes = nodeService.findNodeByFqdn(rangeName,foundGroup.id);
        if(foundNodes.isEmpty()){
            System.err.println("could not find matching range node by name "+rangeName);
            return 1;
        }else if (foundNodes.size()>1){
            System.err.println("found more than one matching range node by name "+rangeName+"\n  "+foundNodes.stream().map(Node::getFqdn).collect(Collectors.joining("\n  ")));
            return 1;
        }
        Node rangeNode = foundNodes.getFirst();

        Node domainNode = null;
        if(domainName!=null && !domainName.isEmpty()){
            foundNodes = nodeService.findNodeByFqdn(domainName, foundGroup.id);
            if(foundNodes.isEmpty()){
                System.err.println("could not find matching domain node by name "+domainName);
                return 1;
            }else if (foundNodes.size()>1){
                System.err.println("found more than one matching domain node by name "+domainName+"\n  "+foundNodes.stream().map(Node::getFqdn).collect(Collectors.joining("\n  ")));
                return 1;
            }
            domainNode = foundNodes.getFirst();
        }

        Node groupByNode = null;
        if(groupBy!=null && !groupBy.isEmpty()){
            foundNodes = nodeService.findNodeByFqdn(groupBy, foundGroup.id);
            if(foundNodes.isEmpty()){
                System.err.println("could not find matching group by node with name"+groupBy);
                return 1;
            }else if (foundNodes.size()>1){
                System.err.println("found more than one matching group by node for name "+groupBy+"\n  "+foundNodes.stream().map(Node::getFqdn).collect(Collectors.joining("\n  ")));
                return 1;
            }
            groupByNode = foundNodes.getFirst();
        }
        if(groupByNode==null){
            groupByNode = foundGroup.root;
        }

        List<Node> fingerprintNodes = new ArrayList<>();
        if(fingerprints!=null && !fingerprints.isEmpty()){
            List<String> fingerprintNames = fingerprints.stream().flatMap(fp->Arrays.stream(fp.split(","))).map(String::trim).filter(v->!v.isBlank()).toList();
            for(String fingeprintName : fingerprintNames){
                foundNodes = nodeService.findNodeByFqdn(fingeprintName,foundGroup.id);
                if(foundNodes.isEmpty()){
                    System.err.println("could not find matching fingerprint node by name "+fingeprintName);
                    return 1;
                }else if (foundNodes.size()>1){
                    System.err.println("found more than one matching fingerprint node by name "+fingeprintName+"\n  "+foundNodes.stream().map(Node::getFqdn).collect(Collectors.joining("\n  ")));
                    return 1;
                }
                fingerprintNodes.add(foundNodes.getFirst());
            }
        }
        FingerprintNode fingerprintNode = new FingerprintNode("_fp-"+name,"",fingerprintNodes);
        fingerprintNode.group=foundGroup;
        RelativeDifference relDifference = new RelativeDifference();

        relDifference.name=name;
        relDifference.group=foundGroup;
        List<Node> sources = new ArrayList<>();
        sources.add(fingerprintNode);
        sources.add(groupByNode);
        sources.add(rangeNode);
        if(domainNode!=null){
            sources.add(domainNode);
        }
        relDifference.sources=sources;
        relDifference.setFilter(filter);
        relDifference.setThreshold(threshold);
        relDifference.setWindow(window);
        relDifference.setMinPrevious(minPrevious);

        Node created = nodeService.create(relDifference);

        return 0;
    }
}
