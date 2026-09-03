package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.jjq.jsonpath.JsonpathToJq;
import io.hyperfoil.tools.jjq.value.JqArray;
import io.hyperfoil.tools.jjq.value.JqObject;
import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.api.ReservedNamespace;
import io.hyperfoil.tools.h5m.api.View;
import io.hyperfoil.tools.h5m.api.ViewComponent;
import io.hyperfoil.tools.h5m.api.node.RelativeDifferenceConfig;
import io.hyperfoil.tools.h5m.api.svc.ViewServiceInterface;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.yaup.Counters;
import io.hyperfoil.tools.yaup.HashedLists;
import io.hyperfoil.tools.yaup.HashedSets;
import io.hyperfoil.tools.yaup.StringUtil;
import jakarta.inject.Inject;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;


import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@CommandDefinition(name = "load-tests", description = "Import test definitions (folder + node graph) from a legacy Horreum PostgreSQL database", generateHelp = true)
public class LoadLegacyTests implements Command<H5mCommandInvocation> {

    public static final String DEFAULT_PREFIX = "_";

    @Option(name = "username", acceptNameWithoutDashes = true, description = "legacy db username", defaultValue = "quarkus")
    String username;

    @Option(name = "password", acceptNameWithoutDashes = true, description = "legacy db password", defaultValue = "quarkus")
    String password;

    @Option(name = "url", acceptNameWithoutDashes = true, description = "legacy connection url", defaultValue = "jdbc:postgresql://0.0.0.0:5432/horreum")
    String url;

    @Option(name = "testId", acceptNameWithoutDashes = true, description = "specify which test to load. Loads all if unspecified")
    Long testId;

    @Option(name = "keepAll", acceptNameWithoutDashes = true, description = "set all nodes to keep", defaultValue = "false")
    boolean keepAll;

    @Option(name = "extractor-prefix", acceptNameWithoutDashes = true, description = "optional prefix for all extractor names", defaultValue = "")
    String extractorPrefix = ""; //setting value for @Inject testing in LoadLegacyTestsTest

    @Option(name = "combined-label-prefix", acceptNameWithoutDashes = true, description = "optional prefix for labels combined during schema merge",defaultValue = "")
    String combinedLabelPrefix = "";



    public static String printTest(Test t){
        StringBuilder sb = new StringBuilder();
        sb.append("Test.id="+t.id()+" name="+t.name()+"\n");
        if(!t.transformers().isEmpty()) {
            sb.append("transformers:\n");
            t.transformers().forEach(transformer -> {
                sb.append("  " + transformer.name() + " .id=" + transformer.id() + "\n");
                if (!transformer.extractors().isEmpty()) {
                    sb.append("    extractors:\n");
                    transformer.extractors().forEach(extractor -> {
                        sb.append("      " + extractor.name() + " .isArray=" + extractor.isArray() + "\n");
                    });
                }
            });
        }
        if(!t.schemaPaths().isEmpty()){
            sb.append("schemaPaths:\n");
            t.schemaPaths().forEach((k,lst)->{
                sb.append("  "+k+"\n");
                lst.forEach(lbl->{
                    sb.append("    "+lbl.name()+" .id="+lbl.id()+"\n");
                    if(!lbl.extractors().isEmpty()){
                        sb.append("      extractors:\n");
                        lbl.extractors().forEach(extractor -> {
                            sb.append("        "+extractor.name()+" .isArray="+extractor.isArray()+"\n");
                        });
                    }
                });
            });
        }
        if(!t.variables().isEmpty()){
            sb.append("variables:\n");
            t.variables().forEach(variable->{
                sb.append("  "+variable.name()+" .id="+variable.id()+"\n");
                if(!variable.labels().isEmpty()){
                    sb.append("    labels:\n");
                    variable.labels().forEach(label->{
                        sb.append("      "+label+"\n");
                    });
                }
            });
        }
        if(!t.fingerprints().isEmpty()){
            sb.append("fingerprints:\n");
            t.fingerprints().forEach(fingerprint->{
                sb.append("  "+fingerprint.labels()+" "+fingerprint.filter()+"\n");
            });
        }
        if(!t.changeDetections().isEmpty()){
            sb.append("changeDetections:\n");
            t.changeDetections().forEach(changeDetection->{
                sb.append("  "+changeDetection.id()+" "+changeDetection.model()+" "+changeDetection.variableId()+" "+changeDetection.config()+"\n");
            });
        }
        return sb.toString();
    }

    public static class NodeTracking {

        //The same extractor can have more than 1 NodeEntity depending on sources :(
        HashedLists<Extractor,NodeEntity> extractorNodes = new HashedLists<>();
        Map<Label,NodeEntity> labelToNodes = new HashMap<>();
        // Use IdentityHashMap because NodeEntity.hashCode() changes when id is
        // set by em.persist() — a regular HashMap loses entries whose keys were
        // inserted before persistence (hashCode was based on name+operation)
        // and looked up after persistence (hashCode is based on id).
        Map<NodeEntity,Label> nodeToLabel = new IdentityHashMap<>();
        HashedLists<String,NodeEntity> nodesByName = new HashedLists<>();

        public void tagNodeAsExtractor(Extractor extractor, NodeEntity node) {
            extractorNodes.put(extractor,node);
        }
        public void tagNodeAsLabel(Label label,NodeEntity node){
            labelToNodes.put(label,node);
            nodeToLabel.put(node,label);
            // Label-equivalent nodes should retain their data after processing.
            // Without KEEP, the ephemeral system nullifies them as "intermediate"
            // nodes, but in Horreum these are the final label values.
            node.ephemeral = EphemeralMode.KEEP;
        }
        public void renameNode(NodeEntity node,String oldName){
            nodesByName.remove(oldName,node);
            addNode(node);
        }
        public Set<NodeEntity> getAllNodes(){
            HashSet<NodeEntity> nodes = new HashSet<>();
            nodesByName.values().forEach(nodes::addAll);
            return nodes;
        }
        public void addNode(NodeEntity node){
            nodesByName.put(node.name, node);
        }
        public boolean hasAnyNode(Extractor extractor){
            return extractorNodes.containsKey(extractor);
        }
        public boolean hasNode(Extractor extractor,NodeEntity source){
            return extractorNodes.containsKey(extractor) && extractorNodes.get(extractor).stream().anyMatch(n->n.sources.contains(source));
        }
        public boolean hasNode(Label label){
            return labelToNodes.containsKey(label);
        }
        public boolean hasNode(String name){
            return nodesByName.containsKey(name);
        }
        public NodeEntity getNode(Extractor extractor,NodeEntity source){
            return extractorNodes.get(extractor).stream().filter(n->n.sources.contains(source)).findFirst().orElse(null);
        }
        public NodeEntity getNode(Label label){
            return labelToNodes.get(label);
        }
        public List<NodeEntity> getNodes(String name){
            return nodesByName.get(name);
        }
        public List<NodeEntity> getLabelNodes(String name){
            return getNodes(name).stream().filter(nodeToLabel::containsKey).collect(Collectors.toList());
        }

    }

    @Inject
    FolderService folderService;

    @Inject
    NodeService nodeService;

    @Inject
    ViewServiceInterface viewService;

    /** Result of creating a folder with its label-to-node tracking data */
    record FolderImportResult(FolderEntity folder, NodeTracking nodeTracking) {}

    public static String pad(int pad,String message){
        if(pad==0){
            return message;
        }else{
            String padding = String.format("%"+pad+"s","");
            return padding+message.replaceAll("\n","\n"+padding);
        }
    }
    public static void log(String message){
        log(0,message);
    }
    public static void log(int pad,String message){
        System.out.println(pad(pad,message));
    }


    //select fingerprint_labels, fingerprint_filter, timeline_labels, timeline_function
    public record Fingerprint(List<String> labels,String filter,List<String> timelineLabels, String timelineFunction){};
    //select id,variable_id,model,config
    public record ChangeDetection(long id,long variableId,String model,JqObject config){};
    public record Test(long id, String name, HashedSets<String,Label> schemaPaths, List<Fingerprint> fingerprints, List<ChangeDetection> changeDetections, List<Transformer> transformers, List<Variable> variables){};
    public record Extractor(String name,String jsonpath,boolean isArray){};
    public record Transformer(long id,String name,String function,String targetUri, List<Extractor> extractors,List<Label> targetSchemaLabels){
        @Override
        public boolean equals(Object o) {
            if(o instanceof Transformer t){
                boolean matching = name.equals(t.name) && function.equals(t.function) && targetUri.equals(t.targetUri);
                if(!matching){
                    return false;
                }
                if(extractors.size()!=t.extractors.size()){
                    return false;
                }
                for(int i=0;i<extractors.size();i++){
                    if(!extractors.get(i).equals(t.extractors.get(i))){
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, function, targetUri, extractors);
        }
    };
    public record LabelDef(long id,String name,String function){};
    public record Label(long id,String name,String function,List<Extractor> extractors){

        @Override
        public boolean equals(Object o){
            if(o instanceof Label l){
                if(!l.name.equals(name)){
                    return false;
                }
                if(JsNode.isNullEmptyOrIdentityFunction(function) != JsNode.isNullEmptyOrIdentityFunction(l.function)){
                    return false;
                }
                if(function!=null && !function.equals(l.function) && !JsNode.isNullEmptyOrIdentityFunction(l.function)){
                    return false;
                }
                if(extractors.size()!=l.extractors.size()){
                    return false;
                }
                for(int i=0;i<extractors.size();i++){
                    if(!extractors.get(i).equals(l.extractors.get(i))){
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        @Override
        public int hashCode(){
            return Objects.hash(name,JsNode.isNullEmptyOrIdentityFunction(function) ? null : function,extractors);
        }
    };

    //id,name,labels,calculation
    public record Variable(long id,String name,List<String> labels,String calculation){};

    public boolean createSameValue(NodeEntity a, NodeEntity b){
        if((b == null && a != null) || (a== null && b !=null)){
            return false;
        }
        if(!a.operation.equals(b.operation)){
            return false;
        }
        if(a.sources.size()!=b.sources.size()){
            return false;
        }
        for(int i=0; i<a.sources.size();i++){
            NodeEntity aSource = a.sources.get(i);
            NodeEntity bSource = b.sources.get(i);
            if(!aSource.equals(bSource)){
                return false;
            }
        }
        return true;
    }

    public NodeEntity createNodesFromLabel(Label label, NodeEntity source, NodeGroupEntity group, NodeTracking nodeTracking, Set<String> usedNames, boolean multiSchema){

        // Fix A: Skip labels with no extractors — these are placeholder/stub labels
        // from schemas like rhivos-perf-comprehensive:02 that contribute no data
        if (label.extractors.isEmpty()) {
            return null;
        }
        NodeEntity rtrn = null;
        Map<String,String> extractorRenames = new HashMap<>();
        HashedLists<String,NodeEntity> labelNodesByName = new HashedLists<>();
        boolean reusedNode = false;
        for(Extractor extractor : label.extractors) {
            String extractorName = getExtractorRename(extractor.name);
            if(usedNames.contains(extractorName)){
                extractorName = DEFAULT_PREFIX+extractorName;
                //if this extractor conflicts with a label name
            }

            if(!extractorName.equals(extractor.name)){
                extractorRenames.put(extractor.name, extractorName);
            }

            // Convert jsonpath to jq — sqlall (isArray) wraps in [...] to collect all matches
            String jqOperation = extractor.isArray
                    ? io.hyperfoil.tools.jjq.jsonpath.JsonpathToJq.convertArray(extractor.jsonpath())
                    : io.hyperfoil.tools.jjq.jsonpath.JsonpathToJq.convert(extractor.jsonpath());
            NodeEntity node = JqNode.parse(extractorName, jqOperation, nodeTracking::getNodes);
            if (node == null) {
                System.err.println("ERROR: Failed to create node for extractor " + extractor);
                return null;
            }
            if(keepAll){
                node.ephemeral = EphemeralMode.KEEP;
            }
            node.sources = List.of(source);

            //the name is different but then we change the name and it becomes functionally equivalent...
            //nodeService.functionalyEquivalent(node,nodeTracking.getNode(extractor))
            if(nodeTracking.hasAnyNode(extractor) && createSameValue(node,nodeTracking.getNode(extractor,source))){
                node = nodeTracking.getNode(extractor,source);
                if(!node.name.equals(extractor.name)){
                    extractorRenames.put(extractor.name, node.name);
                }
                reusedNode = true;
            }else{
                group.addNode(node);
                nodeTracking.addNode(node);
                nodeTracking.tagNodeAsExtractor(extractor, node);
            }
            labelNodesByName.put(node.name, node);
        }
        //this could rename a node that is referenced by name by another node! Don't do that!
        if(label.function==null || label.function.trim().isEmpty() || JsNode.isNullEmptyOrIdentityFunction(label.function)){
            if(label.extractors.size() == 1 && label.name!=null && !label.name.trim().isEmpty()){
                String extractorName = getExtractorRename(label.extractors.get(0).name);
                List<NodeEntity> extractorNodes = labelNodesByName.get(extractorName);
                if(extractorNodes.isEmpty() && nodeTracking.hasNode(label.extractors.get(0),source)){
                    extractorNodes=List.of(nodeTracking.getNode(label.extractors.get(0),source));
                }
                if(extractorNodes.size()==1){
                    NodeEntity extractorNode = extractorNodes.get(0);
                    if (!reusedNode && !multiSchema && !label.name.equals( extractorRenames.get(extractorNode.name) ) ){
                        // Only rename if the extractor isn't shared with another label
                        // and this label is NOT part of a multi-schema group (where
                        // renaming would create name collisions between variants)
                        String previousName = extractorNode.name;
                        extractorNode.name = label.name;
                        nodeTracking.renameNode(extractorNode,previousName);
                        rtrn = extractorNode;
                    }else{
                        //we need a node with a name that matches the label
                        rtrn = new JqNode(label.name,".",extractorNodes);
                        if(keepAll){
                            rtrn.ephemeral = EphemeralMode.KEEP;
                        }
                        nodeTracking.addNode(rtrn);
                        group.addNode(rtrn);
                    }
                }else{
                    System.out.println("ERROR: FAILED TO FIND SINGLE EXTRACTOR "+label.extractors.get(0).name+" for label "+label.name+
                            "\nextractors:\n  "+label.extractors.stream().map(Objects::toString).collect(Collectors.joining("\n  "))+
                            "\nfound:\n  "+extractorNodes.stream().map(NodeEntity::toString).collect(Collectors.joining("\n  "))+
                            "\nlabelNodes:\n  "+labelNodesByName.keys().stream().map(s->"  "+s+":\n    "+labelNodesByName.get(s).stream().map(NodeEntity::toString).collect(Collectors.joining("\n      "))).collect(Collectors.joining("\n  ")));
                }
            }else if (label.function==null || label.function.trim().isEmpty()){
                if(extractorRenames.isEmpty()){
                    rtrn = new JsNode(label.name(),"combined=>combined",labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                }else{
                    //we have to restore the names for the merged object
                    //TODO this only works if all attributes are renamed, need an alg that works for rename and not-renamed
                    StringBuilder sb = new StringBuilder("(args)=>({");
                    sb.append(extractorRenames.keySet().stream().map(k->"...(args['"+extractorRenames.get(k)+"'] != undefined && {'"+k+"':args['"+extractorRenames.get(k)+"']})").collect(Collectors.joining(", ")));
                    sb.append("})");
                    rtrn = new JsNode(label.name(),sb.toString(),labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                }
                if(keepAll){
                    rtrn.ephemeral = EphemeralMode.KEEP;
                }
                nodeTracking.addNode(rtrn);
                group.addNode(rtrn);
            }
        }else {
            // For import, we know the sources — they are the extractor nodes we just
            // created. No need to call JsNode.parse() (which tries to match JS parameter
            // names to node names — designed for user-created nodes, not import).
            // createParameters() handles building the correct JS input object at runtime,
            // whether the function uses named property access (value["key"]) or positional.
            List<NodeEntity> sources = labelNodesByName.values().stream()
                    .flatMap(List::stream).collect(Collectors.toList());
            // Try converting simple JS patterns to jq to avoid GraalVM Truffle
            // interpreter overhead (issue #247). Only single-source labels with
            // known patterns are converted.
            String labelFunction = label.function;
            if(!extractorRenames.isEmpty()){
                labelFunction = NodeService.renameParameters(labelFunction, extractorRenames);
            }
            String jqEquivalent = sources.size() == 1 ? JsToJqPatterns.tryConvert(labelFunction) : null;
            if (jqEquivalent != null) {
                rtrn = new JqNode(label.name, jqEquivalent, sources);
            } else {
                rtrn = new JsNode(label.name, labelFunction, sources);
            }
            if(keepAll){
                rtrn.ephemeral = EphemeralMode.KEEP;
            }
            if(rtrn!=null){
                nodeTracking.addNode(rtrn);
                group.addNode(rtrn);
            }
        }
        return rtrn;
    }

    public static String sanitizeName(String input){
        return input.replaceAll("[^a-zA-Z0-9_$]","_");
    }
    public String getLabelRename(String label){
        if(combinedLabelPrefix!=null && !combinedLabelPrefix.isEmpty()){
            return combinedLabelPrefix+label;
        }else {
            return label;
        }
    }
    public String getExtractorRename(String extractorName){
        if(extractorPrefix!=null && !extractorPrefix.isEmpty()){
            return extractorPrefix + extractorName;
        }else {
            return extractorName;
        }
    }
    public static String getRename(Transformer transformer,int count){
        String transformerSuffix = count > 1 ? "_" + transformer.id() : "";
        String name = "transformer_"+sanitizeName(transformer.name)+transformerSuffix;
        return name;
    }
    public FolderImportResult createFolder(Test test){
        FolderEntity folder = new FolderEntity();
        folder.name = test.name;
        folder.group = new NodeGroupEntity(test.name);

        NodeTracking nodeTracking = new NodeTracking();

        NodeEntity startingNode = folder.group.root;
        Set<String> labelNames = new HashSet<>();
        for(Transformer t : test.transformers()){
                labelNames.addAll(t.targetSchemaLabels().stream().map(label->label.name).collect(Collectors.toSet()));
        }
        test.schemaPaths.forEach((p,lbls)->{
            lbls.forEach(lbl -> labelNames.add(lbl.name));
        });

        if(!test.transformers().isEmpty()){
            // Phase 1: Create transformer nodes for each transformer
            List<NodeEntity> transformerNodes = new ArrayList<>();
            for(Transformer transformer : test.transformers){
                Label l  = new Label(-1,getRename(transformer,test.transformers().size()),transformer.function,transformer.extractors);
                NodeEntity transform = createNodesFromLabel(l,folder.group.root,folder.group,nodeTracking,labelNames, false);
                folder.group.addNode(transform);
                nodeTracking.addNode(transform);
                transformerNodes.add(transform);

                // All transformers target the same schema, so labels go under the root
                // schema path. Using "$" ensures the schemaPaths loop below sources
                // labels from startingNode (the dataset) directly, not from a
                // transformer-named intermediate JQ node.
                test.schemaPaths.putAll("$.\"$schema\"",transformer.targetSchemaLabels);
            }

            // Phase 2: Create a dataset node from the transformer output(s).
            // Single transformer: JQ split (array → individual items).
            // Multiple transformers: JS generator that merges outputs from all
            // transformers — each transformer produces an array, the generator
            // interleaves array elements and yields flat workload objects (not
            // wrapped in transformer name keys).
            if (transformerNodes.size() == 1) {
                startingNode = new JqNode("dataset","if type == \"array\" then .[] else . end",List.of(transformerNodes.get(0)));
                if(keepAll){
                    startingNode.ephemeral = EphemeralMode.KEEP;
                }

                folder.group.addNode(startingNode);
                nodeTracking.addNode(startingNode);
            } else {
                // Iterate object values (one per transformer), unwrap arrays, yield flat items
                startingNode = new JqNode("dataset", "[.[] | if type == \"array\" then .[] else . end][]", transformerNodes);
                if(keepAll){
                    startingNode.ephemeral = EphemeralMode.KEEP;
                }

                folder.group.addNode(startingNode);
                nodeTracking.addNode(startingNode);
            }

        }
        if(!test.schemaPaths().isEmpty()){
            List<String> schemaPaths = new ArrayList<>(test.schemaPaths().keys());
            schemaPaths.sort(String::compareTo);
            //stores nodes renamed because multiple labels shared that name
            HashedLists<String,NodeEntity> nodesByOriginalName = new HashedLists<>();

            HashedLists<String,Label> labelsByName = new HashedLists<>();
            test.schemaPaths.forEach((p,lbls)->{
                lbls.forEach(lbl -> labelsByName.put(lbl.name,lbl));
            });
            for(String jsonpath : schemaPaths){
                List<Label> labelsForJsonpath = new ArrayList<>( test.schemaPaths().get(jsonpath) );
                log(2,jsonpath+" -> "+" "+labelsForJsonpath.size()+" label(s)");
                if(test.schemaPaths().get(jsonpath).isEmpty()){
                    continue;
                }
                NodeEntity sourceNode = startingNode;
                if(!jsonpath.equals("$.\"$schema\"")){
                    String sourcePath = JsonpathToJq.convert( jsonpath.substring(0,jsonpath.indexOf(".\"$schema\"")) , JsonpathToJq.Mode.STRICT);

                    log(4,"Creating a new source node for "+jsonpath+" -> "+sourcePath);

                    sourceNode = new JqNode(jsonpath,sourcePath,startingNode);
                    if(keepAll){
                        sourceNode.ephemeral = EphemeralMode.KEEP;
                    }

                    folder.group.addNode(sourceNode);
                    nodeTracking.addNode(sourceNode);
                }
                Counters<Extractor> extractorCounts = new Counters<>();
                labelsForJsonpath.stream().flatMap(l->l.extractors().stream()).forEach(extractorCounts::add);
                for(Label label : labelsForJsonpath){
                    log(6,"label="+label.name);
                    Collection<Label> labels = labelsByName.get(label.name);
                    boolean multiSchema = labels.size() > 1;
                    //are any of the current label's extractors used by another label?
                    long maxCount = label.extractors.stream().mapToLong(e->extractorCounts.count(e)).max().orElse(0);
                    multiSchema = maxCount > 1;
                    NodeEntity labelNode = createNodesFromLabel(label,sourceNode,folder.group,nodeTracking,labelNames, multiSchema);
                    if(labelNode!=null){
                        if(keepAll){
                            labelNode.ephemeral = EphemeralMode.KEEP;
                        }
                        nodesByOriginalName.put(label.name,labelNode);
                        nodeTracking.tagNodeAsLabel(label, labelNode);
                        folder.group.addNode(labelNode);
                    }

                }
            }// for each jsonpath
            //create all the merge nodes to resolve label name conflicts
            for(String labelName : nodesByOriginalName.keys()){
                List<NodeEntity> sourceNodes = nodesByOriginalName.get(labelName);
                // Deduplicate: variants that are functionally equivalent (same operation
                // and sources) produce the same value at runtime. Only keep unique variants.
                // Skip variants with no sources (can't produce values).
                List<NodeEntity> uniqueSourceNodes = new ArrayList<>();
                for (NodeEntity sn : sourceNodes) {
                    if (sn.sources.isEmpty()) continue;
                    boolean duplicate = uniqueSourceNodes.stream()
                            .anyMatch(existing -> nodeService.functionalyEquivalent(existing, sn));
                    if (!duplicate) {
                        uniqueSourceNodes.add(sn);
                    } else {
                        System.out.println("REJECTING duplicate SourceNode " + sn);
                    }
                }
                if (uniqueSourceNodes.size() == 1) {
                    // Only one unique variant — set its name to the label name
                    uniqueSourceNodes.get(0).name = labelName;
                    nodeTracking.tagNodeAsLabel(new Label(-1,labelName,null,Collections.emptyList()), uniqueSourceNodes.get(0));
                } else if (uniqueSourceNodes.size() > 1) {
                    // NaN check required: old-schema JS functions return NaN for missing data
                    // (e.g., empty array → reduce → 0/0 → NaN). NaN is not null in JS,
                    // so find(v => v != null) picks NaN over the correct value from the
                    // other schema variant. Key order in Object.values() determines which
                    // variant is checked first, making some labels work and others fail.
                    // Must also skip string "NaN" (from NaN.toFixed(3) in old Confidence functions).
                    // Reverse source order so the newest schema variant is tried first.
                    // Object.values() iterates in insertion order — the newest variant
                    // (last added during import) should be checked before older ones,
                    // since older schema extractors may produce partially-valid values
                    // from data that doesn't match their schema.
                    List<NodeEntity> reversedSources = new ArrayList<>(uniqueSourceNodes);
                    Collections.reverse(reversedSources);
                    NodeEntity newNode = new JsNode(labelName, "obj=>Object.values(obj).find(v => v != null && v !== 'NaN' && (typeof v !== 'number' || !isNaN(v)))", reversedSources);
                    if(combinedLabelPrefix != null && !combinedLabelPrefix.isEmpty()){
                        for(int i=0; i<reversedSources.size(); i++){
                            NodeEntity reversedSource = reversedSources.get(i);
                            if(labelName.equals(reversedSource.name)){
                                reversedSource.name = getLabelRename(labelName);
                            }
                        }
                    }
                    if(keepAll){
                        newNode.ephemeral = EphemeralMode.KEEP;
                    }
                    folder.group.addNode(newNode);
                    nodeTracking.addNode(newNode);
                    nodeTracking.tagNodeAsLabel(new Label(-1, labelName, newNode.operation, Collections.emptyList()), newNode);
                }
            }
        }

        //create nodes from variables
        Map<Long,NodeEntity> variableIdToNode = new HashMap<>();
        for(Variable variable : test.variables()){
            if(variable.calculation() == null || variable.calculation().isEmpty()){
                if(variable.labels().size()==1){
                    String labelName = StringUtil.removeQuotes(variable.labels().get(0)).replaceAll(":","_");
                    List<NodeEntity> found = nodeTracking.getLabelNodes(labelName);
                    if(found.size()>=1){
                        variableIdToNode.put(variable.id(),found.get(0));
                        if(found.size()>1){
                            log(4,"WARNING: variable "+variable.name()+" matched "+found.size()+" label nodes, using first");
                        }
                    }else {
                        System.out.println("FAILED TO MAKE VARIABLE "+variable.id()+" for "+test.name+". Found count for "+labelName+" is 0\n labels="+variable.labels());
                    }
                }else{
                    //THIS IS NOT EXPECTED
                }
            }else{
                //create a new Node
                List<NodeEntity> sources = new ArrayList<>();
                for(int i=0;i<variable.labels().size();i++){
                    String sourceName = StringUtil.removeQuotes(variable.labels().get(i).toString());
                    if(nodeTracking.hasNode(sourceName)){
                        List<NodeEntity> foundNodes = nodeTracking.getLabelNodes(sourceName);
                        if(foundNodes.size()>1){
                            //AMBIGUOUS LABEL
                            log(4,"WARNING: ambiguous label "+sourceName+" for variable "+variable.name()+"="+variable.id());
                        }else{
                            sources.add(foundNodes.get(0));
                        }
                    }else{
                        //missing
                    }
                }
                // Try converting simple JS variable calculations to jq (issue #247)
                String jqCalc = sources.size() == 1 ? JsToJqPatterns.tryConvert(variable.calculation()) : null;
                NodeEntity variableNode = jqCalc != null
                        ? new JqNode(variable.name(), jqCalc, sources)
                        : new JsNode(variable.name(), variable.calculation(), sources);
                if(keepAll){
                    variableNode.ephemeral = EphemeralMode.KEEP;
                }
                folder.group.addNode(variableNode);
                nodeTracking.addNode(variableNode);
                variableIdToNode.put(variable.id(),variableNode);
            }

        }
        //all variables are either aliases (when there isn't a calculation) or a Node
        //create fingerprint nodes (that don't use variables)
        for(Fingerprint fingerprint : test.fingerprints()){
            if(!fingerprint.labels().isEmpty()){
                List<NodeEntity> fingerprintNodes = new ArrayList<>();
                for(int i=0; i< fingerprint.labels().size();i++){
                    String labelName = StringUtil.removeQuotes(fingerprint.labels().get(i));
                    if (nodeTracking.hasNode(labelName)) {
                        List<NodeEntity> foundNodes = nodeTracking.getLabelNodes(labelName);
                        if (foundNodes.size() >= 1) {
                            fingerprintNodes.add(foundNodes.get(0));
                            if (foundNodes.size() > 1) {
                                log(4, "WARNING: fingerprint label " + labelName + " matched " + foundNodes.size() + " nodes, using first");
                            }
                        }
                    }else{
                        System.out.println("missing node "+labelName+" for Fingerprint_label on test "+testId+"="+test.name);
                    }
                }
                if(fingerprintNodes.size()>0){
                    NodeEntity newNode = new FingerprintNode(test.name + "_fingerprint", "", fingerprintNodes);
                    folder.group.addNode(newNode);
                    nodeTracking.addNode(newNode);
                }else{
                    System.out.println("FAILED TO CREATE FINGERPRINT FOR "+test.name+" from "+fingerprint.labels());
                    //todo log error
                }
            }
        }
        //create change detections with fingerprints and nodeIds
        // Look up a domain node from Horreum's timeline_labels configuration.
        // This provides the temporal ordering for change detection algorithms.
        // When timeline_labels is empty, domain stays null and detection algorithms
        // fall back to created_at ordering (issue #284).
        NodeEntity domainNode = null;
        if (!test.fingerprints().isEmpty()) {
            List<String> timelineLabels = test.fingerprints().get(0).timelineLabels();
            if (timelineLabels != null && !timelineLabels.isEmpty()) {
                String timelineLabel = timelineLabels.get(0);
                if (nodeTracking.hasNode(timelineLabel)) {
                    domainNode = nodeTracking.getNodes(timelineLabel).get(0);
                    System.out.println("Using timeline label '" + timelineLabel + "' as domain node for detection");
                } else {
                    System.out.println("Timeline label '" + timelineLabel + "' not found in node graph, detection will use created_at ordering");
                }
            }
        }

        for(ChangeDetection changeDetection : test.changeDetections()){
            String fingerPrintName = test.name + "_fingerprint";
            NodeEntity  variableNode = variableIdToNode.get(changeDetection.variableId());
            NodeEntity fingerprintNode = nodeTracking.hasNode(fingerPrintName) ? nodeTracking.getNodes(fingerPrintName).get(0) : null;
            NodeEntity groupBy = nodeTracking.hasNode("dataset") ? nodeTracking.getNodes("dataset").get(0) : folder.group.root;
            String fingerprint_filter = test.fingerprints().isEmpty() ? null : test.fingerprints().get(0).filter();
            if(fingerprintNode == null){
                fingerprintNode = new FingerprintNode(fingerPrintName,"",Collections.emptyList());
                folder.group.addNode(fingerprintNode);
                nodeTracking.addNode(fingerprintNode);
            }
            if(variableNode == null){
                System.out.println("FAILED TO FIND VARIABLE "+changeDetection.variableId()+" for "+test.name);
                continue;
            }
            NodeEntity changeNode = switch (changeDetection.model()) {
                case "relativeDifference"-> {
                    RelativeDifference difference = new RelativeDifference();
                    difference.name="rd."+variableNode.name+"."+changeDetection.id();
                    try {
                        String filterStr = changeDetection.config().get("filter").asString(RelativeDifference.DEFAULT_FILTER.name());
                        difference.setFilter(RelativeDifferenceConfig.Filter.valueOf(filterStr.toUpperCase()));
                    } catch (IllegalArgumentException ignored) {
                        difference.setFilter(RelativeDifference.DEFAULT_FILTER);
                    }
                    difference.setWindow(changeDetection.config().get("window").asInt(RelativeDifference.DEFAULT_WINDOW));
                    difference.setThreshold(changeDetection.config().get("threshold").asDouble(RelativeDifference.DEFAULT_THRESHOLD));
                    difference.setMinPrevious(changeDetection.config().get("minPrevious").asInt(RelativeDifference.DEFAULT_MIN_PREVIOUS));
                    difference.setNodes(fingerprintNode,groupBy,variableNode,domainNode);
                    if(fingerprint_filter!=null && !fingerprint_filter.isEmpty()){
                        difference.setFingerprintFilter(fingerprint_filter);
                    }
                    yield difference;
                }
                case "fixedThreshold" -> {
                    JqObject max = (JqObject) changeDetection.config().get("max");
                    JqObject min = (JqObject) changeDetection.config().get("min");
                    FixedThreshold fixedThreshold = new FixedThreshold();
                    fixedThreshold.name="ft"+changeDetection.id();
                    fixedThreshold.setNodes(fingerprintNode,groupBy,variableNode);
                    fixedThreshold.setMaxInclusive(max.get("inclusive").asBoolean(true));
                    fixedThreshold.setMinInclusive(min.get("inclusive").asBoolean(true));
                    if(max.has("value") && !max.get("value").isNull() && max.get("enabled").asBoolean(false)) {
                        fixedThreshold.setMax(max.get("value").asDouble(0.0));
                    }
                    if(min.has("value") && !min.get("value").isNull() && min.get("enabled").asBoolean(false)) {
                        fixedThreshold.setMin(min.get("value").asDouble(0.0));
                    }
                    if(fingerprint_filter!=null && !fingerprint_filter.isEmpty()){
                        fixedThreshold.setFingerprintFilter(fingerprint_filter);
                    }
                    yield fixedThreshold;
                }
                case "eDivisive" -> {
                    EDivisive divisive = new EDivisive();
                    divisive.name="ed"+changeDetection.id();
                    yield divisive;
                }
                default -> null;
            };
            if(changeNode!=null){
                folder.group.addNode(changeNode);
                nodeTracking.addNode(changeNode);
            }else{
                //this should not happen and is an error
            }
        }
        return new FolderImportResult(folder, nodeTracking);
    }

    @Override
    public CommandResult execute(H5mCommandInvocation invocation) throws InterruptedException {
        try {
            return doExecute(invocation);
        } catch (Exception e) {
            log("Error: " + e.getMessage());
            return CommandResult.FAILURE;
        }
    }

    private CommandResult doExecute(H5mCommandInvocation invocation) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, "1");
        props.put(AgroalPropertiesReader.MIN_SIZE, "1");
        props.put(AgroalPropertiesReader.INITIAL_SIZE, "1");
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.PRINCIPAL,username); //username
        props.put(AgroalPropertiesReader.CREDENTIAL,password);//password
        props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.postgresql.Driver");
        props.put(AgroalPropertiesReader.JDBC_URL, url );
        AgroalDataSource ds  = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props)
                .get());

        HashedSets<Long,Long> testToTransformer = new HashedSets<>();
        try(Connection connection = ds.getConnection()){
            //create function
            log("creating jsonb_paths");
            try(Statement statement = connection.createStatement()){
                statement.execute(
                """
                    CREATE OR REPLACE FUNCTION jsonb_paths (data jsonb, max_depth integer default 3, prefix text default '$') RETURNS SETOF text LANGUAGE plpgsql AS $$
                    DECLARE
                        key text;
                        value jsonb;
                        counter integer := 0;
                        pathStep text := '';
                    BEGIN
                        IF max_depth <= 0 THEN
                            RETURN NEXT prefix;
                        ELSIF jsonb_typeof(data) = 'object' THEN
                            FOR key, value IN
                                SELECT * FROM jsonb_each(data)
                            LOOP
                                IF key !~ '^[A-Za-z][A-Za-z0-9_]*$' THEN pathStep := CONCAT ('."' , key::text , '"' );
                                ELSE pathStep := CONCAT ( '.' , key::text ); END IF;
                    
                                IF jsonb_typeof(value) IN ('array', 'object') THEN
                                    RETURN QUERY SELECT * FROM jsonb_paths (value,max_depth-1,CONCAT (prefix , pathStep )::text);
                                ELSE
                                    RETURN NEXT CONCAT (prefix , pathStep )::text;
                                END IF;
                            END LOOP;
                        ELSIF jsonb_typeof(data) = 'array' THEN
                            FOR value IN
                                SELECT * FROM jsonb_array_elements(data)
                            LOOP
                                IF jsonb_typeof(value) IN ('array', 'object') THEN
                                    RETURN QUERY SELECT * FROM jsonb_paths (value,max_depth-1,CONCAT (prefix || '[' || counter::text || ']' )::text);
                                ELSE
                                    RETURN NEXT CONCAT (prefix || '[' || counter::text || ']' )::text;
                                END IF;
                                counter := counter + 1;
                            END LOOP;
                        END IF;
                    END
                    $$;
                """);
            }
            //load all test definitions
            try(Statement statement = connection.createStatement()){
                log("Creating run_schema_paths");
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS run_schema_paths as select id,testid,paths,jsonb_path_query_first(data,paths::jsonpath) as schema from run ,lateral (select jsonb_paths(data,3,'$') as paths) where paths like '%$schema%';");
            }
            try(Statement statement = connection.createStatement()){
                log("Creating dataset_schema_paths");
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS dataset_schema_paths as select id,testid,runid,paths,jsonb_path_query_first(data,paths::jsonpath) as schema from dataset, lateral (select jsonb_paths(data,3,'$') as paths) where paths like '%schema%';");
            };
            log("Loading legacy tests");
            Map<Long,String> testNames = new HashMap<>();
            try(Statement statement = connection.createStatement()){
                try(ResultSet rs = statement.executeQuery("select id,name from test")){
                    while(rs.next()){
                        testNames.put(rs.getLong("id"),rs.getString("name"));
                    }
                }
            }
            //load all test transformations
            log("Loading legacy test transformers");
            try(Statement statement = connection.createStatement()){
                try(ResultSet rs = statement.executeQuery("select test_id,transformer_id from test_transformers")){
                    while(rs.next()){
                        testToTransformer.put(rs.getLong(1),rs.getLong(2));
                    }
                }
            }
            List<Long> testids = new ArrayList<>(testNames.keySet());
            testids.sort(Comparator.naturalOrder());
            if(testId != null ){
                testids = List.of(testId);
            }
            for(Long testId : testids){
                Test test = new Test(testId,testNames.get(testId),new HashedSets<>(),new ArrayList<>(),new ArrayList<>(),new ArrayList<>(),new ArrayList<>());
                log(String.format("%3d - %s",test.id,test.name));

                if(testToTransformer.has(test.id)){
                    log(2,"has datasets");
                    Set<Long> transformids = testToTransformer.get(test.id);
                    Set<Transformer> transformers = new HashSet<>();
                    for(Long transformId : transformids){
                        Transformer t = loadTransformer(connection,transformId);
                        transformers.add(t);
                        test.transformers.add(t);
                    }
                    assert transformers.size()==transformids.size();

                    if(transformers.size() > 1){
                        log(2, "multiple transformers (" + transformers.size() + ") for same target, creating pipeline for each");
                    }
                } else {
                    //no transform
                    HashedSets<String,String> schemaByPath = new HashedSets<>();
                    //get the jsonpath -> schema used across all runs in this test
                    try (PreparedStatement statement = connection.prepareStatement("select p.paths, p.schema from run_schema_paths p where testid = ?")) {
                        statement.setLong(1,test.id);
                        try(ResultSet rs = statement.executeQuery()){
                            while(rs.next()){
                                String path = rs.getString(1);
                                String schema = StringUtil.removeQuotes(rs.getString(2));
                                schemaByPath.put(path,schema);
                            }
                        }
                    }
                    HashedLists<String,Label> schemaLabels = new HashedLists<>();
                    HashedLists<String,Label> schemaLabelsByName = new HashedLists<>();
                    //load all the labels
                    for(String path : schemaByPath.keys()){
                        Set<String> schemaUris = schemaByPath.get(path);
                        for(String schemaUri : schemaUris){
                            if(!schemaLabels.containsKey(schemaUri)){
                                List<Label> loadedLabels = loadUriLabels(connection,schemaUri);
                                loadedLabels.forEach(l->{
                                    schemaLabels.put(schemaUri,l);
                                    schemaLabelsByName.put(l.name,l);
                                });

                            }else{
                                schemaLabels.get(schemaUri).forEach(l->schemaLabelsByName.put(l.name,l));
                            }
                            schemaLabels.get(schemaUri).forEach(l->test.schemaPaths.put(path,l));
                        }
                    }
                    //stores nodes renamed because multiple labels shared that name
                    HashedLists<String,NodeEntity> nodesByOriginalName = new HashedLists<>();
                    for(String jsonpath : schemaByPath.keys()){
                        Set<String> schemas = schemaByPath.get(jsonpath);
                        List<Label> allLabelsForJsonpath = schemas.stream().filter(schemaLabels::containsKey).flatMap(s->schemaLabels.get(s).stream()).toList();
                        HashedSets<String,Label> labelsByName = new HashedSets<>();
                        allLabelsForJsonpath.stream().forEach(e->{
                            labelsByName.put(e.name,e);
                        });

                        log(2,jsonpath+" -> "+schemas+" "+allLabelsForJsonpath.size()+" label(s)");
                        if(allLabelsForJsonpath.isEmpty()){
                            continue;
                        }
                        //NodeEntity sourceNode = folder.group.root;
                        if(!jsonpath.equals("$.\"$schema\"")){
                            String sourcePath = jsonpath.substring(0,jsonpath.indexOf(".\"$schema\""));
                            log(4,"Creating a new source node for "+jsonpath+" -> "+sourcePath);
                        }
                        log(4,"creating labels");
                        for(String labelName : labelsByName.keys()){
                            log(6,"label="+labelName);
                            Set<Label> labels = labelsByName.get(labelName);
                        }
                    } // for each jsonpath
                }

                //load variables
                try(PreparedStatement statement = connection.prepareStatement("select id,name,labels,calculation from variable where testid=?")) {
                    statement.setLong(1, testId);
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            Long id = rs.getLong("id");
                            String name = rs.getString("name");
                            JqArray labels = (JqArray) JqValues.parse(rs.getString("labels"));
                            String calculation = rs.getString("calculation");
                            Variable variable = new Variable(id,name,new ArrayList<>(),calculation);
                            for (int li = 0; li < labels.length(); li++) variable.labels.add(labels.get(li).asString(""));
                            test.variables.add(variable);
                        }
                    }
                }
                //load fingerprints
                String fingerprint_filter = "";
                try(PreparedStatement statement = connection.prepareStatement("select fingerprint_labels, fingerprint_filter, timeline_labels, timeline_function from test where id = ?")){
                    statement.setLong(1,testId);
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            JqArray fingerprint_labels = (JqArray) JqValues.parse(rs.getString(1));
                            fingerprint_filter = rs.getString(2);
                            String timeline_labels = rs.getString(3);
                            String timeline_function = rs.getString(4);
                            List<String> timelineLabelList = new ArrayList<>();
                            if (timeline_labels != null && !timeline_labels.isBlank()) {
                                JqValue parsed = JqValues.parse(timeline_labels);
                                if (parsed.isArray()) {
                                    for (int ti = 0; ti < parsed.length(); ti++) {
                                        timelineLabelList.add(parsed.getElement(ti).asString(""));
                                    }
                                }
                            }
                            Fingerprint fingerprint = new Fingerprint(new ArrayList<>(),fingerprint_filter,timelineLabelList,timeline_function);
                            for (int li = 0; li < fingerprint_labels.length(); li++) fingerprint.labels.add(fingerprint_labels.get(li).asString(""));
                            test.fingerprints.add(fingerprint);
                        }
                    }
                }
                //load change detections
                try(PreparedStatement statement = connection.prepareStatement("select id,variable_id,model,config from changedetection where variable_id in (select id from variable where testid = ?)")){
                    statement.setLong(1,testId);
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            Long id = rs.getLong("id");
                            Long variableId = rs.getLong("variable_id");
                            //NodeEntity  variableNode = variableIdToNode.get(variableId);
                            String fingerPrintName = test.name + "_fingerprint";
                            //NodeEntity fingerprintNode = nodeTracking.hasNode(fingerPrintName) ? nodeTracking.getNodes(fingerPrintName).get(0) : null;
                            //NodeEntity groupBy = nodeTracking.hasNode("dataset") ? nodeTracking.getNodes("dataset").get(0) : folder.group.root;
                            String model = rs.getString("model");
                            JqObject config = (JqObject) JqValues.parse(rs.getString("config"));
                            ChangeDetection changeDetection = new ChangeDetection(id,variableId,model,config);
                            test.changeDetections().add(changeDetection);
                        }
                    }
                }
                //TODO create a folderService method that persists an entity
                //FolderEntity.persist(folder);
                FolderImportResult result = createFolder(test);
                long folderId = folderService.create(result.folder());
                try {
                    importViews(connection, test, folderId, result.folder().name, result.nodeTracking());
                } catch (Exception e) {
                    // View import is best-effort — the folder and nodes are already
                    // persisted. Catch any unexpected errors to prevent view import
                    // failures from rolling back the folder and node creation.
                    log("WARNING: view import failed for " + test.name + ": " + e.getMessage());
                }
            }
        }
        finally {
            ds.close();
        }
        return CommandResult.SUCCESS;
    }
    public static List<Label> loadUriLabels(Connection connection,String uri) throws SQLException {
        List<Label> labels = new ArrayList<>();
        List<LabelDef> labelDefs = new ArrayList<>();
        //Set<String> labelNames = labelDefs.stream().map(LabelDef::name).collect(Collectors.toSet());
        //List<Label> targetSchemaLabels = new ArrayList<>();
        try(PreparedStatement statement = connection.prepareStatement("select id,name,function from label where schema_id = (select id from schema where uri = ?)")){
            statement.setString(1,uri);
            try(ResultSet rs = statement.executeQuery()){
                while(rs.next()){
                    labelDefs.add(new LabelDef(rs.getLong(1),rs.getString(2),rs.getString(3)));
                }
            }
        }
        for(LabelDef labelDef : labelDefs){
            try(PreparedStatement statement = connection.prepareStatement("select name,jsonpath,isarray from label_extractors where label_id = ?")){
                statement.setLong(1,labelDef.id);
                List<Extractor> labelExtractors = new ArrayList<>();
                try(ResultSet rs = statement.executeQuery()){
                    while(rs.next()){
                        labelExtractors.add(new Extractor(rs.getString(1),rs.getString(2),rs.getBoolean(3)));
                    }
                }
                Label label = new Label(labelDef.id,labelDef.name.replaceAll(":","_"),labelDef.function,labelExtractors);
                labels.add(label);
            }
        }
        return labels;
    }
    public static Transformer loadTransformer(Connection connection,long transformerId) throws SQLException {
        Transformer rtrn = null;
        List<Extractor> transformExtractors = new ArrayList<>();
        try(PreparedStatement statement = connection.prepareStatement("select name,jsonpath,isarray from transformer_extractors where transformer_id=?;")){
            statement.setLong(1,transformerId);
            try(ResultSet rs = statement.executeQuery()){
                while(rs.next()){
                    transformExtractors.add(new Extractor(rs.getString(1),rs.getString(2),rs.getBoolean(3)));
                }
            }
        }
        try(PreparedStatement statement=connection.prepareStatement("select name,function,targetschemauri from transformer where id = ?")){
            statement.setLong(1,transformerId);
            try(ResultSet rs = statement.executeQuery()){
                //should really only happen once
                while(rs.next()){
                    if(rtrn != null){
                        //too many transformers found
                    }
                    rtrn = new Transformer(transformerId,rs.getString(1).replaceAll(":","_"),rs.getString(2),rs.getString(3),transformExtractors, new ArrayList<>());
                }
            }
        }
        List<Label> uriLabels = loadUriLabels(connection,rtrn.targetUri);
        rtrn.targetSchemaLabels.addAll(uriLabels);
        return rtrn;
    }
    /**
     * Import Horreum views for a test into h5m.
     * Queries the view and viewcomponent tables, resolves label names to h5m node IDs
     * using the NodeTracking from folder creation (avoids an extra DB round-trip).
     */
    private void importViews(Connection connection, Test test, long folderId, String folderName,
                              NodeTracking nodeTracking) throws SQLException {
        try (PreparedStatement viewStmt = connection.prepareStatement(
                "SELECT id, name FROM view WHERE test_id = ? ORDER BY name")) {
            viewStmt.setLong(1, test.id());
            try (ResultSet viewRs = viewStmt.executeQuery()) {
                while (viewRs.next()) {
                    long viewId = viewRs.getLong("id");
                    String viewName = viewRs.getString("name");

                    List<ViewComponent> components = new ArrayList<>();
                    try (PreparedStatement compStmt = connection.prepareStatement(
                            "SELECT headername, headerorder, labels FROM viewcomponent WHERE view_id = ? ORDER BY headerorder")) {
                        compStmt.setLong(1, viewId);
                        try (ResultSet compRs = compStmt.executeQuery()) {
                            while (compRs.next()) {
                                String headerName = compRs.getString("headername");
                                int headerOrder = compRs.getInt("headerorder");
                                String labelsJson = compRs.getString("labels");

                                JqValue labelsArray = JqValues.parse(labelsJson);
                                if (labelsArray == null || !labelsArray.isArray() || labelsArray.length() == 0) {
                                    continue;
                                }

                                if (labelsArray.length() > 1) {
                                    System.out.println("  Warning: view '" + viewName + "' component '" + headerName
                                            + "' references " + labelsArray.length() + " labels, using only the first");
                                }

                                String labelName = labelsArray.getElement(0).asText();

                                // Resolve label name to h5m node using NodeTracking.
                                // Try getLabelNodes first (nodes explicitly tagged as labels),
                                // then fall back to getNodes (all nodes by name) as a safety
                                // net in case a node wasn't tagged during import.
                                List<NodeEntity> labelNodes = nodeTracking.getLabelNodes(labelName);
                                if (labelNodes.isEmpty()) {
                                    labelNodes = nodeTracking.getNodes(labelName);
                                }
                                NodeEntity node = labelNodes.isEmpty() ? null : labelNodes.getFirst();

                                if (node != null && node.id != null) {
                                    components.add(new ViewComponent(
                                            null, node.id, node.name,
                                            node.type().display(), headerName, headerOrder));
                                } else {
                                    log("  Warning: label '" + labelName + "' not found for view '" + viewName + "'");
                                }
                            }
                        }
                    }

                    if (components.isEmpty()) {
                        System.out.println("  Skipping view '" + viewName + "' (no components resolved)");
                        continue;
                    }

                    if ("Default".equals(viewName)) {
                        // Legacy Horreum's "Default" view maps to h5m's system default view, already created by folderService.create — update it in place.
                        List<View> existing = viewService.getViews(folderId);
                        View defaultView = existing.stream()
                                .filter(v -> ReservedNamespace.DEFAULT_VIEW_NAME.equals(v.name()))
                                .findFirst().orElse(null);
                        if (defaultView != null) {
                            viewService.updateView(defaultView.id(), new View(defaultView.id(), ReservedNamespace.DEFAULT_VIEW_NAME, folderId, components));
                        } else {
                            viewService.createView(folderId, new View(null, ReservedNamespace.DEFAULT_VIEW_NAME, null, components));
                        }
                    } else {
                        viewService.createView(folderId, new View(null, viewName, null, components));
                    }
                    System.out.println("  Imported view '" + viewName + "' with " + components.size() + " columns");
                }
            }
        }
    }
}
