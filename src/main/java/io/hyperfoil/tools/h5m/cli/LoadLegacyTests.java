package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.errorprone.annotations.Var;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.api.NodeGroup;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.yaup.HashedLists;
import io.hyperfoil.tools.yaup.HashedSets;
import io.hyperfoil.tools.yaup.StringUtil;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name="load-legacy-tests")
public class LoadLegacyTests implements Callable<Integer> {

    @CommandLine.Option(names = {"username"}, description = "legacy db username", defaultValue = "quarkus") String username;
    @CommandLine.Option(names = {"password"}, description = "legacy db password", defaultValue = "quarkus") String password;
    @CommandLine.Option(names = {"url"}, description = "legacy connection url",defaultValue = "jdbc:postgresql://0.0.0.0:5432/horreum") String url;
    @CommandLine.Option(names = {"testId"}, description = "specify which test to load. Loads all if unspecified" ) Long testId;

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

        Map<Extractor,NodeEntity> extractorNodes = new HashMap<>();
        Map<Label,NodeEntity> labelToNodes = new HashMap<>();
        Map<NodeEntity,Label> nodeToLabel = new HashMap<>();
        HashedLists<String,NodeEntity> nodesByName = new HashedLists<>();

        public void tagNodeAsExtractor(Extractor extractor, NodeEntity node) {
            extractorNodes.put(extractor,node);
        }
        public void tagNodeAsLabel(Label label,NodeEntity node){
            labelToNodes.put(label,node);
            nodeToLabel.put(node,label);
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
        public boolean hasNode(Extractor extractor){
            return extractorNodes.containsKey(extractor);
        }
        public boolean hasNode(Label label){
            return labelToNodes.containsKey(label);
        }
        public boolean hasNode(String name){
            return nodesByName.containsKey(name);
        }
        public NodeEntity getNode(Extractor extractor){
            return extractorNodes.get(extractor);
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

    static String jsonpathToJq(String jsonpath) {
        if (jsonpath == null || jsonpath.isEmpty()) return ".";
        String jq = jsonpath;
        if (jq.startsWith("$.")) jq = jq.substring(1);
        else if (jq.equals("$")) return ".";
        jq = jq.replace(".*", "[]?");
        jq = jq.replace("[*]", "[]?");
        return jq;
    }

    @Inject
    FolderService folderService;

    @Inject
    NodeService nodeService;

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
    public record ChangeDetection(long id,long variableId,String model,ObjectNode config){};
    public record Test(long id, String name, HashedLists<String,Label> schemaPaths, List<Fingerprint> fingerprints, List<ChangeDetection> changeDetections, List<Transformer> transformers, List<Variable> variables){};
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
            return false;
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

    public NodeEntity createNodesFromLabel(Label label, NodeEntity source, NodeGroupEntity group, NodeTracking nodeTracking, Set<String> usedNames){
        NodeEntity rtrn = null;
        HashedLists<String,NodeEntity> labelNodesByName = new HashedLists<>();
        boolean reusedNode = false;
        for(Extractor extractor : label.extractors) {
            String extractorName = extractor.name;
            if(extractorName.equals(label.name) || usedNames.contains(extractorName)){
                extractorName = extractor.name;
            }

            NodeEntity node = extractor.isArray ?
                    SqlJsonpathAllNode.parse(extractorName, extractor.jsonpath(), nodeTracking::getNodes) :
                    SqlJsonpathNode.parse(extractorName, extractor.jsonpath(), nodeTracking::getNodes);
            if (node == null) {
                System.err.println("failed to create node for extractor " + extractor);
                return null;
            }
            node.sources = List.of(source);
            group.addNode(node);
            if(nodeTracking.hasNode(extractor) && nodeService.functionalyEquivalent(node,nodeTracking.getNode(extractor))){
                node = nodeTracking.getNode(extractor);
                reusedNode = true;
            }else{
                nodeTracking.addNode(node);
                nodeTracking.tagNodeAsExtractor(extractor, node);
            }
            labelNodesByName.put(node.name, node);
        }
        //this could rename a node that is referenced by name by another node! Don't do that!
        if(label.function==null || label.function.trim().isEmpty() || JsNode.isNullEmptyOrIdentityFunction(label.function)){
            if(label.extractors.size() == 1 && !reusedNode && label.name!=null && !label.name.trim().isEmpty()){
                String extractorName = label.extractors.get(0).name;
                List<NodeEntity> extractorNodes = labelNodesByName.get(extractorName);
                if(extractorNodes.size()==1){
                    NodeEntity extractorNode = extractorNodes.get(0);
                    if(!label.name.equals(extractorNode.name)){
                        extractorNode.name = label.name;
                        nodeTracking.renameNode(extractorNode,label.extractors.get(0).name);
                    }
                    rtrn = extractorNode;
                }else{
                    System.out.println("FAILED TO FIND SINGLE EXTRACTOR "+label.extractors.get(0).name+" for label "+label.name+
                            "\nextractors:\n  "+label.extractors.stream().map(Objects::toString).collect(Collectors.joining("\n  "))+
                            "\nfound:\n  "+extractorNodes.stream().map(NodeEntity::toString).collect(Collectors.joining("\n  "))+
                            "\nlabelNodes:\n  "+labelNodesByName.keys().stream().map(s->"  "+s+":\n    "+labelNodesByName.get(s).stream().map(NodeEntity::toString).collect(Collectors.joining("\n      "))).collect(Collectors.joining("\n  ")));
                }
            }else if (label.function==null || label.function.trim().isEmpty()){
                rtrn = new JsNode(label.name(),"solo=>solo",labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                nodeTracking.addNode(rtrn);
                group.addNode(rtrn);
            }
        }else {
            String function = label.function;
            rtrn = JsNode.parse(label.name, function, labelNodesByName::get);
            if (rtrn == null) {
                List<String> params = JsNode.getParameterNames(label.function);
                if(params.size()==1 && label.extractors.size() > 1) {
                    // Multi-extractor single-param: build a JQ node that combines all extractions
                    // into a single object per dataset item, mirroring Horreum's per-dataset extraction
                    StringBuilder jqExpr = new StringBuilder("{");
                    for (int i = 0; i < label.extractors.size(); i++) {
                        Extractor ext = label.extractors.get(i);
                        if (i > 0) jqExpr.append(", ");
                        String jqPath = jsonpathToJq(ext.jsonpath());
                        if (ext.isArray()) {
                            jqExpr.append(ext.name()).append(": (try [").append(jqPath).append("] catch null)");
                        } else {
                            jqExpr.append(ext.name()).append(": (").append(jqPath).append(" // null)");
                        }
                    }
                    jqExpr.append("}");
                    NodeEntity combiner = new JqNode(label.name() + "_extract", jqExpr.toString(), List.of(source));
                    group.addNode(combiner);
                    nodeTracking.addNode(combiner);
                    rtrn = new JsNode(label.name, function, List.of(combiner));
                } else if(params.size()==1) {
                    rtrn = new JsNode(label.name,function,labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                } else {
                    rtrn = JsNode.parse(label.name, function, labelNodesByName::get,true);
                    if(rtrn==null){
                        System.err.println("Failed to make node for Label:" + label.name + "\n" + label.function + "\n  " + label.extractors.stream().map(Extractor::toString).collect(Collectors.joining("\n  ")));
                        return null;
                    }
                }
            }
            if(rtrn!=null){
                nodeTracking.addNode(rtrn);
            }
        }
        return rtrn;
    }


    public FolderEntity createFolder(Test test){
        FolderEntity folder = new FolderEntity();
        folder.name = test.name;
        folder.group = new NodeGroupEntity(test.name);

        NodeTracking nodeTracking = new NodeTracking();

        if(!test.transformers().isEmpty()){
            // Phase 1: Create transformer nodes for each transformer
            List<NodeEntity> transformerNodes = new ArrayList<>();
            List<Label> allSchemaLabels = null;
            for(Transformer transformer : test.transformers){
                List<Extractor> renamedExtractors = new ArrayList<>();
                Map<String,String> extractorAliases = new HashMap<>();
                transformer.extractors.forEach(e->{
                    Extractor newE = new Extractor("_"+e.name,e.jsonpath,e.isArray);
                    renamedExtractors.add(newE);
                    extractorAliases.put(e.name,newE.name);
                });
                String function = NodeService.renameParameters(transformer.function,extractorAliases);
                //not using function and renamedExtractors
                String transformerSuffix = test.transformers().size() > 1 ? "_" + transformer.id() : "";
                Label l  = new Label(-1,"transformer_"+transformer.name.replaceAll(":","_") + transformerSuffix,transformer.function,transformer.extractors);
                NodeEntity transform = createNodesFromLabel(l,folder.group.root,folder.group,nodeTracking,new HashSet<>());
                folder.group.addNode(transform);
                nodeTracking.addNode(transform);
                transformerNodes.add(transform);

                // Keep the first transformer's labels (all transformers target the same schema)
                if (allSchemaLabels == null) {
                    allSchemaLabels = transformer.targetSchemaLabels();
                }
            }

            // Phase 2: Coalesce transformers, then split into dataset, then create labels
            // Both transformers target the same schema — for a given run only one produces output.
            // Coalesce at transformer level (single value each) so the permutation logic handles
            // empty sources correctly (maxNodeValuesLength == 1 triggers the simple case).
            NodeEntity transformerSource;
            if (transformerNodes.size() == 1) {
                transformerSource = transformerNodes.get(0);
            } else {
                StringJoiner coalesceParams = new StringJoiner(",");
                for (NodeEntity tn : transformerNodes) coalesceParams.add(tn.name);
                StringBuilder coalesceBody = new StringBuilder();
                for (int i = 0; i < transformerNodes.size() - 1; i++) {
                    coalesceBody.append(transformerNodes.get(i).name).append(" != null ? ").append(transformerNodes.get(i).name).append(" : ");
                }
                coalesceBody.append(transformerNodes.get(transformerNodes.size() - 1).name);
                String coalesceFunc = "(" + coalesceParams + ") => " + coalesceBody;
                transformerSource = new JsNode("transformer_coalesce", coalesceFunc, transformerNodes);
                folder.group.addNode(transformerSource);
                nodeTracking.addNode(transformerSource);
            }

            NodeEntity dataset = new JqNode("dataset","if type == \"array\" then .[] else . end",List.of(transformerSource));
            folder.group.addNode(dataset);
            nodeTracking.addNode(dataset);

            if (allSchemaLabels != null) {
                Set<String> labelNames = new HashSet<>();
                for (Label schemaLabel : allSchemaLabels) {
                    log(6, "label=" + schemaLabel.name);
                    NodeEntity labelNode = createNodesFromLabel(schemaLabel, dataset, folder.group, nodeTracking, labelNames);
                    if (labelNode != null) {
                        nodeTracking.tagNodeAsLabel(schemaLabel, labelNode);
                        folder.group.addNode(labelNode);
                    } else {
                        System.out.println("FAILURE NULL NODE FOR LABEL " + schemaLabel);
                    }
                }
            }
        }else if(!test.schemaPaths().isEmpty()){
            List<String> schemaPaths = new ArrayList<>(test.schemaPaths().keys());
            schemaPaths.sort(String::compareTo);
            //stores nodes renamed because multiple labels shared that name
            HashedLists<String,NodeEntity> nodesByOriginalName = new HashedLists<>();

            HashedSets<String,Label> labelsByName = new HashedSets<>();
            test.schemaPaths.forEach((p,lbls)->{
                lbls.forEach(lbl -> labelsByName.put(lbl.name,lbl));
            });
            for(String jsonpath : schemaPaths){
                List<Label> labelsForJsonpath = test.schemaPaths().get(jsonpath);
                log(2,jsonpath+" -> "+" "+labelsForJsonpath.size()+" label(s)");
                if(test.schemaPaths().get(jsonpath).isEmpty()){
                    continue;
                }
                NodeEntity sourceNode = folder.group.root;
                if(!jsonpath.equals("$.\"$schema\"")){
                    String sourcePath = jsonpath.substring(0,jsonpath.indexOf(".\"$schema\""));
                    log(4,"Creating a new source node for "+jsonpath+" -> "+sourcePath);

                    sourceNode = new JqNode(sourcePath,sourcePath,folder.group.root);
                    folder.group.addNode(sourceNode);
                    nodeTracking.addNode(sourceNode);
                }
                for(Label label : labelsForJsonpath){
                    log(6,"label="+label.name);
                    Set<Label> labels = labelsByName.get(label.name);

                    NodeEntity labelNode = createNodesFromLabel(label,sourceNode,folder.group,nodeTracking,labelsByName.keys());
                    if(labelNode!=null){
                        //if this label needs to be renamed and part of a merge group
                        if(labels.size()>1) {
                            labelNode.name = labelNode.name + nodesByOriginalName.get(label.name).size();
                            nodesByOriginalName.put(label.name,labelNode);
                        } else {
                            nodeTracking.tagNodeAsLabel(label, labelNode);
                        }
                    }

                }
            }// for each jsonpath
            //create all the merge nodes to resolve label name conflicts
            for(String labelName : nodesByOriginalName.keys()){
                System.out.println("combining "+labelName);
                List<NodeEntity> sourceNodes = nodesByOriginalName.get(labelName);
                NodeEntity newNode = new JsNode(labelName,"obj=>Object.values(obj).find(v => v != null)",sourceNodes);
                folder.group.addNode(newNode);
                nodeTracking.addNode(newNode);
                nodeTracking.tagNodeAsLabel(new Label(-1,labelName,newNode.operation,Collections.emptyList()),newNode);
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
                            log(4,"variable "+variable.name()+" matched "+found.size()+" label nodes, using first");
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
                        }else{
                            sources.add(foundNodes.get(0));
                        }
                    }else{
                        //missing
                    }
                }
                NodeEntity variableNode = new JsNode(variable.name(),variable.calculation(),sources);
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
                                log(4, "fingerprint label " + labelName + " matched " + foundNodes.size() + " nodes, using first");
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
                    difference.setFilter(changeDetection.config().get("filter").asText());
                    difference.setWindow(changeDetection.config().get("window").asInt());
                    difference.setThreshold(changeDetection.config().get("threshold").asDouble());
                    difference.setMinPrevious(changeDetection.config().get("minPrevious").asInt());
                    difference.setNodes(fingerprintNode,groupBy,variableNode,null);
                    if(fingerprint_filter!=null && !fingerprint_filter.isEmpty()){
                        difference.setFingerprintFilter(fingerprint_filter);
                    }
                    yield difference;
                }
                case "fixedThreshold" -> {
                    ObjectNode max = (ObjectNode) changeDetection.config().get("max");
                    ObjectNode min = (ObjectNode) changeDetection.config().get("min");
                    FixedThreshold fixedThreshold = new FixedThreshold();
                    fixedThreshold.name="ft"+changeDetection.id();
                    fixedThreshold.setNodes(fingerprintNode,groupBy,variableNode);
                    fixedThreshold.setMaxInclusive(max.get("inclusive").asBoolean());
                    fixedThreshold.setMinInclusive(min.get("inclusive").asBoolean());
                    if(max.get("value") != null && max.get("enabled").asBoolean(false)) {
                        fixedThreshold.setMax(max.get("value").asDouble());
                    }
                    if(min.get("value") != null && min.get("enabled").asBoolean(false)) {
                        fixedThreshold.setMin(min.get("value").asDouble());
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
        return folder;
    }

    @Override
    public Integer call() throws Exception {
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
                Test test = new Test(testId,testNames.get(testId),new HashedLists<>(),new ArrayList<>(),new ArrayList<>(),new ArrayList<>(),new ArrayList<>());
                log(String.format("%3d - %s",test.id,test.name));

                if(testToTransformer.has(test.id)){
                    log(2,"has datasets");
                    Set<Long> transformids = testToTransformer.get(test.id);
                    Set<Transformer> transformers = new HashSet<>();
                    for(Long transformId : transformids){
                        List<Extractor> extractors = new ArrayList<>();
                        try(PreparedStatement statement = connection.prepareStatement("select name,jsonpath,isarray from transformer_extractors where transformer_id=?;")){
                            statement.setLong(1,transformId);
                            try(ResultSet rs = statement.executeQuery()){
                                while(rs.next()){
                                    extractors.add(new Extractor(rs.getString(1),rs.getString(2),rs.getBoolean(3)));
                                }
                            }
                        }
                        try(PreparedStatement statement=connection.prepareStatement("select name,function,targetschemauri from transformer where id = ?")){
                            statement.setLong(1,transformId);
                            try(ResultSet rs = statement.executeQuery()){
                                //should really only happen once
                                while(rs.next()){
                                    Transformer t = new Transformer(transformId,rs.getString(1).replaceAll(":","_"),rs.getString(2),rs.getString(3),extractors, new ArrayList<>());
                                    transformers.add(t);
                                    test.transformers.add(t);
                                }
                            }
                        }
                    }
                    assert transformers.size()==transformids.size();

                    if(transformers.size() > 1){
                        log(2, "multiple transformers (" + transformers.size() + ") for same target, creating pipeline for each");
                    }
                    {
                        for (Transformer transformer : transformers) {
                            List<LabelDef> labelDefs = new ArrayList<>();
                            //Set<String> labelNames = labelDefs.stream().map(LabelDef::name).collect(Collectors.toSet());
                            //List<Label> targetSchemaLabels = new ArrayList<>();
                            try(PreparedStatement statement = connection.prepareStatement("select id,name,function from label where schema_id = (select id from schema where uri = ?)")){
                                statement.setString(1,transformer.targetUri);
                                try(ResultSet rs = statement.executeQuery()){
                                    while(rs.next()){
                                        labelDefs.add(new LabelDef(rs.getLong(1),rs.getString(2),rs.getString(3)));
                                    }
                                }
                            }
                            for(LabelDef labelDef : labelDefs){
                                try(PreparedStatement statement = connection.prepareStatement("select name,jsonpath,isarray from label_extractors where label_id = ?")){
                                    statement.setLong(1,labelDef.id);
                                    List<Extractor> extractors = new ArrayList<>();
                                    try(ResultSet rs = statement.executeQuery()){
                                        while(rs.next()){
                                            extractors.add(new Extractor(rs.getString(1),rs.getString(2),rs.getBoolean(3)));
                                        }
                                    }
                                    Label label = new Label(labelDef.id,labelDef.name.replaceAll(":","_"),labelDef.function,extractors);
                                    transformer.targetSchemaLabels.add(label);
                                    //targetSchemaLabels.add(label);
                                }
                            }
                            labelDefs.clear();//so we don't accidentally use it
                        }
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
                                List<LabelDef> labelDefs = new ArrayList<>();
                                try(PreparedStatement statement = connection.prepareStatement("select id,name,function from label where schema_id = (select id from schema where uri = ?)")){
                                    statement.setString(1,schemaUri);
                                    try(ResultSet rs = statement.executeQuery()){
                                        while(rs.next()){
                                            labelDefs.add(new LabelDef(rs.getLong(1),rs.getString(2),rs.getString(3)));
                                        }
                                    }
                                }
                                for(LabelDef labelDef : labelDefs){
                                    List<Extractor> extractors = new ArrayList<>();
                                    try(PreparedStatement statement = connection.prepareStatement("select name,jsonpath,isarray from label_extractors where label_id = ?")){
                                        statement.setLong(1,labelDef.id);
                                        try(ResultSet rs = statement.executeQuery()){
                                            while(rs.next()){
                                                extractors.add(new Extractor(rs.getString(1),rs.getString(2),rs.getBoolean(3)));
                                            }
                                        }
                                    }
                                    Label newLabel = new Label(labelDef.id,labelDef.name.replaceAll(":","_"),labelDef.function,extractors);
                                    test.schemaPaths.put(path,newLabel);

                                    schemaLabels.put(schemaUri,newLabel);
                                    schemaLabelsByName.put(newLabel.name,newLabel);
                                }
                            }else{
                                schemaLabels.get(schemaUri).forEach(l->schemaLabelsByName.put(l.name,l));
                            }
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
                    ObjectMapper mapper = new ObjectMapper();
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            Long id = rs.getLong("id");
                            String name = rs.getString("name");
                            ArrayNode labels = (ArrayNode) mapper.readTree(rs.getString("labels"));
                            String calculation = rs.getString("calculation");
                            Variable variable = new Variable(id,name,new ArrayList<>(),calculation);
                            labels.forEach(v->variable.labels.add(v.asText()));
                            test.variables.add(variable);
                        }
                    }
                }
                //load fingerprints
                String fingerprint_filter = "";
                try(PreparedStatement statement = connection.prepareStatement("select fingerprint_labels, fingerprint_filter, timeline_labels, timeline_function from test where id = ?")){
                    ObjectMapper mapper = new ObjectMapper();
                    statement.setLong(1,testId);
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            ArrayNode fingerprint_labels = (ArrayNode) mapper.readTree(rs.getString(1));
                            fingerprint_filter = rs.getString(2);
                            String timeline_labels = rs.getString(3); // not sure how this is used atm
                            String timeline_function = rs.getString(4); // not sure how this is used atm
                            Fingerprint fingerprint = new Fingerprint(new ArrayList<>(),fingerprint_filter,new ArrayList<>(),timeline_function);
                            fingerprint_labels.forEach(l->fingerprint.labels.add(l.toString()));
                            test.fingerprints.add(fingerprint);
                        }
                    }
                }
                //load change detections
                try(PreparedStatement statement = connection.prepareStatement("select id,variable_id,model,config from changedetection where variable_id in (select id from variable where testid = ?)")){
                    statement.setLong(1,testId);
                    ObjectMapper mapper = new ObjectMapper();
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            Long id = rs.getLong("id");
                            Long variableId = rs.getLong("variable_id");
                            //NodeEntity  variableNode = variableIdToNode.get(variableId);
                            String fingerPrintName = test.name + "_fingerprint";
                            //NodeEntity fingerprintNode = nodeTracking.hasNode(fingerPrintName) ? nodeTracking.getNodes(fingerPrintName).get(0) : null;
                            //NodeEntity groupBy = nodeTracking.hasNode("dataset") ? nodeTracking.getNodes("dataset").get(0) : folder.group.root;
                            String model = rs.getString("model");
                            ObjectNode config = (ObjectNode) mapper.readTree(rs.getString("config"));
                            ChangeDetection changeDetection = new ChangeDetection(id,variableId,model,config);
                            test.changeDetections().add(changeDetection);
                        }
                    }
                }
                //TODO create a folderService method that persists an entity
                //FolderEntity.persist(folder);
                FolderEntity folder = createFolder(test);
                folderService.create(folder);
            }
        }
        finally {
            ds.close();
        }
        return 0;
    }
}
