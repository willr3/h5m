package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    public class NodeTracking {

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

    public record Test(long id,String name){};
    public record Extractor(String name,String jsonpath,boolean isArray){};
    public record Transformer(long id,String name,String function,String targetUri, List<Extractor> extractors){
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
        if(label.function==null || label.function.trim().isEmpty()){
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
            }else{
                rtrn = new JsNode(label.name(),"solo=>solo",labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                nodeTracking.addNode(rtrn);
                group.addNode(rtrn);
            }
        }else {
            String function = label.function;
            rtrn = JsNode.parse(label.name, function, labelNodesByName::get);
            if (rtrn == null) {
                List<String> params = JsNode.getParameterNames(label.function);
                if(params.size()==1) {//collect all extractors into the value
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
        Map<Long,Test> allTests = new HashMap<>();
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
            try(Statement statement = connection.createStatement()){
                try(ResultSet rs = statement.executeQuery("select id,name from test")){
                    while(rs.next()){
                        Test t = new Test(rs.getLong(1),rs.getString(2));
                        allTests.put(t.id,t);
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
            List<Long> testids = new ArrayList<>(allTests.keySet());
            testids.sort(Comparator.naturalOrder());
            if(testId != null ){
                testids = List.of(testId);
            }
            for(Long testId : testids){
                Test test = allTests.get(testId);
                log(String.format("%3d - %s",test.id,test.name));
                try(PreparedStatement statement = connection.prepareStatement("select count(*) from run where testid=?")){
                    statement.setLong(1,testId);
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            log(2,"run count "+rs.getLong(1));
                        }
                    }
                }
                FolderEntity folder = new FolderEntity();
                folder.name=test.name;
                folder.group = new NodeGroupEntity(test.name);

                NodeTracking nodeTracking = new NodeTracking();

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
                                    transformers.add(new Transformer(transformId,rs.getString(1).replaceAll(":","_"),rs.getString(2),rs.getString(3),extractors));
                                }
                            }
                        }
                    }
                    assert transformers.size()==transformids.size();

                    if(transformers.size() > 1){
                        log("MORE THAN 1 TRANSFORMER FOR "+test);
                    }else {
                        for (Transformer transformer : transformers) {
                            List<Extractor> renamedExtractors = new ArrayList<>();
                            Map<String,String> extractorAliases = new HashMap<>();
                            transformer.extractors.forEach(e->{
                                Extractor newE = new Extractor("_"+e.name,e.jsonpath,e.isArray);
                                renamedExtractors.add(newE);
                                extractorAliases.put(e.name,newE.name);
                            });
                            String function = NodeService.renameParameters(transformer.function,extractorAliases);
                            //Label l = new Label(-1,"transform_"+transformer.name,function,renamedExtractors);
                            Label l  = new Label(-1,"transformer_"+transformer.name.replaceAll(":","_"),transformer.function,transformer.extractors);
                            NodeEntity transform = createNodesFromLabel(l,folder.group.root,folder.group,nodeTracking,new HashSet<>());
                            folder.group.addNode(transform);
                            nodeTracking.addNode(transform);

                            NodeEntity dataset = new JqNode("dataset",".[]",List.of(transform));
                            dataset.group=folder.group;
                            nodeTracking.addNode(dataset);

                            List<LabelDef> labelDefs = new ArrayList<>();
                            Set<String> labelNames = labelDefs.stream().map(LabelDef::name).collect(Collectors.toSet());
                            List<Label> targetSchemaLabels = new ArrayList<>();
                            try(PreparedStatement statement = connection.prepareStatement("select id,name,function from label where schema_id = (select id from schema where uri = ?)")){
                                statement.setString(1,transformer.targetUri);
                                try(ResultSet rs = statement.executeQuery()){
                                    while(rs.next()){
                                        labelDefs.add(new LabelDef(rs.getLong(1),rs.getString(2),rs.getString(3)));
                                    }
                                }
                            }
                            for(LabelDef labelDef : labelDefs){
                                if(nodeTracking.hasNode(labelDef.name)){
                                    //conflicting name for label but ignorable
                                    System.err.println("transform label conflict for "+labelDef+"\n  conflicts with "+nodeTracking.getNodes(labelDef.name));
                                }
                                try(PreparedStatement statement = connection.prepareStatement("select name,jsonpath,isarray from label_extractors where label_id = ?")){
                                    statement.setLong(1,labelDef.id);
                                    List<Extractor> extractors = new ArrayList<>();
                                    try(ResultSet rs = statement.executeQuery()){
                                        while(rs.next()){
                                            extractors.add(new Extractor(rs.getString(1),rs.getString(2),rs.getBoolean(3)));
                                        }
                                    }
                                    targetSchemaLabels.add(new Label(labelDef.id,labelDef.name.replaceAll(":","_"),labelDef.function,extractors));
                                }
                            }
                            labelDefs.clear();//so we don't accidentally use it
                            for(Label label : targetSchemaLabels){
                                log(6,"label="+label);
                                NodeEntity labelNode = createNodesFromLabel(label,dataset,folder.group,nodeTracking,labelNames);
                                if ( labelNode!=null ) {
                                    nodeTracking.tagNodeAsLabel(label,labelNode);
                                    folder.group.addNode(labelNode);
                                }else{
                                    System.out.println("FAILURE NULL NODE FOR LABEL "+label);
                                    System.exit(1);
                                    //reused
                                }
                            }
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
                        NodeEntity sourceNode = folder.group.root;
                        if(!jsonpath.equals("$.\"$schema\"")){
                            String sourcePath = jsonpath.substring(0,jsonpath.indexOf(".\"$schema\""));
                            log(4,"Creating a new source node for "+jsonpath+" -> "+sourcePath);

                            sourceNode = new JqNode(sourcePath,sourcePath,folder.group.root);
                            folder.group.addNode(sourceNode);
                            nodeTracking.addNode(sourceNode);
                        }
                        log(4,"creating labels");
                        for(String labelName : labelsByName.keys()){
                            log(6,"label="+labelName);
                            Set<Label> labels = labelsByName.get(labelName);
                            for(Label label : labels){
                                NodeEntity labelNode = createNodesFromLabel(label,sourceNode,folder.group,nodeTracking,schemaLabelsByName.keys());
                                if(labelNode!=null){
                                    //if this label needs to be renamed and part of a merge group
                                    if(schemaLabelsByName.get(labelName).size()>1) {
                                        labelNode.name = labelNode.name + nodesByOriginalName.get(labelName).size();
                                        nodesByOriginalName.put(labelName,labelNode);
                                    } else {
                                        nodeTracking.tagNodeAsLabel(label, labelNode);
                                    }
                                }
                            }
                        }
                    } // for each jsonpath
                    //create all the merge nodes to resolve label name conflicts
                    for(String labelName : nodesByOriginalName.keys()){
                        List<NodeEntity> sourceNodes = nodesByOriginalName.get(labelName);
                        NodeEntity newNode = new JsNode(labelName,"obj=>Object.values(obj).find(v => v != null)",sourceNodes);
                        folder.group.addNode(newNode);
                        nodeTracking.addNode(newNode);
                        nodeTracking.tagNodeAsLabel(new Label(-1,labelName,newNode.operation,Collections.emptyList()),newNode);
                    }
                }

                //at this point the test should have all the nodes from schemas, time to create nodes from variables
                Map<Long,NodeEntity> variableIdToNode = new HashMap<>();
                try(PreparedStatement statement = connection.prepareStatement("select id,name,labels,calculation from variable where testid=?")) {
                    statement.setLong(1, testId);
                    ObjectMapper mapper = new ObjectMapper();
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            Long id = rs.getLong("id");
                            String name = rs.getString("name");
                            ArrayNode labels = (ArrayNode) mapper.readTree(rs.getString("labels"));
                            String calculation = rs.getString("calculation");
                            //if this node is just an alias
                            if(calculation==null || calculation.isEmpty()){
                                if(labels.size()==1){
                                    String labelName = StringUtil.removeQuotes(labels.get(0).asText()).replaceAll(":","_");
                                    List<NodeEntity> found = nodeTracking.getLabelNodes(labelName);
                                    if(found.size()==1){
                                        variableIdToNode.put(id,found.get(0));
                                    }else {
                                        System.out.println("FAILED TO MAKE VARIABLE "+id+" for "+test.name+". Found count for "+labelName+" is "+found.size()+"\n"+found.stream().map(NodeEntity::toString).collect(Collectors.joining("\n"))+"\n labels="+labels);
                                        if(found.size()>1){
                                            System.exit(1);
                                        }
                                    }
                                }else{
                                    //THIS IS NOT EXPECTED
                                }
                            }else{
                                //create a new Node
                                List<NodeEntity> sources = new ArrayList<>();
                                for(int i=0;i<labels.size();i++){
                                    String sourceName = StringUtil.removeQuotes(labels.get(i).toString());
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
                                NodeEntity variableNode = new JsNode(name,calculation,sources);
                                folder.group.addNode(variableNode);
                                nodeTracking.addNode(variableNode);
                                variableIdToNode.put(id,variableNode);
                            }
                        }
                    }
                }
                //all variables are either aliases (when there isn't a calculation) or a Node
                //create fingerprint nodes
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
                            if( fingerprint_labels.size() > 0) {
                                List<NodeEntity> fingerprintNodes = new ArrayList<>();
                                for (int i = 0; i < fingerprint_labels.size(); i++) {
                                    String labelName = StringUtil.removeQuotes(fingerprint_labels.get(i).toString());
                                    if (nodeTracking.hasNode(labelName)) {
                                        List<NodeEntity> foundNodes = nodeTracking.getLabelNodes(labelName);
                                        if (foundNodes.size() == 1) {
                                            fingerprintNodes.add(foundNodes.get(0));
                                        } else {
                                            // report the ambiguity?
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
                                    System.out.println("FAILED TO CREATE FINGERPRINT FOR "+test.name+" from "+fingerprint_labels);
                                    //todo log error
                                }
                            }
                        }
                    }
                }
                //create change detections with fingerprints and nodeIds
                try(PreparedStatement statement = connection.prepareStatement("select id,variable_id,model,config from changedetection where variable_id in (select id from variable where testid = ?)")){
                    statement.setLong(1,testId);
                    ObjectMapper mapper = new ObjectMapper();
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            Long id = rs.getLong("id");
                            Long variableId = rs.getLong("variable_id");
                            NodeEntity  variableNode = variableIdToNode.get(variableId);
                            String fingerPrintName = test.name + "_fingerprint";
                            NodeEntity fingerprintNode = nodeTracking.hasNode(fingerPrintName) ? nodeTracking.getNodes(fingerPrintName).get(0) : null;
                            NodeEntity groupBy = nodeTracking.hasNode("dataset") ? nodeTracking.getNodes("dataset").get(0) : folder.group.root;
                            String model = rs.getString("model");
                            ObjectNode config = (ObjectNode) mapper.readTree(rs.getString("config"));
                            if(fingerprintNode == null){
                                fingerprintNode = new FingerprintNode(fingerPrintName,"",Collections.emptyList());
                                folder.group.addNode(fingerprintNode);
                                nodeTracking.addNode(fingerprintNode);
                            }
                            if(variableNode == null){
                                System.out.println("FAILED TO FIND VARIABLE "+variableId);
                                continue;
                            }
                            NodeEntity changeNode = switch (model) {
                                case "relativeDifference"-> {
                                    RelativeDifference difference = new RelativeDifference();
                                    difference.name="rd."+variableNode.name+"."+id;
                                    difference.setFilter(config.get("filter").asText());
                                    difference.setWindow(config.get("window").asInt());
                                    difference.setThreshold(config.get("threshold").asDouble());
                                    difference.setMinPrevious(config.get("minPrevious").asInt());
                                    difference.setNodes(fingerprintNode,groupBy,variableNode,null);
                                    if(fingerprint_filter!=null && !fingerprint_filter.isEmpty()){
                                        difference.setFingerprintFilter(fingerprint_filter);
                                    }
                                    yield difference;
                                }
                                case "fixedThreshold" -> {
                                    ObjectNode max = (ObjectNode) config.get("max");
                                    ObjectNode min = (ObjectNode) config.get("min");
                                    FixedThreshold fixedThreshold = new FixedThreshold();
                                    fixedThreshold.name="ft"+id;
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
                                    divisive.name="ed"+id;
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
                    }
                }
                //TODO create a folderService method that persists an entity
                //FolderEntity.persist(folder);
                folderService.create(folder);
            }
        }
        finally {
            ds.close();
        }
        return 0;
    }
}
