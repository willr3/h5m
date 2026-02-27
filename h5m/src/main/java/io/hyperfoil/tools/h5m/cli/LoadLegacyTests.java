package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.entity.Folder;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.yaup.HashedLists;
import io.hyperfoil.tools.yaup.HashedSets;
import io.hyperfoil.tools.yaup.StringUtil;
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
    @CommandLine.Option(names = {"url"}, description = "legacy connection url",defaultValue = "jdbc:postgresql://0.0.0.0:") String url;


    public class NodeTracking {

        Map<Extractor,Node> extractorNodes = new HashMap<>();
        Map<Label,Node> labelNodes = new HashMap<>();
        HashedLists nodesByName = new HashedLists();

        public void addNode(Extractor extractor, Node node){
            extractorNodes.put(extractor,node);
            nodesByName.put(node.name, node);
        }
        public void addNode(Label label, Node node){
            labelNodes.put(label,node);
            nodesByName.put(node.name, node);
        }
        public void addNode(Node node){
            nodesByName.put(node.name, node);
        }
        public boolean hasNode(Extractor extractor){
            return extractorNodes.containsKey(extractor);
        }
        public boolean hasNode(Label label){
            return labelNodes.containsKey(label);
        }
        public boolean hasNode(String name){
            return nodesByName.containsKey(name);
        }
        public Node getNode(Extractor extractor){
            return extractorNodes.get(extractor);
        }
        public Node getNode(Label label){
            return labelNodes.get(label);
        }
        public List<Node> getNodes(String name){
            return nodesByName.get(name);
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


    public Node createNodesFromLabel(Label label,Node source,Folder folder, NodeTracking nodeTracking){
        Node rtrn = null;
        HashedLists<String,Node> labelNodesByName = new HashedLists<>();
        for(Extractor extractor : label.extractors) {
            Node node = extractor.isArray ?
                    SqlJsonpathAllNode.parse(extractor.name(), extractor.jsonpath(), nodeTracking::getNodes) :
                    SqlJsonpathNode.parse(extractor.name(), extractor.jsonpath(), nodeTracking::getNodes);
            if (node == null) {
                System.err.println("failed to create node for extractor " + extractor);
                return null;
            }
            node.group = folder.group;
            node.sources = List.of(source);

            if(nodeTracking.hasNode(extractor) && nodeService.functionalyEquivalent(node,nodeTracking.getNode(extractor))){
                node = nodeTracking.getNode(extractor);
            }else{
                System.out.println("creating conflicting Node for extractor="+extractor+"\n node="+node+"\n conflicting="+nodeTracking.getNode(extractor));
                node = nodeService.create(node);
                nodeTracking.addNode(extractor, node);
            }
            labelNodesByName.put(node.name, node);
        }
        if(label.function==null || label.function.trim().isEmpty()){
            //this can happen for single extractor labels?
            if(label.extractors.size() > 1) {
                rtrn = new JsNode(label.name(),"v=>v",labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            }else if (label.extractors.size() == 1) {
                if(label.name != label.extractors.get(0).name && !label.name.trim().isEmpty()){
                    List<Node> extractorNodes = labelNodesByName.get(label.name);
                    if(extractorNodes.size()==1){
                        extractorNodes.get(0).name = label.name;
                    }else{
                        //This is a condition we do not expect
                    }
                }
            }else{
                //why do we have a label without an extractor or function?
            }
            //return 1;
        }else {
            rtrn = JsNode.parse(label.name, label.function, labelNodesByName::get);
            if (rtrn == null) {
                List<String> params = JsNode.getParameterNames(label.function);
                if(params.size()==1) {//collect all extractors into the value
                    rtrn = new JsNode(label.name,label.function,labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                } else {
                    rtrn = JsNode.parse(label.name, label.function, labelNodesByName::get,true);
                    if(rtrn==null){
                        System.err.println("Failed to make node for Label:" + label.name + "\n" + label.function + "\n  " + label.extractors.stream().map(Extractor::toString).collect(Collectors.joining("\n  ")));
                        return null;
                    }
                }
            }
        }
        if(rtrn!=null){
            rtrn.group=folder.group;
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
                Folder folder = new Folder(test.name);
                folderService.create(folder);
                //HashedLists<String,Node> nodesByName = new HashedLists<>();
                NodeTracking nodeTracking = new NodeTracking();


                if(testToTransformer.has(test.id)){
                    log(2,"has datasets");
                    Set<Long> transformids = testToTransformer.get(test.id);
                    List<Transformer> transformers = new ArrayList<>();
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
                                    transformers.add(new Transformer(transformId,rs.getString(1),rs.getString(2),rs.getString(3),extractors));
                                }
                            }
                        }
                    }
                    assert transformers.size()==transformids.size();

                    if(transformers.size() > 1){
                        log("MORE THAN 1 TRANSFORMER FOR "+test);
                    }else {
                        for (Transformer transformer : transformers) {
                            Label l = new Label(-1,"dataset",transformer.function,transformer.extractors);
                            Node dataset = createNodesFromLabel(l,folder.group.root,folder,nodeTracking);
                            dataset.group=folder.group;
                            dataset = nodeService.create(dataset);
                            nodeTracking.addNode(dataset);

                            List<LabelDef> labelDefs = new ArrayList<>();
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
                                    targetSchemaLabels.add(new Label(labelDef.id,labelDef.name,labelDef.function,extractors));
                                }
                            }
                            labelDefs.clear();//so we don't accidentally use it
                            for(Label label : targetSchemaLabels){
                                Node labelNode = createNodesFromLabel(label,dataset,folder,nodeTracking);
                                if ( labelNode==null ) {
                                    //this can happen if the label is missing a function and has only 1 extractor
                                    if (label.function==null || label.function.isEmpty() || label.extractors.size()<=1) {

                                    } else {
                                        //this is the real issue
                                        System.err.println("failed to create a node for label\n"+label);
                                        return -1;
                                    }
                                } else {
                                    labelNode.group=folder.group;
                                    labelNode = nodeService.create(labelNode);
                                    nodeTracking.addNode(labelNode);
                                }
                            }
                        }
                    }
                } else {
                    //no transform
                    HashedSets<String,String> schemaByPath = new HashedSets<>();
                    //get the jsonpath -> schema used across all runs in this test
                    try (PreparedStatement statement = connection.prepareStatement("select p.paths,p.schema from run_schema_paths p where testid = ?")) {
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
                    HashedSets<String,Label> schemaLabelsByName = new HashedSets<>();
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
                                    Label newLabel = new Label(labelDef.id,labelDef.name,labelDef.function,extractors);
                                    schemaLabels.put(schemaUri,newLabel);
                                    schemaLabelsByName.put(newLabel.name,newLabel);
                                }
                            }
                        }
                    }
                    for(String labelName : schemaLabelsByName.keys()){
                        Set<Label> labels = schemaLabelsByName.get(labelName);
                        if(labels.size()>1){
                            System.out.println("CONFLICTING NAME "+labelName+":\n  "+labels.stream().map(Objects::toString).collect(Collectors.joining("\n  ")));
                        }
                    }
                    HashedLists<String,Node> nodesByOriginalName = new HashedLists<>();

                    for(String jsonpath : schemaByPath.keys()){
                        Set<String> schemas = schemaByPath.get(jsonpath);
                        List<Label> allLabelsForJsonpath = schemas.stream().filter(schemaLabels::containsKey).flatMap(s->schemaLabels.get(s).stream()).toList();
                        HashedSets<String,Label> labelsByName = new HashedSets<>();
                        allLabelsForJsonpath.stream().forEach(e->{
                            labelsByName.put(e.name,e);
                        });

                        log(2,jsonpath+" -> "+schemas+" "+allLabelsForJsonpath.size()+" label(s)");

                        Node sourceNode = folder.group.root;
                        if(!jsonpath.equals("$.\"$schema\"")){
                            String sourcePath = jsonpath.substring(0,jsonpath.indexOf(".\"$schema\""));
                            log(4,"Creating a new source node for "+jsonpath+" -> "+sourcePath);

                            sourceNode = new JqNode(sourcePath,sourcePath,sourceNode);
                            sourceNode.group=folder.group;
                            sourceNode = nodeService.create(sourceNode);
                            nodeTracking.addNode(sourceNode);
                        }
                        log(4,"creating labels");
                        for(String labelName : labelsByName.keys()){
                            log(6,"label="+labelName);
                            Set<Label> labels = labelsByName.get(labelName);
                            for(Label label : labels){
                                Node labelNode = createNodesFromLabel(label,sourceNode,folder,nodeTracking);
                                if(labelNode!=null){
                                    //if this label needs to be renamed and part of a merge group
                                    if(schemaLabelsByName.get(labelName).size()>1){
                                        labelNode.name=labelNode.name+nodesByOriginalName.get(labelName).size();
                                    }
                                    labelNode.group=folder.group;
                                    labelNode = nodeService.create(labelNode);
                                    if(schemaLabelsByName.get(labelName).size()>1){
                                        nodesByOriginalName.put(labelName,labelNode);
                                    }
                                    nodeTracking.addNode(labelNode);

                                }
                            }
                        }
                    } // for each jsonpath
                    //create all the merge nodes to resolve label name conflicts
                    for(String labelName : nodesByOriginalName.keys()){
                        List<Node> sourceNodes = nodesByOriginalName.get(labelName);
                        Node newNode = new JsNode(labelName,"obj=>Object.values(obj).find(v => v != null)",sourceNodes);
                        newNode.group=folder.group;
                        newNode = nodeService.create(newNode);
                        nodeTracking.addNode(newNode);
                    }
                }

                //at this point the test should have all the nodes from schemas, time to create nodes from variables
                //Map<String,String> aliasToLabelName = new HashMap<>();
                Map<Long,Node> variableIdToNode = new HashMap<>();
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
                                    List<Node> found = nodeTracking.getNodes(labels.get(0).toString());
                                    if(found.size()==1){
                                        variableIdToNode.put(id,found.get(0));
                                    }
                                }else{
                                    //THIS IS NOT EXPECTED
                                }
                            }else{
                                //create a new Node
                                List<Node> sources = new ArrayList<>();
                                for(int i=0;i<labels.size();i++){
                                    String sourceName = StringUtil.removeQuotes(labels.get(i).toString());
                                    if(nodeTracking.hasNode(sourceName)){
                                        List<Node> foundNodes = nodeTracking.getNodes(sourceName);
                                        if(foundNodes.size()>1){
                                            //AMBIGUOUS LABEL
                                        }else{
                                            sources.add(foundNodes.get(0));
                                        }
                                    }else{
                                        //missing
                                    }
                                }
                                Node variableNode = new JsNode(name,calculation,sources);
                                variableNode.group=folder.group;
                                variableNode = nodeService.create(variableNode);
                                nodeTracking.addNode(variableNode);
                                variableIdToNode.put(id,variableNode);
                            }
                        }
                    }
                }
                //all variables are either aliases (when there isn't a calculation) or a Node


                //fingerprints...
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
                                List<Node> fingerprintNodes = new ArrayList<>();
                                for (int i = 0; i < fingerprint_labels.size(); i++) {
                                    String labelName = StringUtil.removeQuotes(fingerprint_labels.get(i).toString());
                                    if (nodeTracking.hasNode(labelName)) {
                                        List<Node> foundNodes = nodeTracking.getNodes(labelName);
                                        if (foundNodes.size() == 1) {
                                            fingerprintNodes.add(foundNodes.get(0));
                                        } else {
                                            // report the ambiguity?
                                        }
                                    }
                                }
                                if(fingerprintNodes.size()>1){
                                    Node newNode = new FingerprintNode(test.name + "_fingerprint", "", fingerprintNodes);
                                    newNode.group = folder.group;
                                    newNode = nodeService.create(newNode);
                                    nodeTracking.addNode(newNode);
                                }else{
                                    //todo log error
                                }

                            }

                        }
                    }
                }
                //create change detections with fingerprints and nodeIds
                try(PreparedStatement statement = connection.prepareStatement("select id,variable_id,model,config from changedetection where variable_id in (select id from variable where testid = ?)")){
                    Array array = connection.createArrayOf("integer",variableIdToNode.keySet().toArray());
                    //statement.setArray(1,array);
                    statement.setLong(1,testId);
                    ObjectMapper mapper = new ObjectMapper();
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            Long id = rs.getLong("id");
                            Long variableId = rs.getLong("variable_id");
                            Node  variableNode = variableIdToNode.get(variableId);
                            Node fingerprintNode = nodeTracking.hasNode(test.name + "_fingerprint") ? nodeTracking.getNodes(test.name + "_fingerprint").get(0) : null;
                            Node groupBy = nodeTracking.hasNode("dataset") ? nodeTracking.getNodes("dataset").get(0) : folder.group.root;
                            String model = rs.getString("model");
                            ObjectNode config = (ObjectNode) mapper.readTree(rs.getString("config"));

                            Node changeNode = switch (model) {
                                case "relativeDifference"-> {
                                    RelativeDifference difference = new RelativeDifference();
                                    difference.name="rd"+id;
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
                                    yield null;
                                }
                                case "eDivisive" -> {
                                    yield null;
                                }
                                default -> null;
                            };

                            if(changeNode!=null){
                                changeNode.group=folder.group;
                                changeNode = nodeService.create(changeNode);
                                nodeTracking.addNode(changeNode);
                            }

                        }
                    }

                }
            }
        }
        return 0;
    }
}
