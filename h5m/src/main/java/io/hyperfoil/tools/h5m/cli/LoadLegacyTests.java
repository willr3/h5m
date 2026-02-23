package io.hyperfoil.tools.h5m.cli;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.entity.Folder;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.node.JsNode;
import io.hyperfoil.tools.h5m.entity.node.SqlJsonpathAllNode;
import io.hyperfoil.tools.h5m.entity.node.SqlJsonpathNode;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.yaup.Counters;
import io.hyperfoil.tools.yaup.HashedLists;
import io.hyperfoil.tools.yaup.HashedSets;
import io.hyperfoil.tools.yaup.StringUtil;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name="load-legacy-tests")
public class LoadLegacyTests implements Callable<Integer> {

    @CommandLine.Option(names = {"username"}, description = "legacy db username", defaultValue = "quarkus") String username;
    @CommandLine.Option(names = {"password"}, description = "legacy db password", defaultValue = "quarkus") String password;
    @CommandLine.Option(names = {"url"}, description = "legacy connection url",defaultValue = "jdbc:postgresql://0.0.0.0:") String url;


    @Inject
    FolderService folderService;

    @Inject
    NodeService nodeService;

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
    @Override
    public Integer call() throws Exception {
        Counters<String> counters = new Counters<>();
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
                System.out.println("Creating run_schema_paths");
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS run_schema_paths as select id,testid,paths,jsonb_path_query_first(data,paths::jsonpath) as schema from run ,lateral (select jsonb_paths(data,3,'$') as paths) where paths like '%$schema%';");
            }
            try(Statement statement = connection.createStatement()){
                System.out.println("Creating dataset_schema_paths");
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS dataset_schema_paths as select id,testid,runid,paths,jsonb_path_query_first(data,paths::jsonpath) as schema from dataset, lateral (select jsonb_paths(data,3,'$') as paths) where paths like '%schema%';");
            };
            System.out.println("Loading legacy tests");
            try(Statement statement = connection.createStatement()){
                try(ResultSet rs = statement.executeQuery("select id,name from test")){
                    while(rs.next()){
                        Test t = new Test(rs.getLong(1),rs.getString(2));
                        allTests.put(t.id,t);
                    }
                }
            }
            //load all test transformations
            System.out.println("Loading legacy test transformers");
            try(PreparedStatement statement = connection.prepareStatement("select id,name from test where id = ?;")){

            }
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
                System.out.printf("%3d - %s%n",test.id,test.name);
                try(PreparedStatement statement = connection.prepareStatement("select count(*) from run where testid=?")){
                    statement.setLong(1,testId);
                    try(ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            System.out.println("  run count "+rs.getLong(1));
                        }
                    }
                }
                Folder folder = new Folder(test.name);
                folderService.create(folder);

                List<Node> nodes = new ArrayList<>();
                HashedLists<String,Node> nodesByName = new HashedLists<>();

                if(testToTransformer.has(test.id)){
                    System.out.println("  has datasets");
                    counters.add("dataset");
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
                        System.out.println("MORE THAN 1 TRANSFORMER FOR "+test);
                        counters.add("multiple transformer");
                    }else {
                        //not yet ready for the for loop to handle more than 1 cycle :)
                        for (Transformer transformer : transformers) {
                            HashedLists<String, Node> transformerNodesByName = new HashedLists<>();
                            for (Extractor extractor : transformer.extractors) {
                                Node node = extractor.isArray ?
                                        SqlJsonpathAllNode.parse(extractor.name(), extractor.jsonpath(), nodesByName::get) :
                                        SqlJsonpathNode.parse(extractor.name(), extractor.jsonpath(), nodesByName::get);
                                if (node == null) {
                                    System.err.println("failed to create node for extractor " + extractor);
                                    return 1;
                                }
                                node.group = folder.group;
                                node = nodeService.create(node);
                                nodes.add(node);
                                nodesByName.put(node.name, node);
                                transformerNodesByName.put(node.name, node);
                            }

                            Node dataset = JsNode.parse("dataset", transformer.function, transformerNodesByName::get);
                            if (dataset == null) {
                                //this means there is a missing parameter?
                                System.out.println("Failed to create dataset node:\n" + transformer.function);
                                return 1;
                            }
                            dataset.group=folder.group;
                            dataset = nodeService.create(dataset);
                            nodes.add(dataset);
                            nodesByName.put(dataset.name, dataset);

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
                                if(nodesByName.containsKey(labelDef.name)){
                                    counters.add("conflicting dataset label name");
                                    //conflicting name for label
                                    System.err.println("label "+labelDef+"\n  conflicts with "+nodesByName.get(labelDef.name));
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
                                HashedLists<String, Node> labelNodesByName = new HashedLists<>();
                                for(Extractor extractor : label.extractors ){
                                    if(nodesByName.containsKey(extractor.name)){
                                        counters.add("conflicting extractor name");
                                        System.err.println("extractor "+extractor+"\n  conflicts with "+nodesByName.get(extractor.name));
                                    }
                                    Node node = extractor.isArray ?
                                            SqlJsonpathAllNode.parse(extractor.name(), extractor.jsonpath(),nodesByName::get) :
                                            SqlJsonpathNode.parse(extractor.name(), extractor.jsonpath(),nodesByName::get);
                                    if(node==null){
                                        System.err.println("failed to create node for extractor "+extractor);
                                        return 1;
                                    }
                                    node.group=folder.group;
                                    node.sources=List.of(dataset);
                                    node = nodeService.create(node);
                                    nodes.add(node);
                                    nodesByName.put(node.name, node);
                                    labelNodesByName.put(node.name, node);
                                }
                                if(label.function==null || label.function.isEmpty()){
                                    //this can happen for single extractor labels?
                                    if(label.extractors.size() > 1) {
                                        //TODO use an identify function
                                        counters.add("missing multi extractor function");
                                        System.err.println("missing function: " + label + "\n Extractors:\n  " + label.extractors.stream().map(Extractor::toString).collect(Collectors.joining("\n  ")));
                                    }
                                    //return 1;
                                }else {
                                    Node labelNode = JsNode.parse(label.name, label.function, labelNodesByName::get);
                                    if (labelNode == null) {

                                        List<String> params = JsNode.getParameterNames(label.function);
                                        if(params.size()==1) {//collect all extractors into the value
                                            labelNode = new JsNode(label.name,label.function,labelNodesByName.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                                        } else {
                                            System.err.println("Failed to make node for Label:" + label.name + "\n" + label.function + "\n  " + label.extractors.stream().map(Extractor::toString).collect(Collectors.joining("\n  ")));
                                            return 1;
                                        }
                                    }
                                    labelNode.group=folder.group;
                                    labelNode = nodeService.create(labelNode);
                                    nodes.add(labelNode);
                                    nodesByName.put(labelNode.name, labelNode);
                                }
                            }
                        }
                    }
                } else {
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
                                    schemaLabels.put(schemaUri,new Label(labelDef.id,labelDef.name,labelDef.function,extractors));
                                }
                            }
                        }
                    }
                    //
                    for(String jsonpath : schemaByPath.keys()){
                        Set<String> schemas = schemaByPath.get(jsonpath);
                        List<Label> allLabels = schemas.stream().filter(schemaLabels::containsKey).flatMap(s->schemaLabels.get(s).stream()).toList();
                        HashedLists<String,Label> labelsByName = new HashedLists<>();
                        HashedSets<String,Label> uniqueLabelsByname = new HashedSets<>();
                        allLabels.stream().forEach(e->{
                            labelsByName.put(e.name,e);
                            uniqueLabelsByname.put(e.name,e);
                        });

                        System.out.println("  "+jsonpath+" -> "+schemas+" "+allLabels.size()+" label(s)");

                        Node sourceNode = folder.group.root;
                        if(!jsonpath.equals("$.\"$schema\"")){
                            counters.add("nested $schema");
                            System.out.println("    TODO create a new source node for "+jsonpath);
                            //TODO create a new sourceNode
                        }
                        for(String labelName : labelsByName.keys()){
                            List<Label> labels = labelsByName.get(labelName);
                            boolean allSame = labels.stream().allMatch(l->labels.stream().allMatch(l2->l.equals(l2)));
                            if(labels.size()>1 && !allSame){
                                counters.add("duplicated label name");
                                System.out.println("      duplicated label name "+labelName+
                                        "\n        "+labels.stream().map(Objects::toString).collect(Collectors.joining("\n        ")));
                                System.out.println("    unique count: "+uniqueLabelsByname.get(labelName).size()+
                                        "\n      "+uniqueLabelsByname.get(labelName).stream().map(Objects::toString).collect(Collectors.joining("\n      ")));
                            }


                        }
                    }
                }
            }
        }

        for(String key : counters.entries()){
            System.out.println(key+" : "+counters.count(key));
        }
        return 0;
    }
}
