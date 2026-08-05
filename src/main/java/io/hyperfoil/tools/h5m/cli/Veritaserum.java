package io.hyperfoil.tools.h5m.cli;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.api.*;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeGroupService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.h5m.svc.ValueService;
import io.hyperfoil.tools.jjq.value.*;
import io.hyperfoil.tools.yaup.Sets;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "veritaserum", description = "find the truth")
public class Veritaserum implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(Veritaserum.class);
    @Inject
    FolderService folderService;

    @Inject
    NodeService nodeService;

    @Inject
    ValueService valueService;

    @Inject
    NodeGroupService nodeGroupService;

    @CommandLine.Option(names = {"username"}, description = "legacy db username", defaultValue = "quarkus")
    String username;
    @CommandLine.Option(names = {"password"}, description = "legacy db password", defaultValue = "quarkus")
    String password;
    @CommandLine.Option(names = {"url"}, description = "legacy connection url", defaultValue = "jdbc:postgresql://0.0.0.0:6000/horreum")
    String url;
    @CommandLine.Option(names = {"testId"}, description = "Horreum test ID")
    Long testId;
    @CommandLine.Option(names = {"runId"}, description = "verify a specific run (optional)")
    Long runId;
    @CommandLine.Option(names = {"limit"}, description = "max runs to verify", defaultValue = "5")
    int limit;
    @CommandLine.Option(names = {"offset"}, description = "max runs to verify", defaultValue = "0")
    int offset;

    @Inject
    LoadLegacyTests loadLegacyTests;

    @Override
    public Integer call() throws Exception {



        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, "1");
        props.put(AgroalPropertiesReader.MIN_SIZE, "1");
        props.put(AgroalPropertiesReader.INITIAL_SIZE, "1");
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.PRINCIPAL, username);
        props.put(AgroalPropertiesReader.CREDENTIAL, password);
        props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME, "org.postgresql.Driver");
        props.put(AgroalPropertiesReader.JDBC_URL, url);
        AgroalDataSource legacyDs = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props).get());
        Folder folder = null;

        loadLegacyTests.username = username;
        loadLegacyTests.password = password;
        loadLegacyTests.url      = url;
        loadLegacyTests.testId   = testId;
        loadLegacyTests.keepAll  = true;

        int ec = loadLegacyTests.call();

        System.out.println(ec);


        try (Connection legacyConn = legacyDs.getConnection()) {
            String testName = getTestName(legacyConn,testId);
            if (testName == null) {
                System.err.println("Test not found: " + testId);
                return 1;
            }
            folder = folderService.byName(testName);
            if (folder == null) {
                System.out.println("failed to find folder "+testName);
                return 1;
            }

            JqValue runData = fetchRun(legacyConn,runId);
            if (runData == null) {
                System.out.println("failed to find run "+runId);
                return 1;
            }

            Upload upload = folderService.upload(folder.name(),"",runData);
            upload.future.orTimeout(3, TimeUnit.MINUTES);
            upload.future.join();
            if(upload.future.isCompletedExceptionally()){
                System.out.println("upload failed");
                return 1;
            }

            List<LoadLegacyTests.Transformer> transformers = loadTransformers(legacyConn,testId);
            if(!transformers.isEmpty()){
                for(LoadLegacyTests.Transformer transformer : transformers){
                    System.out.println("Transfomer: "+transformer.name());
                    List<Node> transformerMatches = nodeService.findNodeByFqdn(transformer.name(),folder.groupId());
                    if(transformerMatches.isEmpty()){
                        String name = LoadLegacyTests.getRename(transformer,transformers.size());
                        transformerMatches = nodeService.findNodeByFqdn(name,folder.groupId());
                    }
                    if(transformerMatches.isEmpty()){
                        System.out.println("failed to find match for transformer "+transformer.name());
                        continue;
                    }
                    Node transformerMatch = transformerMatches.get(0);
                    for(Node extractorNode : transformerMatch.sources()){
                        var matchingExtractor = transformer.extractors().stream().filter(e->e.name().equals(extractorNode.name())).findFirst().orElse(null);
                        if(matchingExtractor == null){
                            System.out.println("failed to find match for transformer extractor "+extractorNode.name());
                            continue;
                        }
                        JqValue fromExtractor = extractRun(legacyConn,runId,matchingExtractor.jsonpath(),matchingExtractor.isArray());
                        List<Value> h5mValues = valueService.getNodeValues(extractorNode.id());
                        JqValue fromH5m = h5mValues.isEmpty() ? null : h5mValues.getFirst().data();
                        boolean eq = fromExtractor.equals(fromH5m);

                        System.out.println(extractorNode.name() + " eq=" + fromExtractor.equals(fromH5m));
                        if (!eq) {
                            System.out.println("  H: ");
                            System.out.println("    filter: " + matchingExtractor.jsonpath()+" array="+matchingExtractor.isArray());
                            System.out.println("    value:  " + s(fromExtractor));
                            System.out.println("  5: ");
                            System.out.println("    filter: " + extractorNode.operation());
                            System.out.println("    value:  " + s(fromH5m));
                        }
                    }
                    List<Long> datasetIds = getDatasetIds(legacyConn,runId);
                    System.out.println("Target Labels");
                    for(LoadLegacyTests.Label label : transformer.targetSchemaLabels()){
                        System.out.println("\nLabel: "+label.name());
                        List<Node> labelNodes = nodeService.findNodeByFqdn(label.name(),folder.groupId());
                        if(labelNodes.isEmpty()){
                            System.out.println("failed to find match for label "+label.name());
                        }
                        Node labelNode = labelNodes.get(0);
                        for(Node extractorNode : labelNode.sources()){
                            var matchingExtractor = label.extractors().stream().filter(e->e.name().equals(extractorNode.name())).findFirst().orElse(null);
                            if(matchingExtractor == null){
                                System.out.println("failed to find match for label extractor "+extractorNode.name());
                                continue;
                            }
                            List<Value> h5mValues = valueService.getNodeValues(extractorNode.id());
                            List<JqValue> fromExtractors = datasetIds.stream().map(id-> {
                                try {
                                    return extractDataset(legacyConn,id,matchingExtractor.jsonpath(),matchingExtractor.isArray());
                                } catch (SQLException e) {
                                    return JqNull.NULL;
                                }
                            }).toList();
                            for(int i=0; i<fromExtractors.size(); i++){
                                JqValue fromExtractor = fromExtractors.get(i);
                                JqValue fromH5m = h5mValues.size() > i ? fromExtractors.get(i) : null;
                                boolean eq = fromExtractor.equals(fromH5m);

                                System.out.println(extractorNode.name() + " eq=" + fromExtractor.equals(fromH5m));
                                if (!eq) {
                                    System.out.println("  H: ");
                                    System.out.println("    filter: " + matchingExtractor.jsonpath()+" array="+matchingExtractor.isArray());
                                    System.out.println("    value:  " + s(fromExtractor));
                                    System.out.println("  5: ");
                                    System.out.println("    filter: " + extractorNode.operation());
                                    System.out.println("    value:  " + s(fromH5m));
                                }
                            }

                        }
                    }

                }
            }

        }
        return 0;
    }
    private List<Long> getDatasetIds(Connection connection, Long runId) throws SQLException {
        List<Long> datasetIds = new ArrayList<>();
        try(PreparedStatement statement = connection.prepareStatement("select id from dataset where runid = ?")){
            statement.setLong(1, runId);
            try (ResultSet rs = statement.executeQuery()){
                while (rs.next()) {
                    datasetIds.add(rs.getLong(1));
                }
            }
        }
        return datasetIds;
    }
    private JqValue fetchRun(Connection legacyConn,long runId) throws SQLException{
        JqValue rtrn = null;
        try(PreparedStatement stmt = legacyConn.prepareStatement("select data from run where id=?")){
            stmt.setLong(1, runId);
            try (ResultSet rs = stmt.executeQuery()){
               while(rs.next()){
                   if(rtrn !=null){
                       //please no
                   }
                   byte[] bytes = rs.getBytes(1);
                   rtrn = JqValues.parse(bytes);
               }
            }
        }
        return rtrn;
    }
    private int compareLabelValues(AgroalDataSource legacyDs) throws SQLException {
        JqValue runLabelValues = null;
        Folder folder = null;
        try (Connection legacyConn = legacyDs.getConnection()) {
            String testName = getTestName(legacyConn,testId);
            if (testName == null) {
                System.err.println("Test not found: " + testId);
                return 1;
            }
            folder = folderService.byName(testName);
            if (folder == null) {
                System.out.println("failed to find folder "+testName);
                return 1;
            }
            System.out.println("Verifying test: " + testName + " (id=" + testId + ")");

            runLabelValues = getRunLabelValues(legacyConn,runId);
        }
        if(runLabelValues == null){
            System.out.println("No labelValues found for "+runId);
            return 1;
        }
        List<JqObject> horreumValues = new ArrayList<>();
        for(int i=0;i<runLabelValues.length(); i++){
            JqObject h = (JqObject)runLabelValues.getElement(i);
            JqObject.Builder b = JqObject.builder();
            h.forEach((k,v)->{
                if(!v.isNull()){
                    b.put(k,v);
                }
            });
            horreumValues.add(b.build());
        }
        List<Node> datasetNodes = nodeService.findNodeByFqdn("dataset",folder.groupId());
        NodeGroup nodeGroup = nodeGroupService.byId(folder.groupId());
        long nodeId = datasetNodes.size()==1 ? datasetNodes.getFirst().id() : nodeGroup.root().id();
        List<Value> rootValues = valueService.getNodeValues(nodeGroup.root().id());

        List<JqValue> tmp = valueService.getGroupedValues(nodeId,rootValues.getFirst().id(),null,null,null);
        List<JqObject> h5mValues = new ArrayList<>();
        for(JqValue t:tmp){
            JqObject.Builder b = JqObject.builder();
            ((JqObject)t).forEach((k,v)->{
                if(!v.isNull()){
                    b.put(k,v);
                }
            });
            h5mValues.add(b.build());
        }

        System.out.println("values.size="+h5mValues.size());
        System.out.println("labelValues.size="+runLabelValues.length());

        double[][] eqs = new double[horreumValues.size()][h5mValues.size()];

        for(int h=0; h<horreumValues.size(); h++){
            for(int m=0; m<h5mValues.size(); m++){
                eqs[h][m] = score( horreumValues.get(h), h5mValues.get(m));
            }
        }
        System.out.print("    ");
        for(int h=0; h<h5mValues.size(); h++){
            System.out.print(h<10 ? ("  "+h+"  "):(" "+h+" "));
        }
        System.out.println("");
        for(int h=0; h<horreumValues.size(); h++){
            System.out.print((h<10 ? ("  "+h+"  "):("  "+h+"  ")));
            for(int m=0; m<h5mValues.size(); m++){
                System.out.printf(" %.1f ",eqs[h][m]);
            }
            System.out.println("");
        }
        for(int i=0; i<h5mValues.size(); i++){
            System.out.println(i+":");
            JqObject h = (JqObject) horreumValues.get(i);
            JqObject m = (JqObject) h5mValues.get(i);
            Set<String> keys = Sets.join(h.keys(),m.keys());
            for(String k : keys){
                System.out.println("  "+k+" "+(h.has(k) && m.has(k) && h.get(k).equals(m.get(k))));
                System.out.println("    h="+(h.has(k) ? s(h.get(k)) : ""));
                System.out.println("    m="+(m.has(k) ? s(m.get(k)) : ""));
            }
        }

        return 0;
    }
    private JqObject stripNull(JqObject input){
        JqObject.Builder b = JqObject.builder();
        input.forEach((k,v)->{
            if(v==null || v.isNull()){

            }else{
                b.put(k,v);
            }
        });
        return b.build();
    }

    private JqArray getRunLabelValues(Connection conn, long runId){
        JqValue value = null;
        try(PreparedStatement ps = conn.prepareStatement(
                """
                WITH
                combined as (
                    SELECT
                        label.name AS labelName,
                        lv.value AS value, runId,
                        dataset.id AS datasetId,
                        dataset.start AS start,
                        dataset.stop AS stop
                    FROM 
                        dataset
                        LEFT JOIN label_values lv ON dataset.id = lv.dataset_id
                        LEFT JOIN label ON label.id = lv.label_id
                    WHERE dataset.runid = ?
                ), entry as (
                    SELECT 
                        runId,
                        datasetId,
                        jsonb_object_agg(labelName,value) as data 
                    from 
                        combined 
                    group by runId,datasetId
                ) 
                select 
                    jsonb_agg(                        
                        data -- jsonb_build_object('runId',runId,'datasetId',datasetId,'data',data)
                    ) from entry                        
                """
        )){
            ps.setLong(1, runId);
            try(ResultSet rs = ps.executeQuery()) {
                while(rs.next()){
                    if(value!=null){
                        //This should not happen
                    }
                    value = JqValues.parse(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if(value.isArray()){
            return (JqArray)value;
        }else{
            return JqArray.EMPTY;
        }
    }
    private static List<LoadLegacyTests.Transformer> loadTransformers(Connection conn,long testId) throws SQLException {
        List<LoadLegacyTests.Transformer> transformers = new ArrayList<>();
        List<Long> transformerIds = new ArrayList<>();
        try(PreparedStatement statement = conn.prepareStatement("select transformer_id from test_transformers where test_id = ?")){
            statement.setLong(1, testId);
            try(ResultSet rs = statement.executeQuery()){
                while(rs.next()){
                    transformerIds.add(rs.getLong(1));
                }
            }
        }
        for(Long transformerId : transformerIds){
            transformers.add(LoadLegacyTests.loadTransformer(conn, transformerId));
        }
        return transformers;
    }
    private static JqValue extractRun(Connection conn, long runId,String jsonpath,boolean array) throws SQLException {
        return extract(conn,runId,jsonpath,"run",array);
    }
    private static JqValue extractDataset(Connection conn, long datasetId,String jsonpath,boolean array) throws SQLException {
        return extract(conn,datasetId,jsonpath,"dataset",array);
    }
    private static JqValue extract(Connection conn, long id,String jsonpath,String target,boolean array) throws SQLException {
        JqValue value = null;
        String op = array?"jsonb_path_query_array":"jsonb_path_query_first";

        try(PreparedStatement ps = conn.prepareStatement(
                """
                select OP(data,?::jsonpath) from TARGET where id = ?;
                """.replaceAll("OP",op).replaceAll("TARGET",target))
        ){
            ps.setString(1, jsonpath);
            ps.setLong(2, id);
            try(ResultSet rs = ps.executeQuery()) {
                while(rs.next()){
                    if(value != null){
                        //this should not happened
                    }
                    String response = rs.getString(1);
                    if(response==null){
                        System.out.println("NULL for "+jsonpath+" id="+id+" target="+target+" array="+array);
                    }else {
                        value = JqValues.parse(response);
                    }
                }
            }
        }
        if(value == null){
            return JqNull.NULL;
        }
        return value;
    }
    private String s(Object o){
        if(o == null){
            return "";
        }
        int limit = 180;
        String s = o.toString();
        if(s.length()>limit){
            return s.length()+": "+s.substring(0,limit);
        }else
            return s;
    }
    private double score(JqObject a, JqObject b){
        int div = Math.max(a.length(),b.length());
        Set<String> keys = Sets.join(a.keys(),b.keys());
        int count = 0;
        for(String k : keys){
            if(a.has(k) && b.has(k) && a.get(k).equals(b.get(k))){
                count++;
            }
        }
        return count / (double)div;
    }
    private String getTestName(Connection conn, long testId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM test WHERE id = ?")) {
            ps.setLong(1, testId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
    private List<Long> getRunIds(Connection conn, long testId, int limit,int offset) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM run WHERE testid = ? AND trashed = false ORDER BY id DESC LIMIT ? OFFSET ?")) {
            ps.setLong(1, testId);
            ps.setInt(2, limit);
            ps.setInt(3, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) ids.add(rs.getLong(1));
            }
        }
        return ids;
    }
}
