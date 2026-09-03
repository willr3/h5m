package io.hyperfoil.tools.h5m.cli;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.api.*;
import io.hyperfoil.tools.h5m.svc.*;
import io.hyperfoil.tools.jjq.value.*;
import io.hyperfoil.tools.yaup.Sets;
import io.hyperfoil.tools.yaup.json.Json;
import io.hyperfoil.tools.yaup.json.JsonComparison;
import jakarta.inject.Inject;
import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.aesh.readline.prompt.Prompt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@CommandDefinition(name="veritaserum",description = "find the truth", generateHelp = true)
public class Veritaserum implements Command<H5mCommandInvocation> {

    private static final Logger log = LoggerFactory.getLogger(Veritaserum.class);
    @Inject
    FolderService folderService;

    @Inject
    NodeService nodeService;

    @Inject
    ValueService valueService;

    @Inject
    NodeGroupService nodeGroupService;

    @Inject
    ProcessingService processingService;

    @Option(name = "username", description = "legacy db username", defaultValue = "quarkus", acceptNameWithoutDashes = true)
    String username;
    @Option(name = "password", description = "legacy db password", defaultValue = "quarkus", acceptNameWithoutDashes = true)
    String password;
    @Option(name = "url", description = "legacy connection url", defaultValue = "jdbc:postgresql://127.0.0.1:6000/horreum", acceptNameWithoutDashes = true)
    String url;
    @OptionList(name = "testId", description = "Horreum test ID")
    List<Long> testIds;
    @OptionList(name = "runId", description = "verify a specific run (optional)")
    List<Long> runIds;
    @Option(name = "limit", description = "max runs to verify", defaultValue = "5", acceptNameWithoutDashes = true)
    int limit;
    @Option(name = "offset", description = "max runs to verify", defaultValue = "0", acceptNameWithoutDashes = true)
    int offset;

    @Option(name = "pause", acceptNameWithoutDashes = true, description = "pause for user input after every batch", hasValue = false, defaultValue = "false")
    boolean pause;

    //controls for equality
    @Option(name = "ignore-nulls", description = "remove nulls from h5m arrays before comparing to Horreum",defaultValue = "false", acceptNameWithoutDashes = true)
    boolean ignoreNulls;

    @Option(name = "extractor-prefix", acceptNameWithoutDashes = true, description = "optional prefix for all extractor names", defaultValue = "")
    String extractorPrefix = ""; //setting value for @Inject testing in LoadLegacyTestsTest

    @Option(name = "combined-label-prefix", acceptNameWithoutDashes = true, description = "optional prefix for labels combined during schema merge",defaultValue = "")
    String combinedLabelPrefix = "";


    @Inject
    LoadLegacyTests loadLegacyTests;

    private JqValue purgeNulls(JqValue input){
        if(input == null || input.isNull()){
            return JqNull.NULL;
        }
        if(ignoreNulls){
            if(input.isObject()) {
                JqObject.Builder builder = JqObject.builder();
                JqObject obj = (JqObject) input;
                obj.forEach((k,v)->{
                    if(v!=null && !v.isNull()){
                        builder.put(k,v);
                    }
                });
                input = builder.build();
            }else if (input.isArray()){
                JqArray.ArrayBuilder builder = JqArray.arrayBuilder();
                JqArray ary = (JqArray)  input;
                ary.forEach(v->{
                    if(v!=null && !v.isNull()){
                        builder.add(v);
                    }
                });
                input = builder.build();
            }
        }
        return input;
    }

    @Override
    public CommandResult execute(H5mCommandInvocation invocation) throws InterruptedException {
        CommandResult exitCode = CommandResult.SUCCESS;
        try {
            List<Delta> deltas = new ArrayList<>();


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


            try (Connection legacyConn = legacyDs.getConnection()) {

                if (testIds == null || testIds.isEmpty()) {
                    //fetch all the tests
                    testIds = fetchTestIds(legacyConn);
                }
                //for each test
                for (Long testId : testIds) {


                    String testName = getTestName(legacyConn, testId);
                    if (testName == null) {
                        System.err.println("Test not found: " + testId);
                        exitCode = CommandResult.USAGE_ERROR;
                        continue;
                    }
                    System.out.println("Veritaserum "+testName+" id="+testId);
                    folder = folderService.find(testName);
                    boolean loadFolder = folder == null;
                    if (loadFolder) {
                        System.out.println("loading test " + testName + " id=" + testId);
                        loadLegacyTests.username = username;
                        loadLegacyTests.password = password;
                        loadLegacyTests.url = url;
                        loadLegacyTests.testId = testId;
                        loadLegacyTests.keepAll = true;
                        loadLegacyTests.combinedLabelPrefix=combinedLabelPrefix;
                        loadLegacyTests.extractorPrefix=extractorPrefix;
                        //int ec = loadLegacyTests.call();
                        CommandResult result = loadLegacyTests.execute(null);
                        if (result.getExitCode() != 0) {
                            System.out.println("Error loading test " + testName + " id=" + testId);
                            exitCode = CommandResult.FAILURE;
                            continue;
                        }
                        folder = folderService.find(testName);
                    }
                    if (folder == null) {
                        System.out.println("failed to find folder " + testName);
                        exitCode = CommandResult.USAGE_ERROR;
                        continue;
                    }
                    NodeGroup nodeGroup = nodeGroupService.byId(folder.groupId());

                    //for each run
                    if (runIds == null || runIds.isEmpty()) {
                        runIds = fetchRunIds(legacyConn, testId, limit, offset);
                    }
                    System.out.println(runIds.size() + " runs");
                    for (Long runId : runIds) {
                        JqValue runData = fetchRun(legacyConn, runId);


                        if (runData == null) {
                            System.out.println("failed to find run " + runId);
                            exitCode = CommandResult.USAGE_ERROR;
                            continue;
                        }
                        System.out.println("Uploading run " + runId);
                        long uploadId = valueService.createRootValue(folder.id(),runData);
                        boolean ok = processingService.awaitIngestion(uploadId,3,TimeUnit.MINUTES);
                        if (!ok) {
                            System.out.println("upload failed");
                            continue;
                        }

                        List<LoadLegacyTests.Transformer> transformers = loadTransformers(legacyConn, testId);
                        if (!transformers.isEmpty()) {
                            for (LoadLegacyTests.Transformer transformer : transformers) {
                                System.out.println("\nTransformer: " + transformer.name());
                                List<Node> transformerMatches = nodeService.findNodeByFqdn(transformer.name(), folder.groupId());
                                if (transformerMatches.isEmpty()) {
                                    String name = LoadLegacyTests.getRename(transformer, transformers.size());
                                    transformerMatches = nodeService.findNodeByFqdn(name, folder.groupId());
                                }
                                if (transformerMatches.isEmpty()) {
                                    System.out.println("failed to find match for transformer " + transformer.name());
                                    exitCode = CommandResult.FAILURE;
                                    continue;
                                }
                                Node transformerMatch = transformerMatches.get(0);
                                for (Node extractorNode : transformerMatch.sources()) {

                                    var matchingExtractor = transformer.extractors().stream().filter(e -> extractorNode.name().equals(loadLegacyTests.getExtractorRename( e.name() )) || extractorNode.name().equals(LoadLegacyTests.DEFAULT_PREFIX+loadLegacyTests.getExtractorRename( e.name() ))).findFirst().orElse(null);
                                    if (matchingExtractor == null) {
                                        System.out.println("failed to find match for transformer extractor " + extractorNode.name());
                                        exitCode = CommandResult.FAILURE;
                                        continue;
                                    }
                                    JqValue fromExtractor = extractRun(legacyConn, runId, matchingExtractor.jsonpath(), matchingExtractor.isArray());
                                    List<Value> h5mValues = valueService.getDescendantValues(uploadId, List.of(extractorNode.id()));
                                    JqValue fromH5m = h5mValues.isEmpty() ? null : purgeNulls(h5mValues.getFirst().data());
                                    boolean eq = equalish(fromExtractor, fromH5m);
                                    if (!eq) {
                                        Delta d = new Delta(
                                                folder.name(),
                                                extractorNode.id(),
                                                extractorNode.name(),
                                                "transformer_extractors",
                                                matchingExtractor.name(),
                                                transformer.id(),
                                                "run",
                                                runId,
                                                uploadId,
                                                matchingExtractor.jsonpath(),
                                                fromExtractor,
                                                extractorNode.operation(),
                                                fromH5m
                                        );
                                        deltas.add(d);
                                    }
                                }
                            }
                        }

                        List<LoadLegacyTests.Label> usedLabels = fetchRunLabels(legacyConn, runId);

                        List<Long> datasetIds = getDatasetIds(legacyConn, runId);
                        Node datasetNode = null;
                        List<Value> datasetValues = new ArrayList<>();
                        if(!transformers.isEmpty()){
                            List<Node> datasetNodes = nodeService.findNodeByFqdn("dataset", folder.groupId());
                            if (datasetNodes.isEmpty()) {
                                System.out.println("Cannot find dataset node for " + folder.name() + " groupId=" + folder.groupId());
                                exitCode = CommandResult.FAILURE;
                                continue;
                            }
                            datasetNode = datasetNodes.get(0);
                            datasetValues.addAll(valueService.getDescendantValues(uploadId,List.of(datasetNode.id())));
                        }else{
                            datasetNode = nodeGroup.root();
                            datasetValues.add(valueService.get(uploadId));
                        }

                        if (datasetIds.size() != datasetValues.size()) {
                            System.out.println("INCORRECT NUMBER OF DATASETS h5m=" + datasetValues.size() + " horreum=" + datasetIds.size());
                            //TODO do we stop doing this because now we have comparison?
//                        Delta d = new Delta(
//                                folder.name(),
//                                datasetNode.id(),
//                                "dataset",
//                                "-",
//                                transformers.stream().map(t->t.name()+"="+t.id()).collect(Collectors.joining(",")),
//                                -1,
//                                "run",
//                                runId,
//                                -1,
//                                "",
//                                JqNumber.of(datasetIds.size()),
//                                datasetNode.operation(),
//                                JqNumber.of(datasetValues.size())
//                        );
//                        deltas.add(d);
                        }
                        //find the best match between datasets
                        List<DatasetValuePair> pairs = matchDatasetToValue(legacyConn, datasetIds, datasetValues);

                        //compare datasets
                        for (int i = 0; i < pairs.size(); i++) {
                            DatasetValuePair pair = pairs.get(i);
                            long horreumDatasetId = pair.datasetId;
                            long h5mDatasetId = pair.valueId;
                            if (horreumDatasetId == -1 || h5mDatasetId == -1) {
                                Delta d = new Delta(
                                        testName,
                                        datasetNode.id(),
                                        datasetNode.name(),
                                        "-",
                                        transformers.stream().map(t -> t.name() + "=" + t.id()).collect(Collectors.joining(",")),
                                        -1,
                                        "run",
                                        horreumDatasetId,
                                        h5mDatasetId,
                                        "",
                                        horreumDatasetId == -1 ? JqNull.NULL : JqObject.EMPTY,
                                        datasetNode.operation(),
                                        h5mDatasetId == -1 ? JqNull.NULL : JqObject.EMPTY
                                );
                                deltas.add(d);
                                continue;
                            }
                            System.out.println("Dataset: " + horreumDatasetId + " value: " + h5mDatasetId + " label_values " + getLabelValueCount(legacyConn, horreumDatasetId) + "\n");

                            for (LoadLegacyTests.Label label : usedLabels) {
                                JqValue horreumLabelValue = getLabelValue(legacyConn,horreumDatasetId,label.id());

                                List<Node> matchingNodes = nodeService.findNodeByFqdn(label.name(), folder.groupId());
                                if(matchingNodes.size()>1){
                                    System.out.println("too many matching nodes for "+label.name()+"\n"+matchingNodes.stream().map(n->n.name()+"="+n.id()).collect(Collectors.joining(", ")));
                                }
                                if (matchingNodes.isEmpty()) {
                                    System.out.println("ERROR: failed to find match for label " + label.name());
                                    exitCode = CommandResult.FAILURE;
                                    Delta d = new Delta(
                                            folder.name(),
                                            -1,
                                            "-",
                                            "label",
                                            label.name(),
                                            label.id(),
                                            "dataset",
                                            horreumDatasetId,
                                            h5mDatasetId,
                                            label.function(),
                                            horreumLabelValue,
                                            "-",
                                            JqNull.NULL
                                    );
                                    deltas.add(d);
                                    continue;
                                }
                                for (Node matchingNode : matchingNodes) {
                                    List<Value> matchingNodeValues = valueService.getDescendantValues(h5mDatasetId,List.of(matchingNode.id()));
                                    if(matchingNodeValues.isEmpty()){
                                        //this means h5m failed to calculate a value
                                    }else if (matchingNodeValues.size() > 1){
                                        //this means h5m calculated too many values
                                    }else{//this is just right
                                        boolean labelValueEq = equalish(horreumLabelValue,matchingNodeValues.getFirst().data());
                                        if(!labelValueEq){
                                            //investigate why the value is different
                                            if(matchingNode.type().equals(NodeType.JS)){
                                                for (Node node : matchingNode.sources()) {
                                                    var matchingExtractor = label.extractors().stream().filter(e -> equalish(loadLegacyTests.getExtractorRename( e.name() ), node.name())).findFirst().orElse(null);
                                                    if (matchingExtractor == null) {
                                                        if (label.extractors().size() == 1) {
                                                            matchingExtractor = label.extractors().iterator().next();
                                                        } else {
                                                            System.out.println("failed to find match for label extractor \"" + node.name() + "\" (" + nameSanitize(node.name()) + ") from: " + label.extractors().stream().map(e -> e.name() + "=(" + nameSanitize(e.name()) + ")").collect(Collectors.joining(", ")));
                                                            exitCode = CommandResult.FAILURE;
                                                            continue;
                                                        }
                                                    }
                                                    JqValue fromExtractor = extractDataset(legacyConn, horreumDatasetId, matchingExtractor.jsonpath(), matchingExtractor.isArray());
                                                    List<Value> h5mValues = valueService.getDescendantValues(h5mDatasetId, List.of(node.id()));
                                                    JqValue fromH5m = h5mValues.isEmpty() ? null : purgeNulls( h5mValues.getFirst().data() );
                                                    boolean eq = equalish(fromExtractor, fromH5m);
                                                    if (!eq) {
                                                        Delta d = new Delta(
                                                                folder.name(),
                                                                node.id(),
                                                                node.name(),
                                                                "label_extractors",
                                                                matchingExtractor.name(),
                                                                label.id(),
                                                                "dataset",
                                                                horreumDatasetId,
                                                                h5mDatasetId,
                                                                matchingExtractor.jsonpath(),
                                                                fromExtractor,
                                                                node.operation(),
                                                                fromH5m
                                                        );
                                                        deltas.add(d);
                                                    }
                                                }
                                            }else{
                                                Delta d = new Delta(
                                                        folder.name(),
                                                        matchingNode.id(),
                                                        matchingNode.name(),
                                                        "label",
                                                        label.name(),
                                                        label.id(),
                                                        "dataset",
                                                        horreumDatasetId,
                                                        h5mDatasetId,
                                                        label.function(),
                                                        horreumLabelValue,
                                                        matchingNode.operation(),
                                                        matchingNodeValues.getFirst().data()
                                                );
                                                deltas.add(d);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }// for runId
                    if (pause) {
                        System.out.println(deltas.size() + " DELTAS");
                        for (Delta d : deltas) {
                            System.out.println(d);
                        }
                        invocation.getShell().readLine(new Prompt("Press Enter to continue..."));
                        deltas.clear();
                    }
                    runIds.clear(); //clear for next loop
                }// for testid
            }
            System.out.println(deltas.size() + " DELTAS");
            for (Delta d : deltas) {
                System.out.println(d);
            }
        }catch(SQLException e){
            exitCode = CommandResult.FAILURE;
        }
        return exitCode;
    }
    record Delta(String folderName, long nodeId, String nodeName, String horreumTable, String horreumName, long parentId, String dataTable, long dataId, long valueId, String jsonpath, JqValue horreum, String jq, JqValue h5m){

        @Override
        public String toString() {
            StringBuilder extra = new StringBuilder();
            if(horreum !=null && h5m!=null && ((horreum.toString().length()>=120) || (h5m.toString().length()>=120)) ){
                Json horreumJson = horreum == null ? new Json() : Json.fromString(horreum.toString());
                Json h5mJson = h5m == null ? new Json() : Json.fromString(h5m.toString());
                JsonComparison comp = new JsonComparison();
                if(horreumJson != null && h5mJson != null){
                    comp.load("hrm", horreumJson);
                    comp.load("h5m", h5mJson);
                    comp.getDiffs().forEach(d -> {
                        extra.append(d.getPath());
                        d.forEach((k, v) -> {
                            extra.append(System.lineSeparator());
                            extra.append(k);
                            extra.append(" : ");
                            extra.append(v);

                        });
                        extra.append(System.lineSeparator());
                    });
                }
            }
            return folderName+" "+dataTable+"="+dataId+" "+ horreumTable +" label/transform="+parentId+" valueId="+valueId+"\n"
                    +"  hrm: "+ horreumName +"\n"
                    +"    filter: "+jsonpath+"\n"
                    +"    value: "+(horreum==null ? "null" : (horreum.toString().length()<120)?horreum.toString():("length="+horreum.toString().length()))+"\n"
                    +"  h5m: "+nodeName+"\n"
                    +"    filter: "+jq+"\n"
                    +"    value: "+(h5m == null ? "null" : (h5m.toString().length()<120)?h5m.toString():("length="+h5m.toString().length()))+"\n"
                    +(extra.length()>0 ?("  diffs:\n    "+extra.toString().replaceAll("\n","\n    ")):"");
        }
    }
    record DatasetValuePair(long datasetId,long valueId){}
    private List<DatasetValuePair> matchDatasetToValue(Connection conn, List<Long> datasetIds,List<Value> values) throws SQLException {
        List<DatasetValuePair> pairs = new ArrayList<>();
        try {

            List<JqObject> datasetLabelValues = new ArrayList<>();
            for (Long id : datasetIds) {
                JqObject jqObject = fetchDatasetLabelValues(conn, id);
                datasetLabelValues.add(jqObject);
            }
            List<JqObject> valueLabelValues = new ArrayList<>();
            for (Value v : values) {
                List<JqObject> jqObjects = valueService.getGroupedValues(
                        v.node().id(),
                        v.id(),
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_MAP,
                        null
                );
                if (jqObjects.isEmpty()) {
                    //this shouldn't happen, what do we do?
                    System.out.println("Error: value " + v.id() + " from node=" + v.node().name() + "=" + v.node().id() + " is missing grouped values");
                } else {
                    valueLabelValues.add(jqObjects.getFirst());
                }
            }

            Set<String> datasetKeys = datasetLabelValues.stream().flatMap(o -> o.keys().stream()).collect(Collectors.toSet());
            double scores[][] = new double[datasetLabelValues.size()][valueLabelValues.size()];
            for (int d = 0; d < datasetLabelValues.size(); d++) {
                for (int v = 0; v < valueLabelValues.size(); v++) {
                    scores[d][v] = score(datasetLabelValues.get(d), valueLabelValues.get(v));
                }
            }
            //pick the best matches
            Set<Integer> remainingValueIndexes = new HashSet<>(IntStream.rangeClosed(0, valueLabelValues.size() - 1).boxed().toList());

            for (int d = 0; d < datasetLabelValues.size(); d++) {
                int bestIndex = -1;
                double bestScore = -1;
                for (int remainingIndex : remainingValueIndexes) {
                    if (scores[d][remainingIndex] > bestScore) {
                        bestScore = scores[d][remainingIndex];
                        bestIndex = remainingIndex;
                    }
                }
                if (bestIndex != -1) {
                    remainingValueIndexes.remove(bestIndex);
                    DatasetValuePair dpv = new DatasetValuePair(datasetIds.get(d), values.get(bestIndex).id());
                    pairs.add(dpv);
                }
            }
            if (!remainingValueIndexes.isEmpty()) {
                for (int remainingIndex : remainingValueIndexes) {
                    DatasetValuePair dvp = new DatasetValuePair(-1, values.get(remainingIndex).id());
                    pairs.add(dvp);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return pairs;
    }
    private boolean equalish(JqValue horreum, JqValue h5m){
        return horreum.equals(h5m) || (horreum.isNull() && (h5m == null || h5m.isNull()));
    }
    private static boolean equalish(String a,String b){
        return nameSanitize(a).equalsIgnoreCase(nameSanitize(b));
    }
    private static String nameSanitize(String s){
        return s.replaceAll("[ \\-_]","").toLowerCase();
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



    private int getLabelValueCount(Connection connection, Long datasetId) throws SQLException {
        int rtrn = -1;
        try(PreparedStatement statement = connection.prepareStatement("select count(*) from label_values where dataset_id = ? and value is not null")){
            statement.setLong(1, datasetId);
            try (ResultSet rs = statement.executeQuery()){
                while (rs.next()) {
                    rtrn = rs.getInt(1);
                }
            }
        }
        return rtrn;
    }
    private List<LoadLegacyTests.Label> fetchRunLabels(Connection connection, Long runId) throws SQLException {
        List<LoadLegacyTests.Label> labels = new ArrayList<>();
        List<LoadLegacyTests.LabelDef> labelDefs = new ArrayList<>();
        try(PreparedStatement statement = connection.prepareStatement("select distinct l.id,l.name,l.function from label l where exists (select 1 from label_values v where v.label_id = l.id and v.dataset_id in (select id from dataset where runid = ?))")){
            statement.setLong(1, runId);
            try (ResultSet rs = statement.executeQuery()){
                while (rs.next()) {
                    labelDefs.add(new LoadLegacyTests.LabelDef(rs.getLong(1),rs.getString(2),rs.getString(3)));
                }
            }
        }
        for(LoadLegacyTests.LabelDef labelDef : labelDefs){
            try(PreparedStatement statement = connection.prepareStatement("select name,jsonpath,isarray from label_extractors where label_id = ?")){
                statement.setLong(1,labelDef.id());
                List<LoadLegacyTests.Extractor> labelExtractors = new ArrayList<>();
                try(ResultSet rs = statement.executeQuery()){
                    while(rs.next()){
                        labelExtractors.add(new LoadLegacyTests.Extractor(rs.getString(1),rs.getString(2),rs.getBoolean(3)));
                    }
                }
                LoadLegacyTests.Label label = new LoadLegacyTests.Label(labelDef.id(),labelDef.name().replaceAll(":","_"),labelDef.function(),labelExtractors);
                labels.add(label);
            }
        }
        return labels;
    }
    private List<Long> fetchTestIds(Connection connection) throws SQLException {
        List<Long> testIds = new ArrayList<>();
        try(PreparedStatement statement = connection.prepareStatement("select id from test order by id")){
            try (ResultSet rs = statement.executeQuery()){
                while (rs.next()) {
                    testIds.add(rs.getLong(1));
                }
            }
        }
        return testIds;
    }
    private List<Long> fetchRunIds(Connection connection, Long testId,int limit, int offset) throws SQLException {
        List<Long> runIds = new ArrayList<>();
        try(PreparedStatement statement = connection.prepareStatement("select id from run where testid = ? and trashed = false order by id asc limit ? offset ?")){
            statement.setLong(1, testId);
            statement.setInt(2, limit);
            statement.setInt(3, offset);
            try (ResultSet rs = statement.executeQuery()){
                while (rs.next()) {
                    runIds.add(rs.getLong(1));
                }
            }
        }
        return runIds;
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
    private int compareLabelValues(AgroalDataSource legacyDs,long testId,long runId) throws SQLException {
        JqValue runLabelValues = null;
        Folder folder = null;
        try (Connection legacyConn = legacyDs.getConnection()) {
            String testName = getTestName(legacyConn,testId);
            if (testName == null) {
                System.err.println("Test not found: " + testId);
                return 1;
            }
            folder = folderService.find(testName);
            if (folder == null) {
                System.out.println("failed to find folder "+testName);
                return 1;
            }
            System.out.println("Verifying test: " + testName + " (id=" + testId + ")");

            runLabelValues = fetchRunLabelValues(legacyConn,runId);
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

    private JqObject fetchDatasetLabelValues(Connection conn, long datasetId) {
        JqObject value = null;
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
                    WHERE dataset.id = ?
                ), entry as (
                    SELECT
                        runId,
                        datasetId,
                        jsonb_object_agg(labelName,value) as data
                    from
                        combined
                    where labelName is not null
                    group by runId,datasetId
                )
                select
                    jsonb_agg(
                        data -- jsonb_build_object('runId',runId,'datasetId',datasetId,'data',data)
                    ) from entry
            """
        )) {
            ps.setLong(1, datasetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (value != null) {
                        //This should not happen
                    }
                    String str = rs.getString(1);
                    if(str!=null && !str.isEmpty()){
                        JqArray array = (JqArray) JqValues.parse(rs.getString(1));
                        if(array.isEmpty()){
                            //log this error?
                        }else {
                            value = (JqObject) array.get(0);
                        }
                    }
                }
            }
        } catch (SQLException e){
            System.out.println("datasetId = "+datasetId);
            e.printStackTrace();
        }
        return value == null ? JqObject.EMPTY : value;
    }
    private JqArray fetchRunLabelValues(Connection conn, long runId){
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
    private static JqValue getLabelValue(Connection conn, long datasetId,long labelId) throws SQLException {
        JqValue value = null;
        try(PreparedStatement ps = conn.prepareStatement(
                """
                select value from label_values where dataset_id = ? and label_id = ?
                """
        )){
            ps.setLong(1, datasetId);
            ps.setLong(2, labelId);
            try(ResultSet rs = ps.executeQuery()){
                while(rs.next()){
                    String response = rs.getString(1);
                    if(response!=null && !response.isEmpty()){
                        value = JqValues.parse(response);
                    }
                }
            }
        }
        return value;
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
                        //
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
