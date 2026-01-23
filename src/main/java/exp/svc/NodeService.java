package exp.svc;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.EvaluateRuntimeException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import exp.entity.Node;
import exp.entity.Value;
import exp.entity.node.*;
import exp.pasted.ProxyJacksonArray;
import exp.pasted.ProxyJacksonObject;
import io.hyperfoil.tools.yaup.hash.HashFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.proxy.Proxy;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.hibernate.Session;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class NodeService {

    @Inject
    EntityManager em;

    @Inject
    ValueService valueService;

    @ConfigProperty(name="quarkus.datasource.db-kind")
    String dbKind;


    @Transactional
    public Node create(Node node){

        if(!node.isPersistent()){
            node.id = null;
            Node merged = em.merge(node);
            em.flush();
            node.id = merged.id;
            return merged;
        }
        return node;
    }

    @Transactional
    public Node read(long id){
        return Node.findById(id);
    }

    @Transactional
    public long update(Node node){
        if(node.id == null || node.id == -1){

        }else{
            em.persist(node);
        }
        return node.id;
    }

    @Transactional
    public List<Node> getDependentNodes(Node n){
        List<Node> rtrn = Node.list("SELECT DISTINCT n FROM Node n JOIN n.sources s WHERE s.id = ?1",n.id);
        rtrn.forEach(r->r.hashCode());//lazy hack
        return rtrn;
    }


    @Transactional
    public void delete(Node node){
        if(node.id!=null) {
            //remove nodes that depend on this or just remove the reference?
            getDependentNodes(node).forEach(this::delete);
            Node.deleteById(node.id);
        }
    }

    /**
     * This creates the source value combinations for nodes with source nodes that create multiple values (e.g. datasets).
     * If there are multiple source nodes with multiple values then combinations are either by index (Length) or a full matrix of combinations (NxN)
     * depending on the current node's multiType.
     * @param node
     * @param root
     * @return
     */
    public List<Map<String,Value>> calculateSourceValuePermutations(Node node, Value root) {
        List<Map<String,Value>> rtrn = new ArrayList<>();
        Map<String,List<Value>> nodeValues = node.sources.stream()
                .collect(Collectors.toMap(n->n.name, n -> {
                    List<Value> found = valueService.getDescendantValues(root, n);
                    if (found.isEmpty() && n.sources.isEmpty()) {
                        found = List.of(root);
                    }
                    return found;
                }
                ));

        int maxNodeValuesLength = nodeValues.values().stream().map(Collection::size).max(Integer::compareTo).orElse(0);

        //the two cases where we do not need to worry about MultiIterationType
        if(maxNodeValuesLength == 1 || node.sources.size() == 1){//if we don't need to worry about NxN or byLength
            //to ensure sequence
            for(int i=0; i< maxNodeValuesLength; i++) {
                int idx = i;
                Map<String,Value> sourceValuesAtIndex = node.sources.stream()
                        .filter(n->nodeValues.get(n.name).size()>idx)
                        .collect(Collectors.toMap(n->n.name,n->nodeValues.get(n.name).get(idx)));
                //TODO splitting?
                rtrn.add(sourceValuesAtIndex);
            }
        } else { //NxN or byLength time
            switch (node.multiType){
                case Length -> {
                    //I think this is functionally equivalent
                    for(int i=0; i< maxNodeValuesLength; i++){
                        Map<String,Value> sourceValuesAtIndex = new HashMap<>();
                        int idx = i;//thanks java
                        for(Node n : node.sources){
                            List<Value> nValues = nodeValues.get(n.name);
                            if(nValues.size()==1){
                                if(idx == 0){
                                    sourceValuesAtIndex.put(n.name,nValues.get(idx));
                                }else if (n.scalarMethod.equals(Node.ScalarVariableMethod.All)){
                                    sourceValuesAtIndex.put(n.name,nValues.getFirst());
                                }
                            }else if(nValues.size()>idx){
                                sourceValuesAtIndex.put(n.name,nValues.get(idx));
                            }else{
                                return null;
                            }
                        }
                        rtrn.add(sourceValuesAtIndex);
                    }
                }
                case NxN -> {
                    List<Map<String,Value>> valuePermutations = new ArrayList<>();
                    List<String> multiNodes = nodeValues.entrySet().stream().filter(e->e.getValue().size()>1).map(Map.Entry::getKey).toList();
                    List<String> scalarNodes = nodeValues.entrySet().stream().filter(e->e.getValue().size()==1).map(Map.Entry::getKey).toList();
                    int permutations = nodeValues.values().stream().map(List::size).reduce(1,(a,b)-> b > 0 ? a*b : a );
                    for( int i=0; i<permutations; i++ ) {
                        valuePermutations.add(new HashMap<>());
                    }
                    int loopCount = 1;
                    for( Node sourceNode : node.sources ){
                        List<Value> valueList = nodeValues.get(sourceNode.name);
                        if( !valueList.isEmpty() ){
                            if ( valueList.size() == 1 ) {
                                if ( sourceNode.scalarMethod.equals(Node.ScalarVariableMethod.First) ){
                                    valuePermutations.getFirst().put(sourceNode.name, valueList.getFirst());
                                }else{
                                    valuePermutations.forEach(m->m.put(sourceNode.name, valueList.getFirst()));
                                }
                            } else {//just the multivalue entries
                                int valueCount = valueList.size();
                                int perLoop = permutations / loopCount;
                                int perValue = perLoop / valueCount;

                                for(int loopIndex=0; loopIndex<loopCount; loopIndex++) {
                                    for (int valueIndex = 0; valueIndex < valueList.size(); valueIndex++) {
                                        for (int i = 0; i < perValue; i++) {
                                            int permutationIndex = loopIndex * perLoop + valueIndex * perValue + i;
                                            valuePermutations.get(permutationIndex).put(sourceNode.name, valueList.get(valueIndex));
                                        }
                                    }
                                }
                                loopCount*=valueCount;
                            }
                        }
                    }
                    rtrn.addAll(valuePermutations);
                }
            }
        }
        return rtrn;
    }


    /**
     *
     * @param node
     * @param roots
     * @return
     * @throws IOException
     */
    @Transactional
    public List<Value> calculateValues(Node node, List<Value> roots) throws IOException {
        List<Value> rtrn = new ArrayList<>();
        switch (node.type){
            //nodes that operate one root at a time
            case "ecma":
            case "jq":
            case "nata":
            case "sql":
            case "sqlall":
            case "split":
                for(int vIdx=0; vIdx<roots.size(); vIdx++){
                    Value root =  roots.get(vIdx);
                    try {
                        List<Map<String,Value>> combinations = calculateSourceValuePermutations(node,root);
                        for(int i=0;i<combinations.size();i++){
                            Map<String,Value> combination =  combinations.get(i);
                            List<Value> createdValues = calculateNodeValues(node,combination,rtrn.size());
                            rtrn.addAll(createdValues);
                        }
                    } catch (IOException e) {
                    }
                }
                break;
            default:
                System.err.println("Unknown node type: " + node.type);
        }
        rtrn.forEach(Value::getPath);//forcing entities to be loaded is so dirty
        return rtrn;
    }

    public List<Value> calculateNodeValues(Node node,Map<String,Value> sourceValues,int startingOrdinal) throws IOException {
        return switch(node.type){
            case "jq" -> calculateJqValues((JqNode)node,sourceValues,startingOrdinal+1);
            case "ecma" -> calculateJsValues((JsNode)node,sourceValues,startingOrdinal+1);
            case "nata" -> calculateJsonataValues((JsonataNode)node,sourceValues,startingOrdinal+1);
            case "sql" -> calculateSqlJsonpathValues((SqlJsonpathNode)node,sourceValues,startingOrdinal+1);
            case "sqlall" -> calculateSqlAllJsonpathValues((SqlJsonpathAllNode)node, sourceValues, startingOrdinal+1);
            case "split" -> calculateSplitValues((SplitNode)node,sourceValues,startingOrdinal+1);
            default -> {
                System.err.println("Unknown node type: "+node.type);
                yield Collections.emptyList();
            }
        };
    }

    @Transactional
    public List<Value> calculateSqlJsonpathValues(SqlJsonpathNode node, Map<String,Value> sourceValues, int startingOrdinal) throws IOException {
        return calculateSqlJsonpathValuesFirstOrAll(node,sourceValues,startingOrdinal,"jsonb_path_query_first");
    }
    @Transactional
    public List<Value> calculateSqlAllJsonpathValues(SqlJsonpathAllNode node, Map<String,Value> sourceValues, int startingOrdinal) throws IOException {
        return calculateSqlJsonpathValuesFirstOrAll(node,sourceValues,startingOrdinal,"jsonb_path_query_array");
    }
    private List<Value> calculateSqlJsonpathValuesFirstOrAll(Node node, Map<String,Value> sourceValues, int startingOrdinal,String psqlFunction) throws IOException {
        List<Value> rtrn = new ArrayList<>();
        if(sourceValues.isEmpty()){//end early when there isn't input
            return rtrn;
        }

        if(sourceValues.size()>1 || node.sources.size()>1){
            System.err.println("sql jsonpath only supports one input at a time");
            return Collections.emptyList();
        }

        Value input = sourceValues.get(node.sources.getFirst().name);

        Value tempV = new Value(null,node,null);
        tempV.sources=List.of(input);
        tempV.idx=startingOrdinal;
        Value newValue = valueService.create(tempV);
        Session session = em.unwrap(Session.class);
        session.doWork(conn -> {
            try(PreparedStatement statement = conn.prepareStatement(
                    switch(dbKind) {
                        case "sqlite" ->
                                """
                                update value set data = ( select data from value where id = ?) -> ? where id = ?
                                """;
                        case "postgresql" ->
                                """
                                update value set data = PSQL_FUNCTION( (select data from value where id = ?) , ?::jsonpath ) where id = ?
                                """.replaceAll("PSQL_FUNCTION",psqlFunction);
                        default -> "";
                    }
            )) {
                statement.setLong(1,input.id);
                statement.setString(2,node.operation);
                statement.setLong(3,newValue.id);
                statement.execute();
            }catch (Exception e){
                System.err.println(e.getMessage());
            }

            Value.<Value>find("data is not null and id = ?1", newValue.id)
                 .project(Value.DataProjection.class)
                 .firstResultOptional()

                 .ifPresentOrElse(projection -> newValue.data = projection.data(),()->{
                     System.err.println("no data found for newValue.id="+newValue.id+" node="+newValue.node.id+" "+newValue.node.name);
                 });
            rtrn.add(newValue);
        });
        return rtrn;
    }
    @Transactional
    public List<Value> calculateSplitValues(SplitNode node, Map<String,Value> sourceValues, int startingOrdinal) throws IOException {
        List<Value> rtrn = new ArrayList<>();

        if(sourceValues.isEmpty()){
            return rtrn;
        }
        if(sourceValues.size()>1 || node.sources.size()>1){
            System.err.println("split only supports one input node at a time");
        }
        //this will naively do the split with entities but using json_each would be good
        //TODO should this be done in db with json_each?
        Value v = sourceValues.get(node.sources.getFirst().name);
        if(v!=null){
            if(v.data.isArray()){
                ArrayNode arrayNode = (ArrayNode) v.data;
                for(int i=0;i<arrayNode.size();i++){
                    JsonNode entry = arrayNode.get(i);
                    Value newValue = new Value(null,node,entry);
                    newValue.idx=i;
                    newValue.sources = List.of(v);
                    rtrn.add(newValue);
                }
            }else{
                Value newValue = new Value(null,node,v.data);
                newValue.idx=0;
                newValue.sources = List.of(v);
                rtrn.add(newValue);
            }
        }
        return rtrn;
    }


    //jsonata cannot operate on multiple inputs at once so source
    public List<Value> calculateJsonataValues(JsonataNode node,Map<String,Value> sourceValues,int startingOrdinal) throws IOException {
        if(sourceValues.size()>1 || node.sources.size()>1){
            System.err.println("jsonata only supports one input at a time");
            return Collections.emptyList();
        }

        Value input = sourceValues.isEmpty() ? null : sourceValues.values().iterator().next();

        List<Value> rtrn = new ArrayList<>();

        try {
            Expressions expr = Expressions.parse(node.operation);
            JsonNode result = expr.evaluate(input.data);

            Value newValue = new Value();
            newValue.idx = startingOrdinal+1;
            newValue.node = node;
            newValue.data = result;
            newValue.sources = node.sources.stream().filter(n->sourceValues.containsKey(n.name)).map(n -> sourceValues.get(n.name)).collect(Collectors.toList());
            return List.of(newValue);

        } catch (ParseException e) {
            System.err.println("failed to parse jsonata expression\n"+e.getLocalizedMessage());
        } catch (EvaluateException | EvaluateRuntimeException e) {
            System.err.println("failed to evaluate jsonata expression\n"+e.getLocalizedMessage());
        }

        return rtrn;
    }
    public List<Value> calculateJsValues(JsNode node,Map<String,Value> sourceValues,int startingOrdinal) throws IOException {
        List<Value> rtrn = new ArrayList<>();
        List<String> params = JsNode.getParameterNames(node.operation);
        if(params == null){
            System.err.println("Error occurred reading parameters from js function\n"+node.operation);
            return Collections.emptyList();
        }
        List<JsonNode> input = JsNode.createParameters(node.operation, sourceValues);
        Object result = null;
        try(Context context = Context.newBuilder("js").engine(Engine.newBuilder("js").option("engine.WarnInterpreterOnly", "false").build())
                .allowExperimentalOptions(true)
                .option("js.foreign-object-prototype", "true")
                .option("js.global-property", "true")
//                .out(out)
//                .err(out)
                .build()){
            context.enter();
            context.getBindings("js").putMember("isInstanceLike", new ProxyJacksonObject.InstanceCheck());
            context.eval("js",
                    """
                    Object.defineProperty(Object,Symbol.hasInstance, {
                      value: function myinstanceof(obj) {
                        return isInstanceLike(obj);
                      }
                    });
                    """);
            StringBuilder jsCode = new StringBuilder();
            for(int i=0; i<input.size(); i++) {
                jsCode.append("const __obj").append(i).append(" = ").append(input.get(i)).append(";").append(System.lineSeparator());
            }
            jsCode.append("const __func").append(" = ").append(node.operation).append(";").append(System.lineSeparator());
            jsCode.append("__func(");
            for(int i=0; i<input.size(); i++) {
                if(i>0) jsCode.append(", ");
                jsCode.append("__obj").append(i);
            }
            jsCode.append(");");
            try{
                org.graalvm.polyglot.Value value = context.eval("js", jsCode);
                List<org.graalvm.polyglot.Value> resolvedValues = resolvePromiseOrGenerator(value);
                for(org.graalvm.polyglot.Value resolvedValue : resolvedValues) {
                    try{
                        result = convert(resolvedValue);
                        JsonNode data = null;
                        if(result==null){
                            //data stays null
                        }else if (result instanceof JsonNode){
                            //TODO do we support splitting an array into multiple Values?
                            data = (JsonNode) result;
                        }else{//scalar
                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                data = mapper.readTree(result.toString());
                            } catch (JsonProcessingException e) {
                                System.err.println("failed to convert "+result+" to a javascript object");
                            }
                        }

                        //File valuePath = JqNode.outputPath().resolve(node.name + "." + (startingOrdinal+1)+".jq").toFile();
                        if(data!=null) {
                            Value newValue = new Value();
                            newValue.idx = startingOrdinal+rtrn.size()+1;
                            newValue.node = node;
                            newValue.data = data;
                            newValue.sources = node.sources.stream().filter(n->sourceValues.containsKey(n.name)).map(n -> sourceValues.get(n.name)).collect(Collectors.toList());
                            rtrn.add(newValue);
                        }else{
                            System.err.println("null data from value "+resolvedValue);
                        }
                    }catch (PolyglotException pe){
                        System.err.println("exception jsNode "+node.name+" sourceValues="+sourceValues+"\n"+pe.getMessage());
                    }
                }
            }catch(PolyglotException e){
                System.err.println("exception jsNode "+node.name+" sourceValues="+sourceValues+"\n"+e.getMessage());
            } finally {
                context.leave();
            }
        }
        return rtrn;
    }
    //io.hyperfoil.tools.horreum.exp.data.LabelReducerDao#resolvePromise
    public static List<org.graalvm.polyglot.Value> resolvePromiseOrGenerator(org.graalvm.polyglot.Value value) {
        List<org.graalvm.polyglot.Value> rtrn = new  ArrayList<>();
        if (value.getMetaObject()!=null && value.getMetaObject().getMetaSimpleName().equals("Promise") && value.hasMember("then")
                && value.canInvokeMember("then")) {
            List<org.graalvm.polyglot.Value> resolved = new ArrayList<>();
            List<org.graalvm.polyglot.Value> rejected = new ArrayList<>();
            Object invokeRtrn = value.invokeMember("then", new ProxyExecutable() {
                @Override
                public Object execute(org.graalvm.polyglot.Value... arguments) {
                    resolved.addAll(Arrays.asList(arguments));
                    return arguments;
                }
            }, new ProxyExecutable() {
                @Override
                public Object execute(org.graalvm.polyglot.Value... arguments) {
                    rejected.addAll(Arrays.asList(arguments));
                    return arguments;
                }
            });
            if (!rejected.isEmpty()) {
                value = rejected.get(0);
            } else if (resolved.size() == 1) {
                value = resolved.get(0);
            } else { //resolve.size() > 1, this doesn't happen
                //log.message("resolved promise size="+resolved.size()+", expected 1 for promise = "+value);
            }
        }
        if(value.hasMember("next") && value.canInvokeMember("next")){
            org.graalvm.polyglot.Value target = null;
            List<org.graalvm.polyglot.Value> found = new  ArrayList<>();
            while( (target = value.invokeMember("next"))!=null && !target.getMember("done").asBoolean() ) {
                //target = value.invokeMember("next");
                org.graalvm.polyglot.Value v = target.getMember("value");
                rtrn.add(v);
            }
        }else{
            rtrn.add(value);
        }
        return rtrn;
    }
    //copied from io.hyperfoil.tools.horreum.exp.pasted.ExpUtil#convert but changed to return JsonNode
    public static JsonNode convert(org.graalvm.polyglot.Value value) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        if (value == null) {
            return null;
        } else if (value.isNull()) {
            // Value api cannot differentiate null and undefined from javascript
            if (value.toString().contains("undefined")) {
                return TextNode.valueOf(""); //no return is the same as returning a missing key from a ProxyObject?
            } else {
                return null;
            }
        } else if (value.isProxyObject()) {
            Proxy p = value.asProxyObject();
            if (p instanceof ProxyJacksonArray) {
                return ((ProxyJacksonArray) p).getJsonNode();
            } else if (p instanceof ProxyJacksonObject) {
                return ((ProxyJacksonObject) p).getJsonNode();
            } else {
                //not sure when this would happend
                System.err.println("Unexpected proxy object: "+p);
                return mapper.readTree(p.toString());
            }
        } else if (value.isBoolean()) {
            return BooleanNode.valueOf(value.asBoolean());
        } else if (value.isNumber()) {
            double v = value.asDouble();
            if (v == Math.rint(v)) {
                return LongNode.valueOf((long) v);
            } else {
                return DoubleNode.valueOf(v);
            }
        } else if (value.isString()) {
            return TextNode.valueOf(value.asString());
        } else if (value.hasArrayElements()) {
            return convertArray(value);
        } else if (value.canExecute()) {
            return TextNode.valueOf(value.toString());
        } else if (value.hasMembers()) {
            return convertMapping(value);
        } else {
            //TODO log error wtf is Value?
            return TextNode.valueOf("");
        }
    }
    //io.hyperfoil.tools.horreum.exp.pasted.ExpUtil#convertArray
    public static ArrayNode convertArray(org.graalvm.polyglot.Value value) {
        ArrayNode json = JsonNodeFactory.instance.arrayNode();
        for (int i = 0; i < value.getArraySize(); i++) {
            org.graalvm.polyglot.Value element = value.getArrayElement(i);
            if (element == null || element.isNull()) {
                json.addNull();
            } else if (element.isBoolean()) {
                json.add(element.asBoolean());
            } else if (element.isNumber()) {
                double v = element.asDouble();
                if (v == Math.rint(v)) {
                    json.add(element.asLong());
                } else {
                    json.add(v);
                }
            } else if (element.isString()) {
                json.add(element.asString());
            } else if (element.hasArrayElements()) {
                json.add(convertArray(element));
            } else if (element.hasMembers()) {
                json.add(convertMapping(element));
            } else {
                json.add(element.toString());
            }
        }
        return json;
    }
    //io.hyperfoil.tools.horreum.exp.pasted.ExpUtil#convertMapping
    public static ObjectNode convertMapping(org.graalvm.polyglot.Value value) {
        ObjectNode json = JsonNodeFactory.instance.objectNode();
        for (String key : value.getMemberKeys()) {
            org.graalvm.polyglot.Value element = value.getMember(key);
            if (element == null || element.isNull()) {
                json.set(key, JsonNodeFactory.instance.nullNode());
            } else if (element.isBoolean()) {
                json.set(key, JsonNodeFactory.instance.booleanNode(element.asBoolean()));
            } else if (element.isNumber()) {
                double v = element.asDouble();
                if (v == Math.rint(v)) {
                    json.set(key, JsonNodeFactory.instance.numberNode(element.asLong()));
                } else {
                    json.set(key, JsonNodeFactory.instance.numberNode(v));
                }
            } else if (element.isString()) {
                json.set(key, JsonNodeFactory.instance.textNode(element.asString()));
            } else if (element.hasArrayElements()) {
                json.set(key, convertArray(element));
            } else if (element.hasMembers()) {
                json.set(key, convertMapping(element));
            } else {
                json.set(key, JsonNodeFactory.instance.textNode(element.toString()));
            }
        }
        return json;
    }



    public List<Value> calculateFpValues(FingerprintNode node, Map<String,Value> sourceValues, int startingOrdinal) throws IOException {
        HashFactory hashFactory = new HashFactory();

        String glob = node.sources.stream().map(source->sourceValues.containsKey(source.name) ? sourceValues.get(source.name).data.toString() : "").collect(Collectors.joining(""));
        String hash = hashFactory.getStringHash(glob);
        Value newValue = new Value();
        newValue.idx = startingOrdinal+1;
        newValue.node = node;
        newValue.data = new TextNode(hash);
        newValue.sources = node.sources.stream().filter(n->sourceValues.containsKey(n.name)).map(n -> sourceValues.get(n.name)).collect(Collectors.toList());
        return List.of(newValue);
    }
    public List<Value> calculateJqValues(JqNode node,Map<String,Value> sourceValues,int startingOrdinal) throws IOException {
        List<Value> rtrn = new ArrayList<>();
        List<String> args = new ArrayList<>();
        List<File> paths = new ArrayList<>();
        File tmpFilter = Files.createTempFile("h5m.jq." + node.name,".txt").toFile();
        tmpFilter.deleteOnExit();
        Files.write(tmpFilter.toPath(),node.operation.getBytes());

        args.addAll(List.of(
                "/usr/bin/jq",
                "--from-file",
                tmpFilter.toPath().toAbsolutePath().toString()
        ));
        if ( JqNode.isNullInput(node.operation)){
            args.add("--null-input");
        }else if(node.sources.size()>1 || sourceValues.size() >1){//if this is a multi file input
            args.add("--slurp");
        }

        args.add("--compact-output");
        args.add("--");//terminate argument processing
        //iterate sources to preserve order
        //another CME while iterating an entity collection, who is mutating these nodes?
        List.copyOf(node.sources).forEach(sourceNode -> {
            if(sourceValues.containsKey(sourceNode.name)){
                try {
                    Value sourceValue = sourceValues.get(sourceNode.name);
                    File f = Files.createTempFile("h5m."+sourceNode.name,".json").toFile();
                    valueService.writeToFile(sourceValue.id,f.getAbsolutePath());
                    paths.add(f);
                    args.add(f.getAbsolutePath());
                }catch(IOException e){
                    System.err.println("failed to create temporary file for "+sourceNode.name+"\n"+e.getMessage());
                }

            }
        });
        //if there are no sources we just add all the inputs together
        if(node.sources.isEmpty()){
            sourceValues.values().forEach(sourceValue -> {
                try {
                    File f = Files.createTempFile("h5m.", ".json").toFile();
                    valueService.writeToFile(sourceValue.id, f.getAbsolutePath());
                    paths.add(f);
                    args.add(f.getAbsolutePath());
                }catch(IOException e){
                    System.err.println("failed to create temporary file\n"+e.getMessage());
                }
            });

        }
        Path destinationPath = Files.createTempFile(".h5m." + node.name+".",".out");//getOutPath().resolve(name + "." + startingOrdinal);
        destinationPath.toFile().deleteOnExit();
        ProcessBuilder processBuilder = new ProcessBuilder(args);
        processBuilder.environment().put("TERM", "xterm");
        //processBuilder.directory(getJqPath().resolve(name).toFile()); //not yet creating the working directory
        processBuilder.redirectOutput(destinationPath.toFile());
        //processBuilder.redirectErrorStream(true);
        Process p = processBuilder.start();
        String line = null;
        StringBuilder err = new StringBuilder();
        try (BufferedReader reader = p.errorReader()) {
            while ((line = reader.readLine()) != null) {
                //TODO handle error output from process
                err.append(line);
                err.append(System.lineSeparator());
            }
        }
        try (BufferedReader reader = p.inputReader()) {
            while ((line = reader.readLine()) != null) {
                err.append(line);
                err.append(System.lineSeparator());
                //TODO handle output that somehow wasn't redirected
            }
        }
        if(!err.isEmpty()){
            System.err.println("Error processing "+node.id+" "+node.name+"\n  values: "+sourceValues.entrySet().stream().map(e->e.getKey()+"="+e.getValue().id).collect(Collectors.joining(", "))+"\n"+err);
        }
        //TODO use onExit instead of blocking the thread?
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        int ec = p.exitValue();
        if (ec != 0) {
            //TODO handle failed jq
        } else {
            int order = 0;
            //create a token from each root
            File f = destinationPath.toFile();
            try (FileInputStream fis = new FileInputStream(f)) {
                JsonFactory jf = new JsonFactory();
                JsonParser jp = jf.createParser(fis);
                jp.setCodec(new ObjectMapper());
                jp.nextToken();
                while (jp.hasCurrentToken()) {
                    JsonNode jsNode = jp.readValueAsTree();
                    Value newValue = new Value();
                    newValue.idx = order++;
                    newValue.node = node;
                    newValue.data = jsNode;
                    newValue.sources = node.sources.stream().filter(n->sourceValues.containsKey(n.name)).map(n -> sourceValues.get(n.name)).collect(Collectors.toList());
                    rtrn.add(newValue);
                    jp.nextToken();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        //remove temporary files
        paths.forEach(File::delete);

        return rtrn;
    }

    /**
     * find a Node based on the groupName:nodeName
     * @param name
     * @param groupId
     * @return
     */
    @Transactional
    public List<Node> findNodeByFqdn(String name,Long groupId){
        List<Node> rtrn = new ArrayList<>();
        if(name==null || name.isBlank()){
            return List.of();
        }
        String split[] = name.split(Node.FQDN_SEPARATOR);
        if(split.length==1){
            if(split[0].matches("[0-9]+")){
                rtrn.add(Node.findById(Long.parseLong(split[0])));
            } else {
                rtrn.addAll(Node.find("from Node n where n.group.id=?1 and n.name=?2", groupId, split[0]).list());
            }
        }else if (split.length==2){
            rtrn.addAll(Node.find("from Node n where n.group.id=?1 and n.originalGroup.name = ?2 n.name=?3",groupId,split[0],split[1]).list());
        }
        return rtrn;
    }

    @Transactional
    public List<Node> findNodeByFqdn(String fqdn){
        List<Node> rtrn = new ArrayList<>();
        if(fqdn==null || fqdn.isBlank()){
            return List.of();
        }
        if(fqdn.contains(Node.FQDN_SEPARATOR)){
            String split[] = fqdn.split(Node.FQDN_SEPARATOR);
            if(split.length==1){
                if(split[0].matches("[0-9]+")){
                    rtrn.add(Node.findById(Long.parseLong(split[0])));
                }
            }else if(split.length==2){
                String groupName = split[0];
                String nodeName = split[1];
                rtrn.addAll(Node.find("from Node n where n.group.name=?1 and n.name=?2",groupName,nodeName).list());
            }else if (split.length==3){
                String groupName = split[0];
                String originalGroupName = split[1];
                String nodeName = split[2];
                rtrn.addAll(Node.find("from Node n where n.group.name=?1 and n.originalGroup.name = ?2 and n.name=?3",groupName,originalGroupName,nodeName).list());
            }else{
                //This shouldn't happen
            }
        }
        return rtrn;
    }
}
