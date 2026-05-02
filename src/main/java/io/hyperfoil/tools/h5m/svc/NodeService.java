package io.hyperfoil.tools.h5m.svc;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.EvaluateRuntimeException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import io.hyperfoil.tools.h5m.api.FixedThresholdConfig;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.api.RelativeDifferenceConfig;
import io.hyperfoil.tools.h5m.api.svc.NodeServiceInterface;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.mapper.CycleAvoidingContext;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.pasted.ProxyJacksonArray;
import io.hyperfoil.tools.h5m.pasted.ProxyJacksonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.proxy.Proxy;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.hibernate.Session;

import io.hyperfoil.tools.jjq.JqProgram;
import io.hyperfoil.tools.jjq.jackson.JacksonConverter;
import io.hyperfoil.tools.jjq.jackson.JacksonJqEngine;
import io.hyperfoil.tools.jjq.value.JqArray;
import io.hyperfoil.tools.jjq.value.JqValue;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleBinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@ApplicationScoped
public class NodeService implements NodeServiceInterface {

    private static final JacksonJqEngine JQ_ENGINE = new JacksonJqEngine();
    private static final ConcurrentHashMap<String, JqProgram> JQ_CACHE = new ConcurrentHashMap<>();

    private static JqProgram compileJq(String filter) {
        return JQ_CACHE.computeIfAbsent(filter, JQ_ENGINE::compile);
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Inject
    EntityManager em;

    @Inject
    ApiMapper apiMapper;

    @Inject
    ValueService valueService;

    @ConfigProperty(name="quarkus.datasource.db-kind")
    String dbKind;
    @Inject
    NodeGroupService nodeGroupService;
    @Inject
    FolderService folderService;


    @Transactional
    public NodeEntity create(NodeEntity node){

        if(!node.isPersistent()){
            node.id = null;
            NodeEntity merged = em.merge(node);
            em.flush();
            node.id = merged.id;
            return merged;
        }
        return node;
    }

    @Override
    @Transactional
    public Long create(String name, Long groupId, NodeType type, String operation){
        NodeEntity node = switch (type) {
            case JQ -> JqNode.parse(name, operation, n -> internalFindNodeByFqdn(n, groupId));
            case JS -> JsNode.parse(name, operation, n -> internalFindNodeByFqdn(n, groupId));
            case JSONATA -> JsonataNode.parse(name, operation, n -> internalFindNodeByFqdn(n, groupId));
            case SQL_JSONPATH_NODE -> SqlJsonpathNode.parse(name, operation, n -> internalFindNodeByFqdn(n, groupId));
            case SQL_JSONPATH_ALL_NODE -> SqlJsonpathAllNode.parse(name, operation, n -> internalFindNodeByFqdn(n, groupId));
            default -> throw new IllegalArgumentException("Invalid node type " + type.display());
        };
        node.group = NodeGroupEntity.findById(groupId);
        if(node.sources.isEmpty()){
            node.sources.add(node.group.root);
        }
        NodeEntity merged = em.merge(node);
        em.flush();
        return merged.id;
    }

    @Override
    @Transactional
    public Long createConfigured(String name, Long groupId, NodeType type, List<Long> sources, Object configuration) throws JsonProcessingException {
        NodeEntity node = switch (type) {
            case FINGERPRINT -> new FingerprintNode(name, "");
            case FIXED_THRESHOLD -> new FixedThreshold(name, OBJECT_MAPPER.writeValueAsString(
                    OBJECT_MAPPER.convertValue(configuration, FixedThresholdConfig.class)));
            case RELATIVE_DIFFERENCE -> new RelativeDifference(name, OBJECT_MAPPER.writeValueAsString(
                    OBJECT_MAPPER.convertValue(configuration, RelativeDifferenceConfig.class)));
            default -> throw new IllegalArgumentException("Invalid node type " + type.display());
        };
        node.group = NodeGroupEntity.findById(groupId);
        node.sources = NodeEntity.findByIds(sources);

        NodeEntity merged = em.merge(node);
        em.flush();
        return merged.id;
    }

    @Transactional
    public NodeEntity read(long id){
        return NodeEntity.findById(id);
    }

    @Transactional
    public boolean functionalyEquivalent(NodeEntity a,NodeEntity b){
        if(!Objects.equals(a.name,b.name)){
            return false;
        }
        if(!Objects.equals(a.operation,b.operation)){
            return false;
        }
        if(a.sources.size()!=b.sources.size()){
            return false;
        }
        if(a.id!=null){
            a = NodeEntity.findById(a.id);
        }
        if(b.id!=null){
            b = NodeEntity.findById(b.id);
        }
        for(int i=0;i<a.sources.size() && i<b.sources.size();i++){
            if(!functionalyEquivalent(a.sources.get(i),b.sources.get(i))){
                return false;
            }
        }
        return true;
    }

    @Transactional
    public long update(NodeEntity node){
        if(node.id == null || node.id == -1){

        }else{
            NodeEntity existing = NodeEntity.findById(node.id);
            if(!existing.name.equals(node.name)){
                List<NodeEntity> toChange = em.createNativeQuery(
                        "select n.* from node n join node_edge ne on n.id = ne.child_id where ne.parent_id=? and n.type='ecma'"
                , NodeEntity.class).setParameter(1,node.id).getResultList();
                Map<String,String> changes = Map.of(existing.name,node.name);
                for(NodeEntity n : toChange){
                    n.operation = renameParameters(n.operation, changes);
                    em.merge(n);
                }
            }
            em.merge(node);
        }
        return node.id;
    }

    @Transactional
    public List<NodeEntity> getDependentNodes(NodeEntity n){
        return em.createQuery(
                "SELECT DISTINCT n FROM node n LEFT JOIN FETCH n.sources JOIN n.sources s WHERE s.id = :sourceId",
                NodeEntity.class
        ).setParameter("sourceId", n.id).getResultList();
    }

    @Transactional
    public long getNodeParentCount(NodeEntity node){
        return EdgeQueries.getParentCount(em, "node_edge", node.id);
    }

    @Transactional
    public Map<Long, Long> getNodeParentCounts(List<Long> childIds){
        return EdgeQueries.getParentCounts(em, "node_edge", childIds);
    }

    @Override
    @Transactional
    public void delete(Long nodeId){
        if(nodeId!=null) {
            NodeEntity node = NodeEntity.findById(nodeId);
            if(node == null) return;
            List<NodeEntity> dependents = getDependentNodes(node);
            for(NodeEntity dependent : dependents){
                long parentCount = getNodeParentCount(dependent);
                if(parentCount <= 1){
                    delete(dependent.id);
                }
            }
            // clean up edge rows where this node is a parent (inverse side not managed by JPA)
            EdgeQueries.deleteParentEdges(em, "node_edge", nodeId);
            NodeEntity.deleteById(nodeId);
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
    @Transactional
    public List<Map<String, ValueEntity>> calculateSourceValuePermutations(NodeEntity node, ValueEntity root) {
        List<Map<String, ValueEntity>> rtrn = new ArrayList<>();
        // Batch-fetch descendant values for all source nodes in a single query instead of N separate queries
        Map<Long, List<ValueEntity>> descendantsByNode = valueService.getDescendantValuesByNodes(root, node.sources);
        Map<String,List<ValueEntity>> nodeValues = new HashMap<>();
        for (int i = 0, size = node.sources.size(); i < size; i++) {
            NodeEntity source = node.sources.get(i);
            List<ValueEntity> found = descendantsByNode.getOrDefault(source.getId(), List.of());
            if (found.isEmpty() && source.sources.isEmpty()) {
                found = List.of(root);
            }
            nodeValues.put(source.name, found);
        }

        int maxNodeValuesLength = nodeValues.values().stream().map(Collection::size).max(Integer::compareTo).orElse(0);

        //the two cases where we do not need to worry about MultiIterationType
        if(maxNodeValuesLength == 1 || node.sources.size() == 1){//if we don't need to worry about NxN or byLength
            //to ensure sequence
            for(int i=0; i< maxNodeValuesLength; i++) {
                int idx = i;
                Map<String, ValueEntity> sourceValuesAtIndex = node.sources.stream()
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
                        Map<String, ValueEntity> sourceValuesAtIndex = new HashMap<>();
                        int idx = i;//thanks java
                        for(NodeEntity n : node.sources){
                            List<ValueEntity> nValues = nodeValues.get(n.name);
                            if(nValues.size()==1){
                                if(idx == 0){
                                    sourceValuesAtIndex.put(n.name,nValues.get(idx));
                                }else if (n.scalarMethod.equals(NodeEntity.ScalarVariableMethod.All)){
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
                    List<Map<String, ValueEntity>> valuePermutations = new ArrayList<>();
                    List<String> multiNodes = nodeValues.entrySet().stream().filter(e->e.getValue().size()>1).map(Map.Entry::getKey).toList();
                    List<String> scalarNodes = nodeValues.entrySet().stream().filter(e->e.getValue().size()==1).map(Map.Entry::getKey).toList();
                    int permutations = nodeValues.values().stream().map(List::size).reduce(1,(a,b)-> b > 0 ? a*b : a );
                    for( int i=0; i<permutations; i++ ) {
                        valuePermutations.add(new HashMap<>());
                    }
                    int loopCount = 1;
                    for( NodeEntity sourceNode : node.sources ){
                        List<ValueEntity> valueList = nodeValues.get(sourceNode.name);
                        if( !valueList.isEmpty() ){
                            if ( valueList.size() == 1 ) {
                                if ( sourceNode.scalarMethod.equals(NodeEntity.ScalarVariableMethod.First) ){
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
    public List<ValueEntity> calculateValues(NodeEntity node, List<ValueEntity> roots) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();

        switch (node.type()){
            //nodes that operate one root at a time
            case JS:
            case JQ:
            case JSONATA:
            case SQL_JSONPATH_ALL_NODE:
            case SQL_JSONPATH_NODE:
            case SPLIT:
            case FINGERPRINT:
                for(int vIdx=0; vIdx<roots.size(); vIdx++){
                    ValueEntity root =  roots.get(vIdx);
                    try {
                        List<Map<String, ValueEntity>> combinations = calculateSourceValuePermutations(node,root);
                        for(int i=0;i<combinations.size();i++){
                            Map<String, ValueEntity> combination =  combinations.get(i);
                            List<ValueEntity> createdValues = calculateNodeValues(node,combination,rtrn.size());
                            rtrn.addAll(createdValues);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();//TODO remove debug printStackTrace
                    }
                }
                break;
            case RELATIVE_DIFFERENCE:
                RelativeDifference relDiff = (RelativeDifference) node;
                for(int rIdx=0; rIdx<roots.size(); rIdx++){
                    ValueEntity root =  roots.get(rIdx);
                    List<ValueEntity> found = calculateRelativeDifferenceValues(relDiff,root,rtrn.size());
                    rtrn.addAll(found);
                }
                break;
            case FIXED_THRESHOLD:
                FixedThreshold ft = (FixedThreshold) node;
                for(int rIdx=0; rIdx<roots.size(); rIdx++){
                    ValueEntity root =  roots.get(rIdx);
                    rtrn.addAll(calculateFixedThresholdValues(ft,root,rtrn.size()));
                }
                break;
            default:
                System.err.println("calculateValues unknown node type: " + node.type());
        }
        rtrn.forEach(ValueEntity::getPath);//forcing entities to be loaded is so dirty
        return rtrn;
    }

    @Transactional
    public List<ValueEntity> calculateNodeValues(NodeEntity node,Map<String, ValueEntity> sourceValues,int startingOrdinal) throws IOException {
        return switch(node.type()){
            case JQ -> calculateJqValues((JqNode)node,sourceValues,startingOrdinal+1);
            case JS -> calculateJsValues((JsNode)node,sourceValues,startingOrdinal+1);
            case JSONATA -> calculateJsonataValues((JsonataNode)node,sourceValues,startingOrdinal+1);
            case SQL_JSONPATH_NODE -> calculateSqlJsonpathValues((SqlJsonpathNode)node,sourceValues,startingOrdinal+1);
            case SQL_JSONPATH_ALL_NODE -> calculateSqlAllJsonpathValues((SqlJsonpathAllNode)node, sourceValues, startingOrdinal+1);
            case SPLIT -> calculateSplitValues((SplitNode)node,sourceValues,startingOrdinal+1);
            case FINGERPRINT -> calculateFpValues((FingerprintNode)node,sourceValues,startingOrdinal+1);
            default -> {
                System.err.println("Unknown node type: "+node.type());
                yield Collections.emptyList();
            }
        };
    }
    //this performs change detection across all values, not just recent
    @Transactional
    public List<ValueEntity> calculateRelativeDifferenceValues(RelativeDifference relDiff, ValueEntity root,int startingOrdinal) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();
        try{
            long minPrevious = relDiff.getWindow() > relDiff.getMinPrevious() ? relDiff.getWindow() : relDiff.getMinPrevious();
            NodeEntity groupBy = NodeEntity.findById(relDiff.getGroupByNode().getId());
            List<ValueEntity> fingerprintValues = valueService.getDescendantValues(root,relDiff.getFingerprintNode());
            String fpFilter = relDiff.getFingerprintFilter();
            for(int fIdx=0; fIdx<fingerprintValues.size(); fIdx++){
                ValueEntity fingerprintValue = fingerprintValues.get(fIdx);
                if (fpFilter != null && !evaluateFingerprintFilter(fpFilter, fingerprintValue.data)) {
                    continue;
                }
                if( relDiff.getDomainNode()!=null ){

                    //when would this have more than 1 value?
                    /*
                    List<ValueEntity> domainValueFromRoot = valueService.findMatchingFingerprint(
                            relDiff.getDomainNode(),
                            groupBy,
                            fingerprintValue,
                            null,
                            null,
                            root,
                            -1,
                            -1,
                            true
                    );
                    for(int dIdx=0; dIdx<domainValueFromRoot.size(); dIdx++){
                        ValueEntity domainValue = domainValueFromRoot.get(dIdx);
                        //todo this does not look for values after previous relDiff observation :(
                        List<ValueEntity> rangeValues = valueService.findMatchingFingerprint(
                                relDiff.getRangeNode(),
                                groupBy,
                                fingerprintValue,
                                relDiff.getDomainNode(),
                                domainValue,
                                (int) (relDiff.getWindow() + minPrevious),
                                0,
                                true
                        );
                        //this would need to filter rangeValues that already have a changeDetection detected...
                    }
                    */

                    //changing domainValues to just get the domainValues from this root would change from full series scanning to just scanning the new values
                    //but that would only work if values are added sequentially to the domain value (or we delay relative difference calculation to the end of the work queue.
                    //perhaps we check if root introduced the maximum domainValue then only calculate new changes for that last window
                    //or get the domainValues greater than domain values from root and calculate all those changes?
                    List<ValueEntity> domainValues = valueService.findMatchingFingerprint(
                            relDiff.getDomainNode(),
                            groupBy,
                            fingerprintValue,
                            relDiff.getDomainNode()
                    );
                    for(int dIdx=0; dIdx<domainValues.size(); dIdx++){
                        ValueEntity domainValue = domainValues.get(dIdx);
                        //todo this does not look for values after previous relDiff observation :(
                        List<ValueEntity> rangeValues = valueService.findMatchingFingerprint(
                                relDiff.getRangeNode(),
                                groupBy,
                                fingerprintValue,
                                relDiff.getDomainNode(),
                                domainValue,
                                (int) (relDiff.getWindow() + minPrevious),
                                0,
                                true
                        );
                        List<Double> converted = rangeValues.stream().map(obj->{
                            if (obj.data instanceof NumericNode numericNode){
                                return numericNode.asDouble();
                            }else if (obj.data.toString().matches("[0-9]+\\.?[0-9]*")){
                                return Double.parseDouble(obj.data.toString());
                            }else{
                                return null;
                            }
                        }).filter(Objects::nonNull).toList();

                        if (converted.size() < relDiff.getWindow() + minPrevious) {
                            System.err.println("insufficient samples to calculate "+relDiff.name+" need "+(relDiff.getWindow() + minPrevious)+" have "+converted.size());
                        } else {
                            DoubleBinaryOperator op = switch (relDiff.getFilter()){
                                case "min" -> Double::min;
                                case "max" -> Double::max;
                                case "mean" -> Double::sum;
                                default -> Double::sum;
                            };
                            SummaryStatistics previousStats = new SummaryStatistics();
                            converted
                                    .stream()
                                    .limit(minPrevious)
                                    //.skip(relDiff.getWindow())
                                    .mapToDouble(Double::doubleValue)
                                    .forEach(previousStats::addValue);
                            Double value = converted
                                    .stream()
                                    //.limit(relDiff.getWindow())
                                    .skip(minPrevious)
                                    .mapToDouble(Double::doubleValue)
                                    .reduce(op)
                                    .getAsDouble();
                            if(relDiff.getFilter().equals("mean")){
                                value = value / (converted.size() - minPrevious);
                            }
                            double ratio = value / previousStats.getMean();
                            if( ratio < 1 - relDiff.getThreshold() || ratio > 1 + relDiff.getThreshold() ){
                                // We cannot know which datapoint is first with the regression; as a heuristic approach
                                // we'll select first datapoint with value lower than mean (if this is a drop, e.g. throughput)
                                // or above the mean (if this is an increase, e.g. memory usage).
                                Double cv = null;
                                //why does i start with less than last in window?
                                for(int i = (int)relDiff.getWindow() -1 ; i >= 0; --i){
                                    cv = converted.get(i);
                                    if( ratio < 1 && cv < previousStats.getMean() ){
                                        break;
                                    }else if (ratio > 1 && cv > previousStats.getMean()){
                                        break;
                                    }
                                }
                                assert cv != null;
                                Double prevData = converted.get((int)relDiff.getWindow()-1);
                                Double lastData = cv;
                                ObjectNode data = OBJECT_MAPPER.createObjectNode();
                                data.set("previous",new DoubleNode(prevData));
                                data.set("last",new DoubleNode(lastData));
                                data.set("value",new DoubleNode(value));
                                data.set("ratio",new DoubleNode(100*(ratio-1)));
                                //data.set("dIdx",new IntNode(dIdx));//was added for debug
                                data.set("domainvalue",domainValue.data);
                                //skip domain values due to a detection
                                dIdx+=minPrevious;
                                ValueEntity changeValue = new ValueEntity(root.folder,relDiff,data);
                                changeValue.idx=startingOrdinal;
                                List<ValueEntity> foundParents = valueService.getAncestor(fingerprintValue,groupBy);
                                if(foundParents.size()==1){
                                    changeValue.sources=foundParents;
                                }
                                rtrn.add(changeValue);
                            }
                        }
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return rtrn;
    }

    @Transactional
    public List<ValueEntity> calculateFixedThresholdValues(FixedThreshold ft, ValueEntity root, int startingOrdinal) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();
        try {
            NodeEntity groupBy = NodeEntity.findById(ft.getGroupByNode().getId());
            List<ValueEntity> fingerprintValues = valueService.getDescendantValues(root, ft.getFingerprintNode());
            String fpFilter = ft.getFingerprintFilter();

            for (int fIdx = 0; fIdx < fingerprintValues.size(); fIdx++) {
                ValueEntity fingerprintValue = fingerprintValues.get(fIdx);
                if (fpFilter != null && !evaluateFingerprintFilter(fpFilter, fingerprintValue.data)) {
                    continue;
                }

                // Get range values scoped to this fingerprint and current root
                List<ValueEntity> rangeValues = valueService.findMatchingFingerprint(
                    ft.getRangeNode(), groupBy, fingerprintValue, null, null, root, -1, -1, true
                );
                for (int rIdx = 0; rIdx < rangeValues.size(); rIdx++) {
                    ValueEntity rangeValue = rangeValues.get(rIdx);
                    Double numericValue;
                    if (rangeValue.data instanceof NumericNode numericNode) {
                        numericValue = numericNode.asDouble();
                    } else if (rangeValue.data.toString().matches("[0-9]+\\.?[0-9]*")) {
                        numericValue = Double.parseDouble(rangeValue.data.toString());
                    } else {
                        continue;
                    }

                    FixedThreshold.ViolationType violationType = null;
                    double bound = Double.NaN;

                    if (ft.isMinEnabled()) {
                        if (ft.isMinInclusive() ? numericValue < ft.getMin() : numericValue <= ft.getMin()) {
                            violationType = FixedThreshold.ViolationType.BELOW;
                            bound = ft.getMin();
                        }
                    }
                    if (violationType == null && ft.isMaxEnabled()) {
                        if (ft.isMaxInclusive() ? numericValue > ft.getMax() : numericValue >= ft.getMax()) {
                            violationType = FixedThreshold.ViolationType.ABOVE;
                            bound = ft.getMax();
                        }
                    }

                    if (violationType != null) {
                        ObjectNode data = OBJECT_MAPPER.createObjectNode();
                        data.set("value", new DoubleNode(numericValue));
                        data.set("bound", new DoubleNode(bound));
                        data.put("direction", violationType.label());
                        data.set("fingerprint", fingerprintValue.data);
                        ValueEntity changeValue = new ValueEntity(root.folder, ft, data);
                        changeValue.idx = startingOrdinal;
                        List<ValueEntity> foundParents = valueService.getAncestor(fingerprintValue, groupBy);
                        if (foundParents.size() == 1) {
                            changeValue.sources = foundParents;
                        }
                        rtrn.add(changeValue);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rtrn;
    }

    @Transactional
    public List<ValueEntity> calculateSqlJsonpathValues(SqlJsonpathNode node, Map<String, ValueEntity> sourceValues, int startingOrdinal) throws IOException {
        return calculateSqlJsonpathValuesFirstOrAll(node,sourceValues,startingOrdinal,"jsonb_path_query_first");
    }
    @Transactional
    public List<ValueEntity> calculateSqlAllJsonpathValues(SqlJsonpathAllNode node, Map<String, ValueEntity> sourceValues, int startingOrdinal) throws IOException {
        return calculateSqlJsonpathValuesFirstOrAll(node,sourceValues,startingOrdinal,"jsonb_path_query_array");
    }
    private List<ValueEntity> calculateSqlJsonpathValuesFirstOrAll(NodeEntity node, Map<String, ValueEntity> sourceValues, int startingOrdinal,String psqlFunction) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();
        if(sourceValues.isEmpty()){//end early when there isn't input
            return rtrn;
        }

        if(sourceValues.size()>1 || node.sources.size()>1){
            System.err.println("sql jsonpath only supports one input at a time");
            return Collections.emptyList();
        }
        ValueEntity input = sourceValues.get(node.sources.getFirst().name);
        ValueEntity tempV = new ValueEntity(null,node,null);
        tempV.sources=List.of(input);
        tempV.idx=startingOrdinal;
        ValueEntity newValue = valueService.create(tempV);
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

            JsonNode found = (JsonNode) em.createNativeQuery(
                    switch(dbKind){
                        case "sqlite" -> "select data from value where data is not null and data != 'null' and id = ?";
                        case "postgresql" -> "select data from value where data is not null and data != 'null'::jsonb and id = ?";
                        default -> "";
                    }
                , JsonNode.class)
                .setParameter(1,newValue.id)
                .getSingleResultOrNull();
            if( found == null ){
                valueService.delete(newValue);
            } else {
                //empty result mascarading as an empty array
                if(dbKind.equals("postgresql") && psqlFunction.equals("jsonb_path_query_array") && found.isArray() && found.size()==0){
                    valueService.delete(newValue);
                } else {
                    newValue.data = found;
                    rtrn.add(newValue);
                }
            }
        });
        return rtrn;
    }
    @Transactional
    public List<ValueEntity> calculateSplitValues(SplitNode node, Map<String, ValueEntity> sourceValues, int startingOrdinal) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();
        if(sourceValues.isEmpty()){
            return rtrn;
        }
        if(sourceValues.size()>1 || node.sources.size()>1){
            System.err.println("split only supports one input node at a time");
        }
        //this will naively do the split with entities but using json_each would be good
        //TODO should this be done in db with json_each?
        ValueEntity v = sourceValues.get(node.sources.getFirst().name);
        if(v!=null){
            if(v.data.isArray()){
                ArrayNode arrayNode = (ArrayNode) v.data;
                for(int i=0;i<arrayNode.size();i++){
                    JsonNode entry = arrayNode.get(i);
                    ValueEntity newValue = new ValueEntity(null,node,entry);
                    newValue.idx=i;
                    newValue.sources = List.of(v);
                    rtrn.add(newValue);
                }
            }else{
                ValueEntity newValue = new ValueEntity(null,node,v.data);
                newValue.idx=0;
                newValue.sources = List.of(v);
                rtrn.add(newValue);
            }
        }
        return rtrn;
    }


    //jsonata cannot operate on multiple inputs at once so source
    public List<ValueEntity> calculateJsonataValues(JsonataNode node,Map<String, ValueEntity> sourceValues,int startingOrdinal) throws IOException {
        if(sourceValues.size()>1 || node.sources.size()>1){
            System.err.println("jsonata only supports one input at a time");
            return Collections.emptyList();
        }

        ValueEntity input = sourceValues.isEmpty() ? null : sourceValues.values().iterator().next();

        List<ValueEntity> rtrn = new ArrayList<>();

        try {
            Expressions expr = Expressions.parse(node.operation);
            JsonNode result = expr.evaluate(input.data);

            ValueEntity newValue = new ValueEntity();
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

    private static int preceedingNonSpace(int idx,String input){
        do{
            idx--;
        }while(idx >= 0 && " \t\n".contains(input.substring(idx,idx+1)));
        return idx;
    }
    private static int followingNonSpace(int idx,String input){
        while(idx < input.length() && " \t\n".contains(input.substring(idx,idx+1))){
            idx++;
        }
        return idx;
    }
    public static String jsonpathToJq(String jsonpath) {
        if (jsonpath == null || jsonpath.isEmpty()) return ".";
        String jq = jsonpath;
        if (jq.startsWith("$.")) jq = jq.substring(1);
        else if (jq.equals("$")) return ".";
        jq = jq.replace("[*]", "[]?");
        jq = jq.replace(".*", "[]?");
        // Convert PostgreSQL jsonpath filter expressions to JQ select()
        // ? (@.field == "value") → [] | select(.field == "value")
        // ? (@.field != "value") → [] | select(.field != "value")
        // ? (@.field like_regex "pattern") → [] | select(.field | test("pattern"))
        if (jq.contains("?")) {
            jq = convertJsonpathFilters(jq);
        }
        return jq;
    }

    static String convertJsonpathFilters(String jq) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        while (i < jq.length()) {
            // Look for ? ( or ?( — the jsonpath filter pattern, not []? which is JQ optional
            int filterStart = -1;
            for (int s = i; s < jq.length(); s++) {
                if (jq.charAt(s) == '?' && s + 1 < jq.length()) {
                    int next = s + 1;
                    while (next < jq.length() && jq.charAt(next) == ' ') next++;
                    if (next < jq.length() && jq.charAt(next) == '(') {
                        filterStart = s;
                        break;
                    }
                }
            }
            if (filterStart == -1) {
                result.append(jq, i, jq.length());
                break;
            }
            String before = jq.substring(i, filterStart).trim();
            if (!before.isEmpty()) {
                result.append(before);
            }
            if (!result.toString().endsWith("[]?")) {
                result.append("[]?");
            }
            int parenStart = jq.indexOf("(", filterStart);
            if (parenStart == -1) {
                result.append(jq, filterStart, jq.length());
                break;
            }
            int depth = 1;
            int parenEnd = parenStart + 1;
            while (parenEnd < jq.length() && depth > 0) {
                if (jq.charAt(parenEnd) == '(') depth++;
                else if (jq.charAt(parenEnd) == ')') depth--;
                parenEnd++;
            }
            String filterBody = jq.substring(parenStart + 1, parenEnd - 1).trim();
            // Convert @.field references to .field and handle quoted field names
            // @."special" → .["special"], @.normal → .normal
            filterBody = filterBody.replaceAll("@\\.\"([^\"]+)\"", ".[\"$1\"]");
            filterBody = filterBody.replace("@.", ".");
            // Handle like_regex → test()
            if (filterBody.contains("like_regex")) {
                filterBody = filterBody.replaceAll("(\\S+)\\s+like_regex\\s+\"([^\"]+)\"", "($1 | test(\"$2\"))");
            }
            result.append(" | select(").append(filterBody).append(")");
            i = parenEnd;
        }
        // Convert remaining ."text()" style field access to ["text()"]
        String output = result.toString();
        output = output.replaceAll("\\.\"([^\"]+)\"", ".[\"$1\"]");
        return output;
    }

    public static String renameParameters(String function,Map<String,String> renames){
        for(String key:renames.keySet()){
            Matcher m = Pattern.compile(key).matcher(function);
            while(m.find()){
                int before = preceedingNonSpace(m.start(),function);
                int after = followingNonSpace(m.end(),function);

                String previous = ""+(before > 0 ? function.charAt(before) : ' ');
                String following = ""+(after < function.length() ? function.charAt(after) : ' ');
                //conditions that do not match the variable reference
                if(
                    previous.matches("[a-zA-Z_\\$]") || //part of another name
                    following.matches("[a-zA-Z_\\$0-9]") || //part of another name
                    previous.equals(".") || //method call
                            (following.equals(":") && !previous.equals("?")) // key in an object, not a tertiary expression
                ){
                    //skip it
                }else{
                    function =
                        function.substring(0, m.start()) +
                        renames.get(key) +
                        function.substring(m.end());
                    m.reset(function);
                }
            }
        }
        return function;
    }

    @Transactional
    public List<ValueEntity> calculateJsValues(JsNode node,Map<String, ValueEntity> sourceValues,int startingOrdinal) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();
        List<String> params = JsNode.getParameterNames(node.operation);
        if(params == null){
            System.err.println("Error occurred reading parameters from js function\n"+node.operation);
            return Collections.emptyList();
        }
        List<JsonNode> input = JsNode.createParameters(node.operation, sourceValues, node.sources.size());
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
                            try {
                                data = OBJECT_MAPPER.readTree(result.toString());
                            } catch (JsonProcessingException e) {
                                System.err.println("failed to convert "+result+" to a javascript object");
                            }
                        }

                        //File valuePath = JqNode.outputPath().resolve(node.name + "." + (startingOrdinal+1)+".jq").toFile();
                        if(data!=null) {
                            ValueEntity newValue = new ValueEntity();
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
        if (value == null) {
            return null;
        } else if (value.isNull()) {
            // ValueEntity api cannot differentiate null and undefined from javascript
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
                return OBJECT_MAPPER.readTree(p.toString());
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
            //TODO log error wtf is ValueEntity?
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



    @Transactional
    public List<ValueEntity> calculateFpValues(FingerprintNode node, Map<String, ValueEntity> sourceValues, int startingOrdinal) throws IOException {
        ObjectNode fpObject = OBJECT_MAPPER.createObjectNode();
        TreeMap<String, JsonNode> sorted = new TreeMap<>();
        for (NodeEntity source : node.sources) {
            if (sourceValues.containsKey(source.name)) {
                sorted.put(source.name, sourceValues.get(source.name).data);
            }
        }
        sorted.forEach(fpObject::set);
        ValueEntity newValue = new ValueEntity();
        newValue.idx = startingOrdinal+1;
        newValue.node = node;
        newValue.data = fpObject;
        newValue.sources = node.sources.stream().filter(n->sourceValues.containsKey(n.name)).map(n -> sourceValues.get(n.name)).collect(Collectors.toList());
        return List.of(newValue);
    }
    public boolean evaluateFingerprintFilter(String filter, JsonNode fingerprint) {
        if (filter == null || filter.isBlank()) {
            return true;
        }
        try (Context context = Context.newBuilder("js")
                .engine(Engine.newBuilder("js").option("engine.WarnInterpreterOnly", "false").build())
                .allowExperimentalOptions(true)
                .option("js.foreign-object-prototype", "true")
                .option("js.global-property", "true")
                .build()) {
            context.enter();
            try {
                String jsCode = "const __fp = " + fingerprint.toString() + ";\n" +
                        "const __filter = " + filter + ";\n" +
                        "!!(__filter(__fp));";
                org.graalvm.polyglot.Value result = context.eval("js", jsCode);
                return result.asBoolean();
            } catch (PolyglotException e) {
                System.err.println("failed to evaluate fingerprint filter: " + e.getMessage());
                return true;
            } finally {
                context.leave();
            }
        }
    }

    @Transactional
    public List<ValueEntity> calculateJqValues(JqNode node,Map<String, ValueEntity> sourceValues,int startingOrdinal) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();

        boolean isNullInput = JqNode.isNullInput(node.operation);

        JqProgram program;
        try {
            program = compileJq(node.operation);
        } catch (Exception e) {
            System.err.println("Error compiling jq filter for node " + node.id + " " + node.name + ": " + e.getMessage());
            return rtrn;
        }

        // Collect source data in order, preserving node.sources ordering
        List<JsonNode> sourceData = new ArrayList<>();
        if (!node.sources.isEmpty()) {
            List.copyOf(node.sources).forEach(sourceNode -> {
                if (sourceValues.containsKey(sourceNode.name)) {
                    sourceData.add(sourceValues.get(sourceNode.name).data);
                }
            });
        } else {
            sourceValues.values().forEach(sourceValue -> sourceData.add(sourceValue.data));
        }

        // Determine jq mode, mirroring how jq processes a JSONL stream:
        //   --null-input:  . is null, sources accessible only via inputs/input
        //   --slurp:       all sources combined into a single array
        //   default:       filter runs on the single source value
        boolean isSlurp = !isNullInput && (node.sources.size() > 1 || sourceValues.size() > 1);

        try {
            List<JsonNode> results;
            if (isNullInput) {
                List<JqValue> jqInputs = sourceData.stream()
                        .map(JacksonConverter::fromJsonNodeLazy)
                        .toList();
                results = program.applyNullInput(jqInputs).stream()
                        .map(v -> JacksonConverter.toJsonNode(v, JQ_ENGINE.mapper()))
                        .toList();
            } else if (isSlurp) {
                List<JqValue> jqInputs = sourceData.stream()
                        .map(JacksonConverter::fromJsonNodeLazy)
                        .toList();
                results = program.applyAll(JqArray.ofTrusted(jqInputs)).stream()
                        .map(v -> JacksonConverter.toJsonNode(v, JQ_ENGINE.mapper()))
                        .toList();
            } else {
                JsonNode input = sourceData.isEmpty() ? NullNode.getInstance() : sourceData.getFirst();
                results = JQ_ENGINE.apply(program, input);
            }
            int order = startingOrdinal;
            for (JsonNode jsNode : results) {
                if (!jsNode.isNull()) {
                    ValueEntity newValue = new ValueEntity();
                    newValue.idx = order++;
                    newValue.node = node;
                    newValue.data = jsNode;
                    newValue.sources = node.sources.stream()
                            .filter(n -> sourceValues.containsKey(n.name))
                            .map(n -> sourceValues.get(n.name))
                            .collect(Collectors.toList());
                    rtrn.add(newValue);
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing " + node.id + " " + node.name
                    + "\n  values: " + sourceValues.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue().id)
                    .collect(Collectors.joining(", "))
                    + "\n" + e.getMessage());
        }

        return rtrn;
    }

    /**
     * find a NodeEntity based on the groupName:nodeName
     * @param name
     * @param groupId
     * @return
     */
    @Override
    @Transactional
    public List<Node> findNodeByFqdn(String name,Long groupId){
        CycleAvoidingContext cycleContext = new CycleAvoidingContext();
        return internalFindNodeByFqdn(name, groupId).stream().map(entity -> apiMapper.toNode(entity, cycleContext)).toList();
    }

    private List<NodeEntity> internalFindNodeByFqdn(String name,Long groupId){
        if(name==null || name.isBlank()){
            return List.of();
        }
        String[] split = name.split(NodeEntity.FQDN_SEPARATOR);
        if(split.length==1){
            if(split[0].matches("[0-9]+")){
                return NodeEntity.findById(Long.parseLong(split[0]));
            } else {
                return NodeEntity.list("from node n where n.group.id=?1 and n.name=?2", groupId, split[0]);
            }
        }else if (split.length==2){
            return NodeEntity.list("from node n where n.group.id=?1 and n.originalGroup.name = ?2 n.name=?3",groupId,split[0],split[1]);
        }
        return List.of();
    }

    @Override
    @Transactional
    public List<Node> findNodeByFqdn(String fqdn){
        if(fqdn==null || fqdn.isBlank()){
            return List.of();
        }
        if(fqdn.contains(NodeEntity.FQDN_SEPARATOR)){
            CycleAvoidingContext cycleContext = new CycleAvoidingContext();
            String[] split = fqdn.split(NodeEntity.FQDN_SEPARATOR);
            if(split.length==1){
                if(split[0].matches("[0-9]+")){
                    return List.of(apiMapper.toNode(NodeEntity.findById(Long.parseLong(split[0])), cycleContext));
                }
            }else if(split.length==2){
                String groupName = split[0];
                String nodeName = split[1];
                List<Node> rtrn = new ArrayList<>();
                rtrn.addAll(NodeEntity.<NodeEntity>stream("from node n where n.group.name=?1 and n.name=?2",groupName,nodeName).map(entity -> apiMapper.toNode(entity, cycleContext)).toList());
                rtrn.addAll(em.unwrap(Session.class).createNativeQuery(
                    """
                    select c.*
                    from node c join node_edge ne on c.id = ne.child_id join node p on p.id = ne.parent_id 
                    where c.name=:nodeName and p.name=:parentName
                    """
                    , NodeEntity.class)
                    .setParameter("nodeName",nodeName)
                    .setParameter("parentName",groupName).stream().map(entity -> apiMapper.toNode(entity, cycleContext)).toList()
                );
                return rtrn;
            }else if (split.length==3){
                String groupName = split[0];
                String originalGroupName = split[1];
                String nodeName = split[2];
                return NodeEntity.<NodeEntity>stream("from node n where n.group.name=?1 and n.originalGroup.name = ?2 and n.name=?3",groupName,originalGroupName,nodeName).map(entity -> apiMapper.toNode(entity, cycleContext)).toList();
            }else{
                //This shouldn't happen
            }
        }
        return List.of();
    }
}
