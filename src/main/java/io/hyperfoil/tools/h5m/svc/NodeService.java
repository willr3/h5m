package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.jjq.jsonata.JsonataCompiler;
import io.hyperfoil.tools.jjq.jsonata.JsonataException;
import io.hyperfoil.tools.jjq.value.*;
import io.hyperfoil.tools.h5m.api.EDivisiveConfig;
import io.hyperfoil.tools.h5m.api.FixedThresholdConfig;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.api.RelativeDifferenceConfig;
import io.hyperfoil.tools.h5m.api.StdDevAnomalyConfig;
import io.hyperfoil.tools.jhunter.Analysis;
import io.hyperfoil.tools.jhunter.AnalysisOptions;
import io.hyperfoil.tools.jhunter.ChangePoint;
import io.hyperfoil.tools.h5m.api.svc.NodeServiceInterface;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.mapper.CycleAvoidingContext;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.pasted.ProxyJq;
import io.hyperfoil.tools.h5m.pasted.ProxyJqObject;
import io.hyperfoil.tools.h5m.pasted.Util;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.hibernate.Session;

import io.hyperfoil.tools.jjq.JqProgram;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleBinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@ApplicationScoped
public class NodeService implements NodeServiceInterface {

    private static final ConcurrentHashMap<String, JqProgram> JQ_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, JqProgram> JSONATA_CACHE = new ConcurrentHashMap<>();

    private static JqProgram compileJq(String filter) {
        return JQ_CACHE.computeIfAbsent(filter, JqProgram::compile);
    }

    private static JqProgram compileJsonata(String expression) {
        return JSONATA_CACHE.computeIfAbsent(expression, JsonataCompiler::compile);
    }

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
    public Long createConfigured(String name, Long groupId, NodeType type, List<Long> sources, Object configuration) {
        NodeEntity node = switch (type) {
            case FINGERPRINT -> new FingerprintNode(name, "");
            case FIXED_THRESHOLD -> new FixedThreshold(name, toFixedThresholdConfig(configuration).toJsonString());
            case RELATIVE_DIFFERENCE -> new RelativeDifference(name, toRelativeDifferenceConfig(configuration).toJsonString());
            case STDDEV_ANOMALY -> new StdDevAnomaly(name, toStdDevAnomalyConfig(configuration).toJsonString());
            case EDIVISIVE -> new EDivisive(name, toEDivisiveConfig(configuration).toJsonString());
            default -> throw new IllegalArgumentException("Invalid node type " + type.display());
        };
        node.group = NodeGroupEntity.findById(groupId);
        node.sources = NodeEntity.findByIds(sources);

        NodeEntity merged = em.merge(node);
        em.flush();
        return merged.id;
    }

    /** Convert configuration (Map from REST or record from CLI) to JqObject for FixedThreshold. */
    private static JqObject toFixedThresholdConfig(Object config) {
        if (config instanceof FixedThresholdConfig ft) {
            JqObject.Builder b = JqObject.builder();
            if (ft.min() != null) b.put("min", ft.min());
            if (ft.max() != null) b.put("max", ft.max());
            if (ft.minInclusive() != null) b.put("minInclusive", ft.minInclusive());
            if (ft.maxInclusive() != null) b.put("maxInclusive", ft.maxInclusive());
            if (ft.fingerprintFilter() != null) b.put("fingerprintFilter", ft.fingerprintFilter());
            return b.build();
        } else if (config instanceof Map<?, ?>) {
            JqValue v = JqValues.fromJavaObject(config);
            if (v instanceof JqObject obj) return obj;
        }
        throw new IllegalArgumentException("Invalid FixedThreshold configuration: " + config);
    }

    /** Convert configuration (Map from REST or record from CLI) to JqObject for RelativeDifference. */
    private static JqObject toRelativeDifferenceConfig(Object config) {
        if (config instanceof RelativeDifferenceConfig rd) {
            JqObject.Builder b = JqObject.builder();
            if (rd.filter() != null) b.put("filter", rd.filter());
            b.put("threshold", rd.threshold());
            b.put("window", (long) rd.window());
            b.put("minPrevious", (long) rd.minPrevious());
            if (rd.fingerprintFilter() != null) b.put("fingerprintFilter", rd.fingerprintFilter());
            return b.build();
        } else if (config instanceof Map<?, ?>) {
            JqValue v = JqValues.fromJavaObject(config);
            if (v instanceof JqObject obj) return obj;
        }
        throw new IllegalArgumentException("Invalid RelativeDifference configuration: " + config);
    }

    /** Convert configuration to JqObject for StdDevAnomaly. */
    private static JqObject toStdDevAnomalyConfig(Object config) {
        if (config instanceof StdDevAnomalyConfig sd) {
            JqObject.Builder b = JqObject.builder();
            b.put("windowSize", (long) sd.windowSize());
            b.put("deviations", sd.deviations());
            if (sd.direction() != null) b.put("direction", sd.direction().name());
            b.put("minDataPoints", (long) sd.minDataPoints());
            if (sd.fingerprintFilter() != null) b.put("fingerprintFilter", sd.fingerprintFilter());
            return b.build();
        } else if (config instanceof Map<?, ?>) {
            JqValue v = JqValues.fromJavaObject(config);
            if (v instanceof JqObject obj) return obj;
        }
        throw new IllegalArgumentException("Invalid StdDevAnomaly configuration: " + config);
    }

    /** Convert configuration (Map from REST or record from CLI) to JqObject for EDivisive. */
    private static JqObject toEDivisiveConfig(Object config) {
        if (config instanceof EDivisiveConfig ed) {
            JqObject.Builder b = JqObject.builder();
            b.put("windowLen", (long) ed.windowLen());
            b.put("maxPvalue", ed.maxPvalue());
            b.put("minMagnitude", ed.minMagnitude());
            b.put("maxSeriesLength", (long) ed.maxSeriesLength());
            if (ed.fingerprintFilter() != null) b.put("fingerprintFilter", ed.fingerprintFilter());
            return b.build();
        } else if (config instanceof Map<?, ?>) {
            JqValue v = JqValues.fromJavaObject(config);
            if (v instanceof JqObject obj) return obj;
        }
        throw new IllegalArgumentException("Invalid EDivisive configuration: " + config);
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
            em.flush();
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
                                //do nothing with a missing value
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
            case STDDEV_ANOMALY:
                StdDevAnomaly sd = (StdDevAnomaly) node;
                for(int rIdx=0; rIdx<roots.size(); rIdx++){
                    ValueEntity root =  roots.get(rIdx);
                    rtrn.addAll(calculateStdDevAnomalyValues(sd, root, rtrn.size()));
                }
                break;
            case EDIVISIVE:
                EDivisive ed = (EDivisive) node;
                for(int rIdx=0; rIdx<roots.size(); rIdx++){
                    ValueEntity root = roots.get(rIdx);
                    rtrn.addAll(calculateEDivisiveValues(ed, root, rtrn.size()));
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
            for(int fIdx=0; fIdx<fingerprintValues.size(); fIdx++) {
                ValueEntity fingerprintValue = fingerprintValues.get(fIdx);
                if (fpFilter != null && !evaluateFingerprintFilter(fpFilter, fingerprintValue.data)) {
                    continue;
                }
                if (relDiff.getDomainNode() != null) {
                    //changing domainValues to just get the domainValues from this root would change from full series scanning to just scanning the new values
                    //but that would only work if values are added sequentially to the domain value (or we delay relative difference calculation to the end of the work queue.
                    //perhaps we check if root introduced the maximum domainValue then only calculate new changes for that last window
                    //or get the domainValues greater than domain values from root and calculate all those changes?
                    List<ValueEntity> rootDomainValues = valueService.getDescendantValues(root, relDiff.getDomainNode());
                    List<ValueEntity> allDomainValues = new ArrayList<>();
                    for (int sdIdx = 0; sdIdx < rootDomainValues.size(); sdIdx++) {
                        ValueEntity uploadedDomainValue = rootDomainValues.get(sdIdx);
                        List<ValueEntity> preceedingDomainValues = valueService.findMatchingFingerprint(
                                relDiff.getDomainNode(),
                                groupBy,
                                fingerprintValue,
                                relDiff.getDomainNode(),
                                uploadedDomainValue,
                                null,
                                (int) (relDiff.getWindow() + minPrevious),
                                0,
                                true
                        );

                        List<ValueEntity> followingDomainValues = valueService.findMatchingFingerprint(
                                relDiff.getDomainNode(),
                                groupBy,
                                fingerprintValue,
                                relDiff.getDomainNode(),
                                uploadedDomainValue,
                                null,
                                (int) (relDiff.getWindow() + minPrevious),
                                0,
                                false
                        );
                        if(!followingDomainValues.isEmpty()) {
                            followingDomainValues.remove(0);
                        }
                        allDomainValues.addAll(preceedingDomainValues);
                        allDomainValues.addAll(followingDomainValues);
                        for (int dIdx = 0; dIdx < allDomainValues.size(); dIdx++) {
                            ValueEntity domainValue = allDomainValues.get(dIdx);
                            //todo this does not look for values after previous relDiff observation :(
                            List<ValueEntity> rangeValues = valueService.findMatchingFingerprint(
                                    relDiff.getRangeNode(),
                                    groupBy,
                                    fingerprintValue,
                                    relDiff.getDomainNode(),
                                    domainValue,
                                    null,
                                    (int) (relDiff.getWindow() + minPrevious),
                                    0,
                                    true
                            );
                            List<Double> converted = rangeValues.stream()
                                .map(obj -> obj.data != null ? obj.data.tryDouble() : null)
                                .filter(Objects::nonNull).toList();

                            if (converted.size() < relDiff.getWindow() + minPrevious) {
                                System.err.println("insufficient samples to calculate " + relDiff.name + " need " + (relDiff.getWindow() + minPrevious) + " have " + converted.size());
                            } else {
                                DoubleBinaryOperator op = switch (relDiff.getFilter()) {
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
                                if (relDiff.getFilter().equals("mean")) {
                                    value = value / (converted.size() - minPrevious);
                                }
                                double ratio = value / previousStats.getMean();
                                if (ratio < 1 - relDiff.getThreshold() || ratio > 1 + relDiff.getThreshold()) {
                                    // We cannot know which datapoint is first with the regression; as a heuristic approach
                                    // we'll select first datapoint with value lower than mean (if this is a drop, e.g. throughput)
                                    // or above the mean (if this is an increase, e.g. memory usage).
                                    Double cv = null;
                                    //why does i start with less than last in window?
                                    for (int i = (int) relDiff.getWindow() - 1; i >= 0; --i) {
                                        cv = converted.get(i);
                                        if (ratio < 1 && cv < previousStats.getMean()) {
                                            break;
                                        } else if (ratio > 1 && cv > previousStats.getMean()) {
                                            break;
                                        }
                                    }
                                    assert cv != null;
                                    Double prevData = converted.get((int) relDiff.getWindow() - 1);
                                    Double lastData = cv;
                                    JqValue data = JqObject.builder()
                                            .put("previous", prevData)
                                            .put("last", lastData)
                                            .put("value", value)
                                            .put("ratio", 100 * (ratio - 1))
                                            .put("domainvalue", domainValue.data)
                                            .build();
                                    //skip domain values due to a detection
                                    dIdx += minPrevious;
                                    ValueEntity changeValue = new ValueEntity(root.folder, relDiff, data);
                                    changeValue.idx = startingOrdinal;
                                    List<ValueEntity> foundParents = valueService.getAncestor(domainValue, groupBy);
                                    if (foundParents.size() == 1) {
                                        changeValue.sources = foundParents;
                                    }

                                    rtrn.add(changeValue);
                                }
                            }
                        }
                        List<ValueEntity> persistedChangeValues = valueService.findMatchingFingerprint(
                                relDiff,
                                groupBy,
                                fingerprintValue,
                                relDiff.getDomainNode(),
                                uploadedDomainValue,
                                null,
                                (int) (relDiff.getWindow() + minPrevious),
                                0,
                                false
                        );
                        List<JqValue> domainRemoveScope = new ArrayList<>();
                        for (ValueEntity dv : allDomainValues) {
                            if (dv.data != null) {
                                domainRemoveScope.add(dv.data);
                            }
                        }
                        if (!rtrn.isEmpty()) {
                            for (ValueEntity existingValue : persistedChangeValues) {
                                JqValue existingDomainValue = existingValue.data != null ? existingValue.data.getField("domainvalue") : JqNull.NULL;
                                if (existingDomainValue.isNull()) continue;
                                if (domainRemoveScope.contains(existingDomainValue)) {
                                    boolean match = false;
                                    for (ValueEntity currentValue : rtrn) {
                                        JqValue currentDomainValue = currentValue.data != null ? currentValue.data.getField("domainvalue") : JqNull.NULL;
                                        if (currentDomainValue.isNull()) continue;
                                        if (existingDomainValue.equals(currentDomainValue)) {
                                            match = true;
                                            break;
                                        }
                                    }
                                    if (!match) {
                                        valueService.delete(existingValue);
                                    }
                                }
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
                    Double numericValue = rangeValue.data != null ? rangeValue.data.tryDouble() : null;
                    if (numericValue == null) {
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
                        JqValue data = JqObject.builder()
                                .put("value", numericValue)
                                .put("bound", bound)
                                .put("direction", violationType.label())
                                .put("fingerprint", fingerprintValue.data)
                                .build();
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

    /**
     * Calculates e-divisive change points for the given root value.
     * Collects the most recent maxSeriesLength range values per fingerprint
     * (ordered by domain node), deletes existing e-divisive values within the
     * analysis window, then calls jhunter to detect change points and creates
     * a ValueEntity for each.
     *
     * A domain node is required for e-divisive — the algorithm needs a
     * meaningful ordering to produce useful results. Change points outside
     * the maxSeriesLength window are preserved from previous analyses.
     *
     * Note: uploads with domain values older than the maxSeriesLength window
     * (e.g., backported runs) will not be included in the analysis.
     * A manual recalculate with a larger maxSeriesLength can cover them.
     *
     * E-divisive Work is marked as cumulative, so the work queue deduplicates
     * multiple Work items for the same node in a batch — only one runs.
     * During recalculation (where Work is queued for every root value),
     * the cumulative flag ensures e-divisive blocks until all extraction
     * Work completes, and Work.equals() ignores sourceValues for cumulative
     * Work, so duplicate cascade-created e-divisive Work items are
     * deduplicated by WorkQueue.hasWork(). Result: e-divisive runs exactly
     * once per recalculation batch, analyzing the full series.
     */
    @Transactional
    public List<ValueEntity> calculateEDivisiveValues(EDivisive ed, ValueEntity root, int startingOrdinal) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();
        try {
            if (ed.getDomainNode() == null) {
                Log.errorf("e-divisive node %s requires a domain node for meaningful ordering", ed.name);
                return rtrn;
            }

            NodeEntity groupBy = NodeEntity.findById(ed.getGroupByNode().getId());
            List<ValueEntity> fingerprintValues = valueService.getDescendantValues(root, ed.getFingerprintNode());
            String fpFilter = ed.getFingerprintFilter();
            int maxSeriesLength = ed.getMaxSeriesLength();
            for (int fIdx = 0; fIdx < fingerprintValues.size(); fIdx++) {
                ValueEntity fingerprintValue = fingerprintValues.get(fIdx);
                if (fpFilter != null && !evaluateFingerprintFilter(fpFilter, fingerprintValue.data)) {
                    continue;
                }

                // Check if this upload contributed a range value for this fingerprint.
                // Scope through the groupBy ancestor to handle multi-dataset uploads.
                List<ValueEntity> groupByValues = valueService.getAncestor(fingerprintValue, groupBy);
                if (groupByValues.isEmpty()) {
                    continue;
                }
                ValueEntity groupByValue = groupByValues.getFirst();
                List<ValueEntity> currentRangeValues = valueService.getDescendantValues(groupByValue, ed.getRangeNode());
                if (currentRangeValues.isEmpty()) {
                    continue; // this upload didn't produce a range value for this fingerprint
                }

                // Collect the most recent maxSeriesLength range values for this fingerprint,
                // ordered by domain ascending. The limit is applied in SQL.
                // preceedingValues=true fetches the most recent values and returns them
                // in ascending order (findMatchingFingerprint reverses internally).
                List<ValueEntity> rangeValues = valueService.findMatchingFingerprint(
                        ed.getRangeNode(), groupBy, fingerprintValue,
                        ed.getDomainNode(), null,
                        maxSeriesLength, 0, true);

                if (rangeValues.size() < ed.getWindowLen()) {
                    continue;
                }

                // Extract double[] from range values
                double[] series = new double[rangeValues.size()];
                int validCount = 0;
                for (int i = 0; i < rangeValues.size(); i++) {
                    Double d = rangeValues.get(i).data != null ? rangeValues.get(i).data.tryDouble() : null;
                    if (d != null) {
                        series[validCount++] = d;
                    }
                }

                if (validCount < ed.getWindowLen()) {
                    continue;
                }

                // Trim array if some values were non-numeric
                if (validCount < series.length) {
                    series = Arrays.copyOf(series, validCount);
                }

                // Run jhunter analysis
                AnalysisOptions options = new AnalysisOptions(
                        ed.getWindowLen(), ed.getMaxPvalue(), ed.getMinMagnitude());
                List<ChangePoint> changePoints = Analysis.computeChangePoints(series, options);

                // Delete existing e-divisive change points within the analysis window.
                // E-divisive recomputes the entire window each time, so all old change points
                // within the window are replaced by the new results. Change points outside the
                // window (from older analyses with values that fell out of maxSeriesLength) are
                // preserved — they represent historical detections that are no longer in scope.
                Set<Long> windowRangeIds = new HashSet<>();
                for (ValueEntity rv : rangeValues) {
                    if (rv.id != null) windowRangeIds.add(rv.id);
                }
                List<ValueEntity> existingEdValues = valueService.findMatchingFingerprint(
                        ed, groupBy, fingerprintValue, (NodeEntity) null);
                for (ValueEntity existing : existingEdValues) {
                    JqValue rangeIdNode = existing.data != null ? existing.data.getField("rangeValueId") : JqNull.NULL;
                    if (!rangeIdNode.isNull() && windowRangeIds.contains((long) rangeIdNode.asDouble(0.0))) {
                        valueService.delete(existing);
                    }
                }

                // Create a ValueEntity for each detected change point
                for (ChangePoint cp : changePoints) {
                    JqObject.Builder dataBuilder = JqObject.builder();
                    dataBuilder.put("index", (long) cp.index());
                    dataBuilder.put("pvalue", cp.pvalue());
                    dataBuilder.put("meanBefore", cp.meanBefore());
                    dataBuilder.put("meanAfter", cp.meanAfter());
                    dataBuilder.put("stdBefore", cp.stdBefore());
                    dataBuilder.put("stdAfter", cp.stdAfter());
                    dataBuilder.put("magnitude", cp.magnitude());
                    // Hazard level: symmetric measure for triage sorting (MongoDB/ICPE 2020)
                    if (cp.meanBefore() != 0.0 && cp.meanAfter() > 0.0) {
                        dataBuilder.put("hazardLevel", Math.log(cp.meanAfter() / cp.meanBefore()));
                    }
                    dataBuilder.put("fingerprint", fingerprintValue.data);
                    // Reference the range and domain values at the change point index
                    if (cp.index() < rangeValues.size()) {
                        ValueEntity rangeAtCp = rangeValues.get(cp.index());
                        // Store range value ID for windowed deletion on recomputation
                        if (rangeAtCp.id != null) {
                            dataBuilder.put("rangeValueId", rangeAtCp.id);
                        }
                        // Find the domain value that shares the same groupBy ancestor as
                        // the range value at the change point. This handles split/dataset
                        // scoping correctly (siblings in the DAG, not ancestors).
                        List<ValueEntity> rangeGroupBy = valueService.getAncestor(rangeAtCp, groupBy);
                        if (!rangeGroupBy.isEmpty()) {
                            List<ValueEntity> domainValues = valueService.getDescendantValues(rangeGroupBy.getFirst(), ed.getDomainNode());
                            if (!domainValues.isEmpty()) {
                                dataBuilder.put("domainvalue", domainValues.getFirst().data);
                            }
                        }
                    }
                    JqValue data = dataBuilder.build();

                    ValueEntity changeValue = new ValueEntity(root.folder, ed, data);
                    changeValue.idx = startingOrdinal + rtrn.size();
                    List<ValueEntity> foundParents = valueService.getAncestor(fingerprintValue, groupBy);
                    if (foundParents.size() == 1) {
                        changeValue.sources = foundParents;
                    }
                    rtrn.add(changeValue);
                }
            }
        } catch (Exception e) {
            Log.errorf(e, "Error in e-divisive calculation for node %s: %s", ed.name, e.getMessage());
        }
        return rtrn;
    }

    @Transactional
    public List<ValueEntity> calculateStdDevAnomalyValues(StdDevAnomaly sd, ValueEntity root, int startingOrdinal) throws IOException {
        List<ValueEntity> rtrn = new ArrayList<>();
        try {
            NodeEntity groupBy = NodeEntity.findById(sd.getGroupByNode().getId());
            List<ValueEntity> fingerprintValues = valueService.getDescendantValues(root, sd.getFingerprintNode());
            String fpFilter = sd.getFingerprintFilter();

            for (int fIdx = 0; fIdx < fingerprintValues.size(); fIdx++) {
                ValueEntity fingerprintValue = fingerprintValues.get(fIdx);
                if (fpFilter != null && !evaluateFingerprintFilter(fpFilter, fingerprintValue.data)) {
                    continue;
                }

                // Get the groupBy ancestor for this fingerprint — this scopes to the
                // specific dataset/split branch. For non-split uploads, this is the root.
                List<ValueEntity> groupByValues = valueService.getAncestor(fingerprintValue, groupBy);
                if (groupByValues.isEmpty()) {
                    continue;
                }
                ValueEntity groupByValue = groupByValues.getFirst();

                // Get the range value from the CURRENT upload that belongs to the same
                // dataset as this fingerprint (shared groupBy ancestor).
                List<ValueEntity> currentRangeValues = valueService.getDescendantValues(groupByValue, sd.getRangeNode());
                if (currentRangeValues.isEmpty()) {
                    continue;
                }

                // Get the domain value from the current upload for the output data
                JqValue currentDomainData = null;
                ValueEntity domainPivot = null;
                if (sd.getDomainNode() != null) {
                    List<ValueEntity> currentDomainValues = valueService.getDescendantValues(groupByValue, sd.getDomainNode());
                    if (!currentDomainValues.isEmpty()) {
                        domainPivot = currentDomainValues.getFirst();
                        currentDomainData = domainPivot.data;
                    }
                }

                // For each range value in the current dataset:
                for (ValueEntity currentRangeValue : currentRangeValues) {
                    Double currentNumeric = currentRangeValue.data != null ? currentRangeValue.data.tryDouble() : null;
                    if (currentNumeric == null) {
                        continue;
                    }

                    // Fetch historical range values for the baseline window.
                    // Use the current upload's domain value as pivot to get preceding values.
                    // Request windowSize + 1 because the current value will be in the result set
                    // (its domain matches the pivot with <=). We skip it by ID below,
                    // leaving windowSize baseline values.
                    List<ValueEntity> historicalValues = valueService.findMatchingFingerprint(
                            sd.getRangeNode(), groupBy, fingerprintValue,
                            sd.getDomainNode(), domainPivot, null,
                            sd.getWindowSize() + 1, 0, true);

                    // Build the numeric series: historical values in chronological order,
                    // with the current value as the last element
                    List<Double> values = new ArrayList<>();
                    for (ValueEntity rv : historicalValues) {
                        // Skip the current value if it appears in the historical set
                        if (rv.getId().equals(currentRangeValue.getId())) {
                            continue;
                        }
                        Double d = rv.data != null ? rv.data.tryDouble() : null;
                        if (d != null) {
                            values.add(d);
                        }
                    }
                    // Append the current value as the last element — this is what gets checked
                    values.add(currentNumeric);

                    // Delegate the math to the pure calculator
                    StdDevAnomalyCalculator.Result result = StdDevAnomalyCalculator.evaluate(
                            values, sd.getWindowSize(), sd.getDeviations(),
                            sd.getDirection(), sd.getMinDataPoints());

                    if (result != null && result.anomaly()) {
                        JqObject.Builder dataBuilder = JqObject.builder();
                        dataBuilder.put("value", result.currentValue());
                        dataBuilder.put("mean", result.mean());
                        dataBuilder.put("stddev", result.stddev());
                        dataBuilder.put("deviations", result.deviations());
                        dataBuilder.put("direction", result.direction());
                        dataBuilder.put("threshold", result.threshold());
                        dataBuilder.put("fingerprint", fingerprintValue.data);
                        if (currentDomainData != null) {
                            dataBuilder.put("domainvalue", currentDomainData);
                        }
                        JqValue data = dataBuilder.build();
                        ValueEntity changeValue = new ValueEntity(root.folder, sd, data);
                        changeValue.idx = startingOrdinal;
                        List<ValueEntity> foundParents = valueService.getAncestor(fingerprintValue, groupBy);
                        if (foundParents.size() == 1) {
                            changeValue.sources = foundParents;
                        }
                        rtrn.add(changeValue);
                    }
                }

                // Stale change cleanup: delete previously persisted StdDev anomalies
                // for this fingerprint at the current upload's domain value.
                // The new results (in rtrn) will replace them when persisted by the caller.
                // This handles: reprocessing, recalculation, and baseline shifts.
                if (currentDomainData != null) {
                    List<ValueEntity> persistedChanges = valueService.findMatchingFingerprint(
                            sd, groupBy, fingerprintValue,
                            sd.getDomainNode(), null, null,
                            -1, -1, true);
                    for (ValueEntity existing : persistedChanges) {
                        JqValue existingDomain = existing.data != null ? existing.data.getField("domainvalue") : JqNull.NULL;
                        if (!existingDomain.isNull() && existingDomain.equals(currentDomainData)) {
                            valueService.delete(existing);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
            if(v.data instanceof JqArray jqArr){
                for(int i=0;i<jqArr.length();i++){
                    JqValue entry = jqArr.get(i);
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
            // JSONata compiled to jq via jjq-jsonata — executes natively on JqValue
            JqProgram program = compileJsonata(node.operation);
            JqValue jqInput = input != null && input.data != null ? input.data : JqNull.NULL;
            JqValue result = program.apply(jqInput);
            if (result == null) {
                result = JqNull.NULL;
            }

            ValueEntity newValue = new ValueEntity();
            newValue.idx = startingOrdinal+1;
            newValue.node = node;
            newValue.data = result;
            newValue.sources = node.sources.stream().filter(n->sourceValues.containsKey(n.name)).map(n -> sourceValues.get(n.name)).collect(Collectors.toList());
            return List.of(newValue);

        } catch (JsonataException e) {
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
    /**
     * Converts a jsonpath expression to a jq expression that collects all
     * matches into an array — equivalent to PostgreSQL's jsonb_path_query_array().
     * For expressions with wildcards ([*] or .*), the jq iterator naturally
     * produces multiple outputs which [...] collects.  For simple scalar paths,
     * uses {@code // empty} to filter null values, producing an empty array
     * when the path doesn't exist — matching jsonb_path_query_array's behavior
     * of returning {@code []} for missing paths.
     */
    public static String jsonpathToJqArray(String jsonpath) {
        String jq = jsonpathToJq(jsonpath);
        if (jq.contains("[]?")) {
            // Has iterators — [...] naturally produces [] when no matches
            return "[" + jq + "]";
        } else {
            // Scalar path — filter null to produce [] for missing paths,
            // matching jsonb_path_query_array behavior
            return "[" + jq + " // empty]";
        }
    }

    public static String jsonpathToJq(String jsonpath) {
        if (jsonpath == null || jsonpath.isEmpty()) return ".";
        String jq = jsonpath;
        if(jq.equals("$")){
            return ".";
        }
        jq = jq.replace("$.",".");
        jq = jq.replace("[*].*", "[]?");
        jq = jq.replace("[*]", "[]?");
        // Replace **{N} recursive descent BEFORE .* — otherwise .* corrupts the **{N} pattern.
        // jq has no direct equivalent of PostgreSQL jsonpath **{N}; use recurse to approximate.
        jq = jq.replaceAll("\\.\\*\\*\\{\\d+\\}", " | recurse");
        // Replace .* (wildcard object values) with []? but only OUTSIDE quoted strings
        // to avoid corrupting quoted keys like ."mw********.lab"
        jq = replaceOutsideQuotes(jq, ".*", "[]?");
        // Replace PostgreSQL jsonpath methods with jq equivalents
        jq = jq.replace(".size()", " | length");
        jq = jq.replace(".keyvalue()", " | to_entries[] | ");
        // Convert PostgreSQL jsonpath filter expressions to JQ select()
        if (jq.contains("?")) {
            jq = convertJsonpathFilters(jq);
        }
        return jq;
    }

    /**
     * Replaces occurrences of {@code target} with {@code replacement} only when
     * outside double-quoted strings. This prevents corrupting quoted keys that
     * happen to contain the target pattern (e.g., {@code ."mw********.lab"}).
     */
    private static String replaceOutsideQuotes(String input, String target, String replacement) {
        StringBuilder result = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '"' && (i == 0 || input.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                result.append(input.charAt(i));
            } else if (!inQuotes && input.startsWith(target, i)) {
                result.append(replacement);
                i += target.length() - 1;
            } else {
                result.append(input.charAt(i));
            }
        }
        return result.toString();
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
            // Convert @."special" → .["special"] (quoted field access on current item)
            filterBody = filterBody.replaceAll("@\\.\"([^\"]+)\"", ".[\"$1\"]");
            // Convert @.field → .field (field access on current item)
            filterBody = filterBody.replace("@.", ".");
            // Convert bare @ → . (current item reference without field access)
            // Only replace @ when followed by a space, ), or operator — not inside
            // quoted key names like ["@name"]
            filterBody = filterBody.replaceAll("@(?=[\\s)&|,]|$)", ".");
            // Convert && → and (jsonpath logical AND → jq logical AND)
            filterBody = filterBody.replace("&&", "and");
            // Convert || → or (jsonpath logical OR → jq logical OR)
            filterBody = filterBody.replace("||", "or");
            // Handle like_regex with optional flags → test()
            // like_regex "pattern" flag "i" → test("pattern"; "i")
            // like_regex "pattern" → test("pattern")
            if (filterBody.contains("like_regex")) {
                filterBody = filterBody.replaceAll(
                        "(\\S+)\\s+like_regex\\s+\"([^\"]+)\"\\s+flag\\s+\"([^\"]+)\"",
                        "($1 | test(\"$2\"; \"$3\"))");
                filterBody = filterBody.replaceAll(
                        "(\\S+)\\s+like_regex\\s+\"([^\"]+)\"",
                        "($1 | test(\"$2\"))");
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
        List<JqValue> input = JsNode.createParameters(node.operation, sourceValues, node.sources.size());
        try(Context context = Context.newBuilder("js").engine(Engine.newBuilder("js").option("engine.WarnInterpreterOnly", "false").build())
                .allowExperimentalOptions(true)
                .option("js.foreign-object-prototype", "true")
                .option("js.global-property", "true")
//                .out(out)
//                .err(out)
                .build()){
            context.enter();
            context.getBindings("js").putMember("isInstanceLike", new ProxyJqObject.InstanceCheck());
            context.eval("js",
                    """
                    Object.defineProperty(Object,Symbol.hasInstance, {
                      value: function myinstanceof(obj) {
                        return isInstanceLike(obj);
                      }
                    });
                    """);
            // Bind input parameters as GraalVM proxy objects — JqValue passed directly
            for(int i=0; i<input.size(); i++) {
                context.getBindings("js").putMember("__obj" + i, ProxyJq.wrap(input.get(i)));
            }
            StringBuilder jsCode = new StringBuilder();
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
                        JqValue data = Util.convertToJqValue(resolvedValue);

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

    @Transactional
    public List<ValueEntity> calculateFpValues(FingerprintNode node, Map<String, ValueEntity> sourceValues, int startingOrdinal) throws IOException {
        JqObject.Builder fpBuilder = JqObject.builder();
        TreeMap<String, JqValue> sorted = new TreeMap<>();
        for (NodeEntity source : node.sources) {
            if (sourceValues.containsKey(source.name)) {
                sorted.put(source.name, sourceValues.get(source.name).data);
            }
        }
        sorted.forEach(fpBuilder::put);
        ValueEntity newValue = new ValueEntity();
        newValue.idx = startingOrdinal+1;
        newValue.node = node;
        newValue.data = fpBuilder.build();
        newValue.sources = node.sources.stream().filter(n->sourceValues.containsKey(n.name)).map(n -> sourceValues.get(n.name)).collect(Collectors.toList());
        return List.of(newValue);
    }
    public boolean evaluateFingerprintFilter(String filter, JqValue fingerprint) {
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
                context.getBindings("js").putMember("__fp", ProxyJq.wrap(fingerprint));
                String jsCode = "const __filter = " + filter + ";\n" +
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
        // Source data is already JqValue — no conversion needed
        List<JqValue> sourceData = new ArrayList<>();
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
            List<JqValue> results;
            if (isNullInput) {
                // No JacksonConverter round-trip — source data is already JqValue
                results = program.applyNullInput(sourceData);
            } else if (isSlurp) {
                results = program.applyAll((JqValue) JqArray.ofTrusted(sourceData));
            } else {
                JqValue input = sourceData.isEmpty() ? JqNull.NULL : sourceData.getFirst();
                results = program.applyAll(input);
            }
            int order = startingOrdinal;
            for (JqValue jqResult : results) {
                if (!jqResult.isNull()) {
                    ValueEntity newValue = new ValueEntity();
                    newValue.idx = order++;
                    newValue.node = node;
                    newValue.data = jqResult;
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
