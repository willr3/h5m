package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.api.Value;
import io.hyperfoil.tools.h5m.api.svc.ValueServiceInterface;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.mapper.CycleAvoidingContext;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.queue.KahnDagSort;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.NotFoundException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hibernate.Session;
import org.hibernate.query.NativeQuery;

import java.io.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class ValueService implements ValueServiceInterface {

    @Inject
    EntityManager em;

//    @Inject
//    @Named("duckdb")
//    AgroalDataSource duckDatasource;

    @Inject
    ApiMapper apiMapper;

    @ConfigProperty(name="quarkus.datasource.db-kind")
    String dbKind;
    @Inject
    NodeService nodeService;

    @Override
    @Transactional
    public void purgeValues(){
        em.createNativeQuery("delete from Value").executeUpdate();

    }

    @Transactional
    public ValueEntity create(ValueEntity value){
        if(!value.isPersistent()){
            ValueEntity merged = em.merge(value);
            em.flush();
            value.id = merged.id;
            return merged;
        }else{
            value.persist();
        }
        return value;
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<ValueEntity> getDependentValues(ValueEntity v){
        // Query IDs only, then load via findMultiple() to hit 2LC
        List<Number> ids = em.createNativeQuery(
                "SELECT DISTINCT child_id FROM value_edge WHERE parent_id = :parentId")
                .setParameter("parentId", v.id).getResultList();
        List<Long> longIds = ids.stream().map(Number::longValue).toList();
        return em.unwrap(Session.class).findMultiple(ValueEntity.class, longIds);
    }

    @Transactional
    public long getParentCount(ValueEntity value){
        return EdgeQueries.getParentCount(em, "value_edge", value.id);
    }

    @Transactional
    public Map<Long, Long> getParentCounts(List<Long> childIds){
        return EdgeQueries.getParentCounts(em, "value_edge", childIds);
    }

    @Transactional
    public void delete(ValueEntity value){
        if(value.id != null && ValueEntity.findById(value.id) != null){
            List<ValueEntity> dependents = getDependentValues(value);
            for(ValueEntity dependent : dependents){
                long parentCount = EdgeQueries.getParentCount(em, "value_edge", dependent.id);
                if(parentCount <= 1){
                    delete(dependent);
                }
            }
            deleteValueAndEdges(value.id);
        }
    }

    @Transactional
    public ValueEntity byId(Long id){
        return ValueEntity.findById(id);
    }

    /**
     * Returns the JSON data for a value, eagerly loaded within a transaction.
     * Use this for REST endpoints to avoid LazyInitializationException.
     */
    @Transactional
    public JqValue getValueData(Long id) {
        ValueEntity value = ValueEntity.findById(id);
        if (value == null) return null;
        // Access data within the transaction to initialize the lazy proxy
        return value.data;
    }

    @Transactional
    public ValueEntity byPath(String path){
        return ValueEntity.find("path",path).firstResult();
    }

    /**
     * returns the values that depend on the root value as a source somewhere up the hierarchy.
     * @param root
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<ValueEntity> getDescendantValues(ValueEntity root){
        // Query IDs only, then load via findMultiple() to hit 2LC
        List<Number> ids = em.createNativeQuery(
                """
                WITH RECURSIVE sourceRecursive (v_id) AS (
                    SELECT ve.child_id from value_edge ve where ve.parent_id = :rootId
                    UNION ALL
                    SELECT ve.child_id from value_edge ve JOIN sourceRecursive sr
                    ON ve.parent_id = sr.v_id
                )
                SELECT distinct v.id FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id
                """
        ).setParameter("rootId", root.id).getResultList();
        List<Long> longIds = ids.stream().map(Number::longValue).toList();
        return em.unwrap(Session.class).findMultiple(ValueEntity.class, longIds);
    }


    public List<ValueEntity> getDirectDescendantValues(ValueEntity root, NodeEntity node){
        List<ValueEntity> rtrn = new ArrayList<>();
        rtrn.addAll(em.createNativeQuery(
        """
           SELECT * from Value v RIGHT JOIN value_edge ve ON ve.child_id = v.id WHERE v.node_id = :nodeId AND ve.parent_id = :rootId
           """
        ).setParameter("rootId", root.id).setParameter("nodeId",node.id).getResultList());
        return rtrn;
    }

    /*
     * Finds the values for an ancestor or relative node Source where a relative created the expected fingerprint value
     */
    @Transactional
    public List<ValueEntity> findMatchingFingerprint(NodeEntity source, ValueEntity fingerprint){
        source = NodeEntity.findById(source.id);
        return findMatchingFingerprint(source,source.group.root,fingerprint,null,null,-1,-1,true);
    }
    /*
     * Finds the values for a relative node Source where a descendant created the expected fingerprint value, now it works on siblings and cousins
     * we want this to also find sibling and cousin values but that probably requires another traversal
     * sorting by a value is useful if that value is our timestamp but we also need that value
     */
    @Transactional
    public List<ValueEntity> findMatchingFingerprint_unused(NodeEntity source, ValueEntity fingerprint, NodeEntity sort){
        List<ValueEntity> rtrn = new ArrayList<>(em.createNativeQuery(
            switch(dbKind){
                case "sqlite"->
                    """
                    with recursive ancestor(vid) as (
                        select v.id as vid 
                            from value v where v.node_id = :nodeId and v.data = :data
                        union 
                        select v.id as vid 
                            from value v join value_edge ve on v.id = ve.parent_id join ancestor a on a.vid = ve.child_id
                    ),
                    sorter(vid,sortable) as (
                        select v.id as vid,v.data as sortable 
                            from value v where v.node_id = :sortId
                        union
                        select v.id as vid, s.sortable as sortable
                            from value v join value_edge ve on v.id = ve.parent_id join sorter s on s.vid = ve.child_id
                    ),
                    descendant(vid,sortable) as (
                       select v.id as vid, s.sortable as sortable
                         from value v join sorter s on v.id = s.vid join ancestor a on v.id = a.vid join node n on n.id = v.node_id where n.type = 'root' --only the root values from ancestor with sorter
                       union
                       select v.id as vid, d.sortable as sortable
                             from value v join value_edge ve on v.id = ve.child_id join descendant d on d.vid = ve.parent_id
                    )                        
                    select * from value v join descendant d on v.id=d.vid where v.node_id=:sourceId order by sortable asc;
                    """;
                case "postgresql"->
                    """
                    with recursive ancestor(vid) as (
                        select v.id as vid 
                            from value v where v.node_id = :nodeId and v.data = cast( :data as jsonb)
                        union 
                        select v.id as vid 
                            from value v join value_edge ve on v.id = ve.parent_id join ancestor a on a.vid = ve.child_id
                    ),
                    sorter(vid,sortable) as (
                        select v.id as vid,v.data as sortable 
                            from value v where v.node_id = :sortId
                        union
                        select v.id as vid, s.sortable as sortable
                            from value v join value_edge ve on v.id = ve.parent_id join sorter s on s.vid = ve.child_id
                    ),
                    descendant(vid,sortable) as (
                       select v.id as vid, s.sortable as sortable
                         from value v join sorter s on v.id = s.vid join ancestor a on v.id = a.vid join node n on n.id = v.node_id where n.type = 'root' --only the root values from ancestor with sorter
                       union
                       select v.id as vid, d.sortable as sortable
                             from value v join value_edge ve on v.id = ve.child_id join descendant d on d.vid = ve.parent_id
                    )                        
                    select * from value v join descendant d on v.id=d.vid where v.node_id=:sourceId order by sortable asc;                    
                    """;
                default -> "";
            }, ValueEntity.class)
                                                   .setParameter("nodeId", fingerprint.node.id)
                                                   .setParameter("data", fingerprint.data.toString())
                                                   .setParameter("sourceId", source.id)
                                                   .setParameter("sortId",sort.id)
                                                   .getResultList());
        return rtrn;
    }

    //

    //this is to support getting the necessary values for change detection across all values
    @Transactional
    public List<ValueEntity> findMatchingFingerprint(NodeEntity source, ValueEntity fingerprint, NodeEntity sort){
        return findMatchingFingerprint(source,source.group.root,fingerprint,sort,null,-1,-1,true);
    }
    public List<ValueEntity> findMatchingFingerprint(NodeEntity source, NodeEntity groupBy, ValueEntity fingerprint, NodeEntity sort){
        return findMatchingFingerprint(source,groupBy,fingerprint,sort,null,-1,-1,true);
    }

    //TODO I want a way to specify a required ancestor value for the resulting values
    @Transactional
    public List<ValueEntity> findMatchingFingerprint(NodeEntity rangeNode, NodeEntity groupBy, ValueEntity fingerprint, NodeEntity domainNode, ValueEntity domainValue,int limit,int offset,boolean preceedingValues){
        return findMatchingFingerprint(rangeNode,groupBy,fingerprint,domainNode,domainValue,null,limit,offset,preceedingValues);
    }

    @Transactional
    public List<ValueEntity> findMatchingFingerprint(NodeEntity rangeNode, NodeEntity groupBy, ValueEntity fingerprint, NodeEntity domainNode, ValueEntity domainValue, ValueEntity ancestorValue,int limit,int offset,boolean preceedingValues){

        assert rangeNode!=null && groupBy!=null && fingerprint!=null;
        String sql = "";
        if(ancestorValue!=null){
            sql +=
                """
                with recursive valueDescendants(vid) as (
                    select ve.child_id as vid from value_edge ve where ve.parent_id = :ancestorValueId
                    union
                    select ve.child_id as vid from value_edge ve join valueDescendants vd on vd.vid = ve.parent_id
                ),
                """;
        }
        sql = sql + switch (dbKind){
            case "sqlite"->
                    """
                        ANCESTOR_PREFIX ancestor(vid) as (
                            select v.id as vid
                                from value v where v.node_id = :nodeId and v.data = :fingerprint VALUE_ANCESTOR_CRITERIA
                            union
                            select v.id as vid
                                from value v join value_edge ve on v.id = ve.parent_id join ancestor a on a.vid = ve.child_id
                        ),
                    """;
            case "postgresql"->
                    """
                    ANCESTOR_PREFIX ancestor(vid) as (
                        select v.id as vid
                            from value v where v.node_id = :nodeId and v.data = cast( :fingerprint as jsonb) VALUE_ANCESTOR_CRITERIA
                        union 
                        select v.id as vid 
                            from value v join value_edge ve on v.id = ve.parent_id join ancestor a on a.vid = ve.child_id
                    ),
                    """;
            default -> "";
        };
        sql = sql
                .replace("ANCESTOR_PREFIX",ancestorValue==null?"with recursive":"")
                .replace("VALUE_ANCESTOR_CRITERIA",ancestorValue==null?"":" and exists ( select 1 from valueDescendants where vid = v.id)");

        if(domainValue!=null || domainNode!=null) { //we have a sortable domain value
            String domainValueComp = switch (dbKind){
                case "sqlite"-> "and v.data GTLT :domain";
                case "postgresql"-> "and v.data GTLT cast( :domain as jsonb)";
                default -> "";
            };
            sql += switch (dbKind) {
                case "sqlite" -> """
                        sorter(vid,sortable) as (
                            select v.id as vid,v.data as sortable
                                from value v where v.node_id = :sortId DOMAIN_VALUE_COMP
                            union
                            select v.id as vid, s.sortable as sortable
                                from value v join value_edge ve on v.id = ve.parent_id join sorter s on s.vid = ve.child_id
                        ),
                        descendant(vid,sortable) as (
                           select v.id as vid, s.sortable as sortable
                             from value v join sorter s on v.id = s.vid join ancestor a on v.id = a.vid
                             where v.node_id = :groupById --limit descendants to values from the grouping node
                           union
                           select v.id as vid, d.sortable as sortable
                                 from value v join value_edge ve on v.id = ve.child_id join descendant d on d.vid = ve.parent_id
                        )
                        select v.id from value v join descendant d on v.id=d.vid 
                            where v.node_id=:sourceId order by sortable ORDER_DIRECTION
                        """;
                case "postgresql" -> """
                        sorter(vid,sortable) as (
                            select v.id as vid,v.data as sortable
                                from value v where v.node_id = :sortId DOMAIN_VALUE_COMP
                            union
                            select v.id as vid, s.sortable as sortable
                                from value v join value_edge ve on v.id = ve.parent_id join sorter s on s.vid = ve.child_id
                        ),
                        descendant(vid,sortable) as (
                           select v.id as vid, s.sortable as sortable
                             from value v join sorter s on v.id = s.vid join ancestor a on v.id = a.vid
                             where v.node_id = :groupById --limit descendants to values from the grouping node
                           union
                           select v.id as vid, d.sortable as sortable
                                 from value v join value_edge ve on v.id = ve.child_id join descendant d on d.vid = ve.parent_id
                        )
                        select v.id from value v join descendant d on v.id=d.vid
                            where v.node_id=:sourceId order by sortable ORDER_DIRECTION
                        """;
                default -> "";
            };
            sql = sql.replace("DOMAIN_VALUE_COMP",domainValue != null ? domainValueComp : "");
        }else{
            //sorting by created_at
            // No domain sorting — order by created_at. Both dialects share the same SQL.
            sql+= """
                        descendant(vid) as (
                           select v.id as vid
                             from value v join ancestor a on v.id = a.vid
                             where v.node_id = :groupById --limit descendants to values from the grouping node
                           union
                           select v.id as vid
                                 from value v join value_edge ve on v.id = ve.child_id join descendant d on d.vid = ve.parent_id
                        )
                        select v.id from value v join descendant d on v.id=d.vid
                            where v.node_id=:sourceId order by created_at ORDER_DIRECTION
                        """;
        }
        sql = sql
                .replace("GTLT", preceedingValues ? "<=" : ">=") //TODO I think these should be <= and >= to include current sample
                .replace("ORDER_DIRECTION", preceedingValues ? "desc" : "asc");
        if(offset > 0){
            sql+=" offset :offset";
        }
        if(limit > 0){
            sql+=" limit :limit";
        }
        // Query IDs only (skip data column transfer), then load entities via
        // findMultiple() which hits the 2LC. This avoids re-fetching the full
        // JSONB/BYTEA data for values already cached from prior processing.
        @SuppressWarnings("unchecked")
        var query = (NativeQuery<Number>) em.createNativeQuery(sql);
        query
                .setParameter("nodeId", fingerprint.node.id)
                .setParameter("fingerprint", fingerprint.data.toString())
                .setParameter("sourceId", rangeNode.id)
                .setParameter("groupById",groupBy.id);
        if(ancestorValue!=null){
            query.setParameter("ancestorValueId",ancestorValue.id);
        }
        if(domainValue!=null){
            query
                .setParameter("sortId",domainValue.node.id)
                .setParameter("domain",domainValue.data.toString());
        }else if (domainNode!=null){
            query.setParameter("sortId",domainNode.id);
        }
        if(offset > 0){
            query.setParameter("offset",offset);
        }
        if(limit > 0){
            query.setParameter("limit",limit);
        }
        List<Long> orderedIds = query.getResultList().stream()
                .map(Number::longValue).toList();
        if (orderedIds.isEmpty()) {
            return List.of();
        }
        // Load entities from 2LC (cache hit) or DB (cache miss, batched)
        Session session = em.unwrap(Session.class);
        Map<Long, ValueEntity> byId = new HashMap<>();
        for (ValueEntity ve : session.findMultiple(ValueEntity.class, orderedIds)) {
            byId.put(ve.getId(), ve);
        }
        // Preserve the SQL ordering (findMultiple does not guarantee order)
        List<ValueEntity> rtrn = new ArrayList<>(orderedIds.size());
        for (Long id : orderedIds) {
            ValueEntity ve = byId.get(id);
            if (ve != null) rtrn.add(ve);
        }
        if(preceedingValues){
            rtrn = rtrn.reversed();
        }
        return rtrn;
    }

    @SuppressWarnings("unchecked")
    public List<ValueEntity> getAncestor(ValueEntity value, NodeEntity node){
        // Query IDs only, then load via findMultiple() to hit 2LC
        List<Number> ids = em.createNativeQuery("""
            with recursive ancestor(vid) as (
                select v.id as vid 
                    from value v where v.id = :valueId
                union 
                select v.id as vid 
                    from value v join value_edge ve on v.id = ve.parent_id join ancestor a on a.vid = ve.child_id
            )
            select v.id from value v join ancestor a on v.id = a.vid where v.node_id = :nodeId
        """).setParameter("nodeId", node.id).setParameter("valueId",value.id).getResultList();
        List<Long> longIds = ids.stream().map(Number::longValue).toList();
        return em.unwrap(Session.class).findMultiple(ValueEntity.class, longIds);
    }




    /**
     * Returns grouped values for a node, optionally filtered to specific node IDs.
     * One row per upload, with keys being node names.
     *
     * @param nodeId The root node ID to start the value DAG traversal from.
     * @param filterNodeIds Optional list of node IDs to include. If null, all nodes are included.
     * @return A list of JSON objects, one per upload.
     */
    @Override
    @Transactional
    public List<JqValue> getGroupedValues(Long nodeId, List<Long> filterNodeIds){
        return getGroupedValues(nodeId,filterNodeIds,null,null);
    }

    @Override
    @Transactional
    public List<JqValue> getGroupedValues(Long nodeId){
        return getGroupedValues(nodeId, null);
    }


    @Override
    @Transactional
    public List<JqValue> getGroupedValues(Long nodeId, List<Long> filterNodeIds, Map<Long,JqValue> fingerprints, Long sortByNodeId) {
        String nodeFilter = filterNodeIds != null && !filterNodeIds.isEmpty() ? "node_id in (:nodeIds)" : "";
        String sortCte = sortByNodeId != null ? switch (dbKind) {
            case "sqlite"     ->
                    """
                    root_sort as (
                    select root_id, 
                    min(case when typeof(json_extract(data,'$')) in ('integer','real') then cast(data as real) end) as sort_num,
                    min(data) as sort_txt
                    from tree where node_id = :sortNodeId group by root_id),
                    """;
            case "postgresql" ->
                    """
                    root_sort as (
                    select
                        root_id, 
                        min(case when jsonb_typeof(data) = 'number' then (data::text)::numeric end) as sort_num,
                        min(data::text) as sort_txt from tree where node_id = :sortNodeId group by root_id),
                    """;
            default -> "";
        } : "";
        String sortJoin    = sortByNodeId != null ? "left join root_sort rs on b.root_id = rs.root_id" : "";
        String sortGroupBy = sortByNodeId != null ? ", rs.sort_num, rs.sort_txt" : "";
        String sortOrder   = sortByNodeId != null ? "order by rs.sort_num asc nulls last, rs.sort_txt asc" : "";
        String fingerPrintWhere = "";
        List<Long> fingerprintIds = fingerprints!=null ? new ArrayList<>(fingerprints.keySet()) : Collections.emptyList();
        if(!fingerprintIds.isEmpty()){
            for(int idx=0;idx<fingerprintIds.size();idx++){
                Long id = fingerprintIds.get(idx);
                JqValue fingerprint = fingerprints.get(id);
                if(!fingerPrintWhere.isEmpty()){
                    fingerPrintWhere+=" and ";
                }
                fingerPrintWhere+= " root_id in ( select ft.root_id from tree ft where ft.node_id = "+id+" and ft.data = "+
                        switch(dbKind){
                            case "sqlite"->":data_"+idx;
                            case "postgresql"->"cast( :data_"+idx+" as jsonb)";
                            default -> "";
                        }+") ";
            }
        }

        String filter = !nodeFilter.isEmpty() || !fingerPrintWhere.isEmpty() ? "where "+ nodeFilter + (!nodeFilter.isEmpty() && !fingerPrintWhere.isEmpty() ? " and " : "" ) + fingerPrintWhere : "";

        var query = em.unwrap(Session.class).createNativeQuery(
                switch (dbKind) {
                    case "sqlite" ->
                            """
                            with recursive tree(id,node_id,root_id,idx,data) as (
                                select v.id,v.node_id,ve.parent_id as root_id,v.idx,v.data
                                    from value_edge ve left join value v on ve.child_id = v.id
                                    where ve.parent_id in (select id from value where node_id = :nodeId)
                                union
                                select v.id,v.node_id,t.root_id,v.idx,v.data
                                    from value v join value_edge ve on v.id = ve.child_id join tree t on ve.parent_id = t.id
                            ),
                            SORT_CTE
                            bynode as (
                                select node_id,root_id,json_group_array(json(data)) as data
                                    from tree NODE_FILTER group by node_id,root_id order by idx
                            )
                            select json_group_object(n.name,json((case when json_array_length(b.data) > 1 then b.data else b.data->0 end))) as data
                                from bynode b join node n on b.node_id = n.id SORT_JOIN group by b.root_id SORT_GROUPBY SORT_ORDER;
                            """
                                    .replace("NODE_FILTER", filter).replace("SORT_CTE", sortCte)
                                    .replace("SORT_JOIN", sortJoin).replace("SORT_GROUPBY", sortGroupBy).replace("SORT_ORDER", sortOrder);
                    case "postgresql" ->
                            """
                            with recursive tree(id,node_id,root_id,idx,data) as (
                                select v.id,v.node_id,ve.parent_id as root_id,v.idx,v.data
                                    from value_edge ve left join value v on ve.child_id = v.id
                                    where ve.parent_id in (select id from value where node_id = :nodeId)
                                union
                                select v.id,v.node_id,t.root_id,v.idx,v.data
                                    from value v join value_edge ve on v.id = ve.child_id join tree t on ve.parent_id = t.id
                            ),
                            SORT_CTE
                            bynode as (
                                select node_id,root_id,jsonb_agg(to_jsonb(data)) as data
                                    from tree NODE_FILTER group by node_id,root_id,idx order by idx
                            )
                            select jsonb_object_agg(n.name,to_jsonb((case when jsonb_array_length(b.data) > 1 then b.data else b.data->0 end))) as data
                                from bynode b join node n on b.node_id = n.id SORT_JOIN group by b.root_id SORT_GROUPBY SORT_ORDER;
                            """
                                    .replace("NODE_FILTER", filter).replace("SORT_CTE", sortCte)
                                    .replace("SORT_JOIN", sortJoin).replace("SORT_GROUPBY", sortGroupBy).replace("SORT_ORDER", sortOrder);
                    default -> "";
                }, String.class
        ).setParameter("nodeId", nodeId);
        if (filterNodeIds != null && !filterNodeIds.isEmpty()) query.setParameter("nodeIds", filterNodeIds);
        if (sortByNodeId != null)  query.setParameter("sortNodeId", sortByNodeId);
        if(!fingerprintIds.isEmpty()){
            for(int idx=0;idx<fingerprintIds.size();idx++){
                Long id = fingerprintIds.get(idx);
                JqValue fingerprint = fingerprints.get(id);
                query.setParameter("data_"+idx,fingerprint.toString());
            }
        }
        return query.getResultList().stream().map(JqValues::parse).toList();
    }

    /**
     * get all value that have a value from node as an ancestor
     * @param nodeId
     * @return
     */
    @Transactional
    public List<Value> getNodeDescendantValues(Long nodeId){
        CycleAvoidingContext cycleContext = new CycleAvoidingContext();
        return em.unwrap(Session.class).createNativeQuery(
                """
                WITH RECURSIVE sourceRecursive (v_id) AS (
                     SELECT ve.child_id from value_edge ve where ve.parent_id in (select v.id from value v where v.node_id = :nodeId)
                     UNION ALL
                     SELECT ve.child_id from value_edge ve JOIN sourceRecursive sr ON ve.parent_id = sr.v_id
                )
                SELECT distinct * FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id
                """, ValueEntity.class
        ).setParameter("nodeId",nodeId).getResultStream().map(entity -> apiMapper.toValue(entity, cycleContext)).toList();
    }
    /**
     * returns the values that depend on the root value somewhere up the hierarchy and come from the specified node
     * @param root
     * @param node
     * @return
     */
    @Transactional
    @SuppressWarnings("unchecked")
    public List<ValueEntity> getDescendantValues(ValueEntity root, NodeEntity node){
        // Query only IDs (skip JSONB data), then batch-load via findMultiple() to hit 2LC.
        // findMultiple() issues a single batched query for any cache misses.
        List<Number> ids = em.createNativeQuery(
                """
                WITH RECURSIVE sourceRecursive (v_id) AS (
                    SELECT ve.child_id from value_edge ve where ve.parent_id = :rootId
                    UNION ALL
                    SELECT ve.child_id from value_edge ve JOIN sourceRecursive sr
                    ON ve.parent_id = sr.v_id
                )
                SELECT distinct v.id FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id WHERE v.node_id = :nodeId
                """
        ).setParameter("rootId", root.id).setParameter("nodeId",node.id).getResultList();
        List<Long> longIds = ids.stream().map(Number::longValue).toList();
        return em.unwrap(Session.class).findMultiple(ValueEntity.class, longIds);
    }

    //get the values from node that descend from root with the associated value source path (nodeId and index for each source value)
    @Transactional
    public Map<String, ValueEntity> getDescendantValueByPath(ValueEntity root, NodeEntity node){
        List<ValueEntity> found = getDescendantValues(root,node);
        if (!found.isEmpty()) {
            // Re-fetch with sources eagerly loaded in a single query instead of N+1 lazy inits
            found = em.createQuery(
                    "SELECT DISTINCT v FROM value v LEFT JOIN FETCH v.sources WHERE v IN :values",
                    ValueEntity.class
            ).setParameter("values", found).getResultList();
        }
        return found.stream().collect(Collectors.toMap(ValueEntity::getPath,v->v));
    }

    /**
     * Batch-fetch descendant values for multiple nodes in a single recursive CTE query.
     * Replaces N separate getDescendantValues(root, node) calls with one query.
     *
     * Uses an ID-only native query (skips JSONB data column transfer) then loads
     * entities via em.find() which hits the Hibernate 2LC for cached values.
     */
    @Transactional
    @SuppressWarnings("unchecked")
    public Map<Long, List<ValueEntity>> getDescendantValuesByNodes(ValueEntity root, List<NodeEntity> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return Collections.emptyMap();
        }
        List<Long> nodeIds = new ArrayList<>(nodes.size());
        for (int i = 0, size = nodes.size(); i < size; i++) {
            nodeIds.add(nodes.get(i).getId());
        }
        // Query only IDs + node_id (skip JSONB data column transfer).
        // Batch-load entities via findMultiple() which hits the 2LC and issues
        // a single batched query for any cache misses.
        List<Object[]> rows = em.createNativeQuery("""
                WITH RECURSIVE sourceRecursive (v_id) AS (
                    SELECT ve.child_id from value_edge ve where ve.parent_id = :rootId
                    UNION ALL
                    SELECT ve.child_id from value_edge ve JOIN sourceRecursive sr ON ve.parent_id = sr.v_id
                )
                SELECT distinct v.id, v.node_id, v.idx FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id WHERE v.node_id IN (:nodeIds)
                ORDER BY v.idx asc
                """)
                                  .setParameter("rootId", root.id)
                                  .setParameter("nodeIds", nodeIds)
                                  .getResultList();
        // Collect all value IDs and their node mappings
        List<Long> valueIds = new ArrayList<>(rows.size());
        Map<Long, Long> valueToNode = new LinkedHashMap<>();
        for (Object[] row : rows) {
            long valueId = ((Number) row[0]).longValue();
            long nodeId = ((Number) row[1]).longValue();
            valueIds.add(valueId);
            valueToNode.put(valueId, nodeId);
        }
        // Batch-load all entities in one call
        List<ValueEntity> loaded = em.unwrap(Session.class).findMultiple(ValueEntity.class, valueIds);
        Map<Long, List<ValueEntity>> result = new HashMap<>();
        for (ValueEntity v : loaded) {
            if (v != null) {
                Long nodeId = valueToNode.get(v.id);
                result.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(v);
            }
        }
        return result;
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<ValueEntity> getValues(NodeEntity node){
        // Query IDs only, then load via findMultiple() to hit 2LC
        List<Number> ids = em.createNativeQuery("SELECT id FROM value WHERE node_id = :nodeId")
                .setParameter("nodeId", node.id).getResultList();
        List<Long> longIds = ids.stream().map(Number::longValue).toList();
        return em.unwrap(Session.class).findMultiple(ValueEntity.class, longIds);
    }

    @Override
    @Transactional
    public List<Value> getNodeValues(Long nodeId){
        CycleAvoidingContext cycleContext = new CycleAvoidingContext();
        List<ValueEntity> entities = ValueEntity.find("node.id", nodeId).list();
        return entities.stream().map(entity -> apiMapper.toValue(entity, cycleContext)).toList();
    }

    @Override
    @Transactional
    public List<JqValue> getLabelValues(Long folderId, Long groupByNodeId, List<Long> nodeIds, Long sortByNodeId) {
        FolderEntity folder = FolderEntity.findById(folderId);
        if (folder == null) {
            throw new  NotFoundException("Folder not found: " + folderId);
        }
        Long rootNodeId=folder.group.root.id;
        List<Long> filterNodeIds = new ArrayList<>();
        if(nodeIds != null ){filterNodeIds.addAll(nodeIds);}
        if(groupByNodeId != null){
            filterNodeIds.add(groupByNodeId);
        }
        if(sortByNodeId != null){
            filterNodeIds.add(sortByNodeId);
        }
        return getGroupedValues(rootNodeId, filterNodeIds.isEmpty() ? null : filterNodeIds, null, sortByNodeId);

    }


    @Transactional
    public int deleteDescendantValues(ValueEntity root, NodeEntity node){
        List<ValueEntity> descendants = getDescendantValues(root,node);
        Set<Long> deletionSet = new HashSet<>();
        deletionSet.add(root.id);
        descendants.forEach(d -> deletionSet.add(d.id));
        descendants = KahnDagSort.sort(descendants,v->v.getSources()).reversed();
        int deleted = 0;
        for(ValueEntity v : descendants){
            if(!hasExternalParent(v, deletionSet)){
                deleteValueAndEdges(v.id);
                deleted++;
            }
        }
        return deleted;
    }

    @Transactional
    public int purge(ValueEntity root){
        if(root.node instanceof RootNode){
            return 0;//don't want to support deleting uploads just yet
        }
        List<ValueEntity> descendants = getDescendantValues(root);
        Set<Long> purgeSet = new HashSet<>();
        purgeSet.add(root.id);
        descendants.forEach(d -> purgeSet.add(d.id));
        descendants = KahnDagSort.sort(descendants,v->v.getSources()).reversed();
        int deleted = 0;
        for(ValueEntity d : descendants){
            if(!hasExternalParent(d, purgeSet)){
                deleteValueAndEdges(d.id);
                deleted++;
            }
        }
        deleteValueAndEdges(root.id);
        return 1 + deleted;
    }

    private void deleteValueAndEdges(long valueId) {
        EdgeQueries.deleteParentEdges(em, "value_edge", valueId);
        EdgeQueries.deleteChildEdges(em, "value_edge", valueId);
        em.createNativeQuery("DELETE FROM value WHERE id = :id")
                .setParameter("id", valueId).executeUpdate();
    }

    private boolean hasExternalParent(ValueEntity value, Set<Long> deletionSet){
        List<?> externalParents = em.createNativeQuery(
            "SELECT 1 FROM value_edge WHERE child_id = :childId AND parent_id NOT IN (:deletionSet)"
        ).setParameter("childId", value.id)
         .setParameter("deletionSet", deletionSet)
         .setMaxResults(1)
         .getResultList();
        return !externalParents.isEmpty();
    }

    //TODO getHash(ValueEntity value) to see if a new value is different than the persisted one
    public String getHash(ValueEntity value) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try(
            DigestOutputStream out = new DigestOutputStream(OutputStream.nullOutputStream(), md);
            ObjectOutputStream oo = new ObjectOutputStream(out);) {
            oo.writeObject(value.data);
        } catch (FileNotFoundException e) {
            return null;//TODO handle error for missing value
        } catch (IOException e) {
            return null;//TODO handle IO Error?
        }
        //String fx = "%0" + (md.getDigestLength()*2) + "x";
        //return String.format(fx, new BigInteger(1, md.digest()));
        return HexFormat.of().formatHex(md.digest());

    }

    /**
     * Nulls out value.data for ephemeral nodes scoped to descendants of the
     * given root value. Called after upload processing completes to reclaim storage.
     *
     * Ephemeral cleanup happens in two places:
     * 1. At upload completion (this method) — nullifies intermediate values
     *    created during the upload, resolved inline based on current graph structure.
     * 2. At node creation (NodeService.nullifyEphemeralSources) — immediately
     *    nullifies existing historical data for source nodes that become intermediate
     *    when a child node is added.
     *
     * Handles both explicit DISCARD and AUTO nodes:
     * - DISCARD: always nullified (user explicitly wants data discarded)
     * - AUTO with non-detection children: nullified (intermediate node, system decides)
     * - AUTO without children (leaf): kept
     * - KEEP: never nullified (user explicitly wants data kept)
     *
     * Root and detection nodes are excluded as a safety net.
     * Value rows and edges are always preserved for ancestry queries.
     *
     * @return the number of values whose data was nulled
     */
    @Transactional
    public int nullifyEphemeralData(long rootValueId) {
        return em.createNativeQuery("""
            WITH RECURSIVE descendants (v_id) AS (
                SELECT ve.child_id FROM value_edge ve WHERE ve.parent_id = :rootId
                UNION ALL
                SELECT ve.child_id FROM value_edge ve JOIN descendants d ON ve.parent_id = d.v_id
            )
            UPDATE value SET data = NULL
            WHERE id IN (SELECT v_id FROM descendants)
              AND node_id IN (
                SELECT id FROM node WHERE
                  (ephemeral = 'DISCARD'
                   OR (ephemeral = 'AUTO' AND EXISTS (
                     SELECT 1 FROM node_edge ne
                     JOIN node child ON child.id = ne.child_id
                     WHERE ne.parent_id = node.id
                       AND child.type NOT IN ('ft', 'rd', 'sd', 'ed')
                   )))
                  AND type NOT IN ('root', 'ft', 'rd', 'sd', 'ed')
                  -- Protect nodes that are sources of detection nodes.
                  -- Range, domain, and fingerprint nodes need their data
                  -- for historical change detection queries.
                  AND NOT EXISTS (
                    SELECT 1 FROM node_edge ne2
                    JOIN node det ON det.id = ne2.child_id
                    WHERE ne2.parent_id = node.id
                      AND det.type IN ('ft', 'rd', 'sd', 'ed')
                  )
              )
              AND data IS NOT NULL
            """)
            .setParameter("rootId", rootValueId)
            .executeUpdate();
    }

    /**
     * Marks nodes as auto-ephemeral based on graph structure.
     * Nodes with ephemeral=AUTO that have non-detection children
     * (i.e., intermediate nodes) are set to ephemeral=DISCARD.
     * Nodes with ephemeral=KEEP (user override) are not touched.
     * Root and detection nodes are never marked ephemeral.
     *
     * @param groupId the node group to evaluate
     * @return number of nodes marked as ephemeral
     */
    @Transactional
    public int markAutoEphemeral(long groupId) {
        return em.createNativeQuery("""
            UPDATE node SET ephemeral = 'DISCARD'
            WHERE group_id = :groupId
              AND ephemeral = 'AUTO'
              AND type NOT IN ('root', 'ft', 'rd', 'sd', 'ed')
              AND EXISTS (
                SELECT 1 FROM node_edge ne
                JOIN node child ON child.id = ne.child_id
                WHERE ne.parent_id = node.id
                  AND child.type NOT IN ('ft', 'rd', 'sd', 'ed')
              )
            """)
            .setParameter("groupId", groupId)
            .executeUpdate();
    }

    /**
     * Nulls out all existing value data for a specific node.
     * Called when a user explicitly sets ephemeral to DISCARD on a node.
     *
     * @return the number of values whose data was nulled
     */
    @Transactional
    public int nullifyNodeData(long nodeId) {
        return em.createNativeQuery("""
            UPDATE value SET data = NULL
            WHERE node_id = :nodeId AND data IS NOT NULL
            """)
            .setParameter("nodeId", nodeId)
            .executeUpdate();
    }

}
