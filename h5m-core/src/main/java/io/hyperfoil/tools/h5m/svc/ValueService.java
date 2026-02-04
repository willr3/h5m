package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.Value;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.queue.KahnDagSort;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hibernate.Session;
import org.hibernate.query.NativeQuery;

import java.io.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class ValueService {

    @Inject
    EntityManager em;

//    @Inject
//    @Named("duckdb")
//    AgroalDataSource duckDatasource;

    @ConfigProperty(name="quarkus.datasource.db-kind")
    String dbKind;
    @Inject
    NodeService nodeService;


    @Transactional
    public void purgeValues(){
        em.createNativeQuery("delete from Value").executeUpdate();

    }

    @Transactional
    public Value create(Value value){
        if(!value.isPersistent()){
            Value merged = em.merge(value);
            em.flush();
            value.id = merged.id;
            return merged;
        }else{
            value.persist();
        }
        return value;
    }

    @Transactional
    public List<Value> getDependentNodes(Value v){
        return Node.list("SELECT DISTINCT v FROM Value v JOIN v.sources s WHERE s.id = ?1",v.id);
    }


    @Transactional
    public void delete(Value value){
        if(value.id != null){
            //remove values that depend on this or just remove the reference?
            getDependentNodes(value).forEach(this::delete);
            Value.deleteById(value.id);
        }
    }

    @Transactional
    public Value byId(Long id){
        return Value.findById(id);
    }

    @Transactional
    public Value byPath(String path){
        return Value.find("path",path).firstResult();
    }

    /**
     * returns the values that depend on the root value as a source somewhere up the hierarchy.
     * @param root
     * @return
     */
    public List<Value> getDescendantValues(Value root){
        List<Value> rtrn = new ArrayList<>();
        //noinspection unchecked
        rtrn.addAll(em.createNativeQuery(
                """
                WITH RECURSIVE sourceRecursive (v_id) AS (
                    SELECT ve.child_id from value_edge ve where ve.parent_id = :rootId
                    UNION ALL
                    SELECT ve.child_id from value_edge ve JOIN sourceRecursive sr
                    ON ve.parent_id = sr.v_id
                )
                SELECT * FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id
                """, Value.class
        ).setParameter("rootId", root.id).getResultList());
        return rtrn;
    }


    public List<Value> getDirectDescendantValues(Value root, Node node){
        List<Value> rtrn = new ArrayList<>();
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
    public List<Value> findMatchingFingerprint(Node source,Value fingerprint){
        source = Node.findById(source.id);
        return findMatchingFingerprint(source,source.group.root,fingerprint,null,null,-1,-1,true);
    }
    /*
     * Finds the values for a relative node Source where a descendant created the expected fingerprint value, now it works on siblings and cousins
     * we want this to also find sibling and cousin values but that probably requires another traversal
     * sorting by a value is useful if that value is our timestamp but we also need that value
     */
    @Transactional
    public List<Value> findMatchingFingerprint_unused(Node source, Value fingerprint, Node sort){
        List<Value> rtrn = new ArrayList<>(em.createNativeQuery(
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
            },Value.class)
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
    public List<Value> findMatchingFingerprint(Node source,Value fingerprint, Node sort){
        return findMatchingFingerprint(source,source.group.root,fingerprint,sort,null,-1,-1,true);
    }
    public List<Value> findMatchingFingerprint(Node source,Node groupBy,Value fingerprint, Node sort){
        return findMatchingFingerprint(source,groupBy,fingerprint,sort,null,-1,-1,true);
    }

    //TODO I want a way to specify a required ancestor value for the resulting values
    @Transactional
    public List<Value> findMatchingFingerprint(Node rangeNode,Node groupBy,Value fingerprint,Node domainNode, Value domainValue,int limit,int offset,boolean preceedingValues){
        return findMatchingFingerprint(rangeNode,groupBy,fingerprint,domainNode,domainValue,null,limit,offset,preceedingValues);
    }

    @Transactional
    public List<Value> findMatchingFingerprint(Node rangeNode,Node groupBy,Value fingerprint,Node domainNode, Value domainValue,Value ancestorValue,int limit,int offset,boolean preceedingValues){

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
                        select * from value v join descendant d on v.id=d.vid 
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
                        select * from value v join descendant d on v.id=d.vid
                            where v.node_id=:sourceId order by sortable ORDER_DIRECTION
                        """;
                default -> "";
            };
            sql = sql.replace("DOMAIN_VALUE_COMP",domainValue != null ? domainValueComp : "");
        }else{
            //sorting by created_at
            sql+=switch(dbKind){
                case "sqlite"->
                        """
                        descendant(vid) as (
                           select v.id as vid
                             from value v join ancestor a on v.id = a.vid
                             where v.node_id = :groupById --limit descendants to values from the grouping node
                           union
                           select v.id as vid
                                 from value v join value_edge ve on v.id = ve.child_id join descendant d on d.vid = ve.parent_id
                        )
                        select * from value v join descendant d on v.id=d.vid
                            where v.node_id=:sourceId order by created_at ORDER_DIRECTION
                        """;
                case "postgresql"->
                        """
                        descendant(vid) as (
                           select v.id as vid
                             from value v join ancestor a on v.id = a.vid
                             where v.node_id = :groupById --limit descendants to values from the grouping node
                           union
                           select v.id as vid
                                 from value v join value_edge ve on v.id = ve.child_id join descendant d on d.vid = ve.parent_id
                        )                        
                        select * from value v join descendant d on v.id=d.vid
                            where v.node_id=:sourceId order by created_at ORDER_DIRECTION
                        """;
                default -> "";
            };
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
        //noinspection unchecked
        NativeQuery<Value> query = (NativeQuery<Value>) em.createNativeQuery(sql,Value.class);
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
        List<Value> rtrn = query
                .getResultList();
        //reversed
        return rtrn.reversed();
    }

    public List<Value> getAncestor(Value value,Node node){
        List<Value> rtrn = new ArrayList<>();
        rtrn.addAll(em.createNativeQuery("""
            with recursive ancestor(vid) as (
                select v.id as vid 
                    from value v where v.id = :valueId
                union 
                select v.id as vid 
                    from value v join value_edge ve on v.id = ve.parent_id join ancestor a on a.vid = ve.child_id
            )
            select v.* from Value v join ancestor a on v.id = a.vid where v.node_id = :nodeId
        """,Value.class).setParameter("nodeId", node.id).setParameter("valueId",value.id).getResultList());
        return rtrn;
    }



    /**
     * returns a json object with the values created by child nodes of the groupBy node.
     * @param groupBy
     * @return
     */
    @Transactional
    //TODO here thar be dragons
    public List<JsonNode> getGroupedValues(Node groupBy){
        return em.unwrap(Session.class).createNativeQuery(
            switch(dbKind) {
                case "sqlite" ->
                    """
                    with recursive tree(id,node_id,root_id,idx,data) as (
                        select v.id,v.node_id,ve.parent_id as root_id,v.idx,v.data 
                            from value_edge ve left join value v on ve.child_id = v.id 
                            where ve.parent_id in (select id from value where node_id = :nodeId)
                        union
                        select v.id,v.node_id,t.root_id,v.idx,v.data 
                            from value v join value_edge ve on v.id = ve.child_id join tree t on ve.parent_id = t.id
                    ), bynode as (
                        select node_id,root_id,json_group_array(json(data)) as data 
                            from tree group by node_id,root_id order by idx
                    )
                    select json_group_object(n.name,json( ( case when json_array_length(b.data) > 1 then b.data else b.data->0 end))) as data 
                        from bynode b join node n on b.node_id = n.id group by root_id; 
                    """;
                case "postgresql" ->
                    """
                    with recursive tree(id,node_id,root_id,idx,data) as (
                        select v.id,v.node_id,ve.parent_id as root_id,v.idx,v.data 
                            from value_edge ve left join value v on ve.child_id = v.id 
                            where ve.parent_id in (select id from value where node_id = :nodeId)
                        union
                        select v.id,v.node_id,t.root_id,v.idx,v.data 
                            from value v join value_edge ve on v.id = ve.child_id join tree t on ve.parent_id = t.id
                    ), bynode as (
                        select node_id,root_id,jsonb_agg(to_jsonb(data)) as data 
                            from tree group by node_id,root_id,idx order by idx
                    )
                    select jsonb_object_agg(n.name,to_jsonb( ( case when jsonb_array_length(b.data) > 1 then b.data else b.data->0 end))) as data 
                    from bynode b join node n on b.node_id = n.id group by root_id;
                    """;
            default ->"";
            }, JsonNode.class
        ).setParameter("nodeId",groupBy.id).getResultList();
    }
    /**
     * get all value that have a value from node as an ancestor
     * @param node
     * @return
     */
    @Transactional
    public List<Value> getDescendantValues(Node node){
        List<Value> rtrn = new ArrayList<>();
        rtrn.addAll(em.createNativeQuery(
                """
                WITH RECURSIVE sourceRecursive (v_id) AS (
                     SELECT ve.child_id from value_edge ve where ve.parent_id in (select v.id from value v where v.node_id = :nodeId)
                     UNION ALL
                     SELECT ve.child_id from value_edge ve JOIN sourceRecursive sr ON ve.parent_id = sr.v_id
                )
                SELECT distinct * FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id
                """, Value.class
        ).setParameter("nodeId",node.id).getResultList());
        return rtrn;
    }
    /**
     * returns the values that depend on the root value somewhere up the hierarchy and come from the specified node
     * @param root
     * @param node
     * @return
     */
    @Transactional
    public List<Value> getDescendantValues(Value root, Node node){

        List<Value> rtrn = new ArrayList<>();
        //noinspection unchecked
        rtrn.addAll(em.createNativeQuery(
                """
                WITH RECURSIVE sourceRecursive (v_id) AS (
                    SELECT ve.child_id from value_edge ve where ve.parent_id = :rootId OR ve.child_id = :rootId
                    UNION ALL
                    SELECT ve.child_id from value_edge ve JOIN sourceRecursive sr
                    ON ve.parent_id = sr.v_id
                )
                SELECT distinct * FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id WHERE v.node_id = :nodeId
                """, Value.class
        ).setParameter("rootId", root.id).setParameter("nodeId",node.id).getResultList());
        return rtrn;
    }

    //get the values from node that descend from root with the associated value source path (nodeId and index for each source value)
    @Transactional
    public Map<String,Value> getDescendantValueByPath(Value root,Node node){
        List<Value> found = getDescendantValues(root,node);
        found.forEach(v->{v.hashCode();});//lazy hack
        return found.stream().collect(Collectors.toMap(Value::getPath,v->v));
    }

    @Transactional
    public List<Value> getValues(Node node){
        return Value.find("node.id",node.id).list();
    }


    //TODO concerned about writeToFile using "'s to wrap strings
    //TODO have write to file return the filePath instead of accepting it as an argument
    @Transactional
    public void writeToFile(long valueId,String filePath){
        Session session = em.unwrap(Session.class);
        session.doWork(conn -> {
            try(PreparedStatement statement = conn.prepareStatement("select data from value where id=?")){
                statement.setLong(1,valueId);
                try(ResultSet rs = statement.executeQuery()){
                    if(rs.next()) {
                        try (InputStream inputStream = rs.getBinaryStream(1);
                             OutputStream outputStream = new FileOutputStream(filePath)) {

                            long bytesCopied = inputStream.transferTo(outputStream);
                            if (bytesCopied < 1) {
                                System.err.println("failed to transfer data for value=" + valueId);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Transactional
    public int deleteDescendantValues(Value root,Node node){
        List<Value> descendants = getDescendantValues(root,node);
        descendants = KahnDagSort.sort(descendants,v->v.getSources()).reversed();
        descendants.forEach(PanacheEntityBase::delete);
        return descendants.size();
    }

    @Transactional
    public int purge(Value root){
        if(root.node instanceof RootNode){
            return 0;//don't want to support deleting uploads just yet
        }
        List<Value> descendants = getDescendantValues(root);
        descendants.forEach(Value::delete);
        root.delete();
        return 1+descendants.size();
    }

    //TODO getHash(Value value) to see if a new value is different than the persisted one
    public String getHash(Value value) throws NoSuchAlgorithmException {
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



}
