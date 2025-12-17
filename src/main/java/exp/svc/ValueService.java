package exp.svc;

import com.fasterxml.jackson.databind.JsonNode;
import exp.entity.Node;
import exp.entity.Value;
import exp.entity.node.RootNode;
import exp.pasted.JsonBinaryType;
import exp.queue.KahnDagSort;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.hibernate.Session;
import org.hibernate.query.NativeQuery;

import java.io.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
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



    @Transactional
    public void purgeValues(){
        em.createNativeQuery("delete from Value").executeUpdate();

    }

    @Transactional
    public Value create(Value value){
        if(!value.isPersistent()){
            value.id = null;
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
                    SELECT ve.value_id from value_edge ve where ve.source_id = :rootId
                    UNION ALL
                    SELECT ve.value_id from value_edge ve JOIN sourceRecursive sr
                    ON ve.source_id = sr.v_id
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
           SELECT * from Value v RIGHT JOIN value_edge ve ON ve.value_id = v.id WHERE v.node_id = :nodeId AND ve.source_id = :rootId
           """
        ).setParameter("rootId", root.id).setParameter("nodeId",node.id).getResultList());
        return rtrn;
    }

    /*
     * Finds the values for an ancestor node Source where a descendant created the expected fingerprint value, does not look for sibling or cousin values
     */
    @Transactional
    public List<Value> findMatchingFingerprint(Node source,Value fingerprint){
        List<Value> rtrn = new ArrayList<>(em.createNativeQuery(
            """
            with recursive ancestor(vid) as (
                select v.id as vid 
                    from value v where v.node_id = :nodeId and v.data = :data
                union 
                select v.id as vid 
                    from value v join value_edge ve on v.id = ve.source_id join ancestor a on a.vid = ve.value_id
            ) 
            select * from value v join ancestor a on v.id=a.vid where v.node_id=:sourceId
            """,Value.class)
                .setParameter("nodeId", fingerprint.node.id)
                .setParameter("data", fingerprint.data.toString())
                .setParameter("sourceId", source.id)
                .getResultList());
        return rtrn;
    }
    /*
     * Finds the values for a relative node Source where a descendant created the expected fingerprint value, now it works on siblings and cousins
     * we want this to also find sibling and cousin values but that probably requires another traversal
     * sorting by a value is useful if that value is our timestamp but we also need that value
     */
    @Transactional
    public List<Value> findMatchingFingerprintOrderBy(Node source,Value fingerprint,Node sort){
        List<Value> rtrn = new ArrayList<>(em.createNativeQuery(
                        """
                        with recursive ancestor(vid) as (
                            select v.id as vid 
                                from value v where v.node_id = :nodeId and v.data = :data
                            union 
                            select v.id as vid 
                                from value v join value_edge ve on v.id = ve.source_id join ancestor a on a.vid = ve.value_id
                        ),
                        sorter(vid,sortable) as (
                            select v.id as vid,v.data as sortable 
                                from value v where v.node_id = :sortId
                            union
                            select v.id as vid, s.sortable as sortable
                                from value v join value_edge ve on v.id = ve.source_id join sorter s on s.vid = ve.value_id
                        ),
                        descendant(vid,sortable) as (
                           select v.id as vid, s.sortable as sortable
                             from value v join sorter s on v.id = s.vid join ancestor a on v.id = a.vid join node n on n.id = v.node_id where n.type = 'root' --only the root values from ancestor with sorter
                           union
                           select v.id as vid, d.sortable as sortable
                                 from value v join value_edge ve on v.id = ve.value_id join descendant d on d.vid = ve.source_id
                        )                        
                        select * from value v join descendant d on v.id=d.vid where v.node_id=:sourceId order by sortable asc;
                        """,Value.class)
                .setParameter("nodeId", fingerprint.node.id)
                .setParameter("data", fingerprint.data.toString())
                .setParameter("sourceId", source.id)
                .setParameter("sortId",sort.id)
                .getResultList());
        return rtrn;
    }
    @Transactional
    public List<Value> findMatchingFingerprintOrderBy(Node source,Value fingerprint,Value before,int limit){
        List<Value> rtrn = new ArrayList<>(em.createNativeQuery(
                        """
                        with recursive ancestor(vid) as (
                            select v.id as vid 
                                from value v where v.node_id = :nodeId and v.data = :data
                            union 
                            select v.id as vid 
                                from value v join value_edge ve on v.id = ve.source_id join ancestor a on a.vid = ve.value_id
                        ),
                        sorter(vid,sortable) as (
                            select v.id as vid,v.data as sortable 
                                from value v where v.node_id = :sortId and v.data < :before
                            union
                            select v.id as vid, s.sortable as sortable
                                from value v join value_edge ve on v.id = ve.source_id join sorter s on s.vid = ve.value_id
                        ),
                        descendant(vid,sortable) as (
                           select v.id as vid, s.sortable as sortable
                             from value v join sorter s on v.id = s.vid join ancestor a on v.id = a.vid join node n on n.id = v.node_id where n.type = 'root' --only the root values from ancestor with sorter
                           union
                           select v.id as vid, d.sortable as sortable
                                 from value v join value_edge ve on v.id = ve.value_id join descendant d on d.vid = ve.source_id
                        )                        
                        select * from value v join descendant d on v.id=d.vid where v.node_id=:sourceId order by sortable desc limit :limit;
                        """,Value.class)
                .setParameter("nodeId", fingerprint.node.id)
                .setParameter("data", fingerprint.data.toString())
                .setParameter("sourceId", source.id)
                .setParameter("sortId",before.node.id)
                .setParameter("before",before.data.toString())
                .setParameter("limit",limit)
                .getResultList());
        //reversed
        return rtrn.reversed();
    }





    /**
     * returns a json object with the values created by child nodes of the groupBy node.
     * @param groupBy
     * @return
     */
    @Transactional
    public List<JsonNode> getGroupedValues(Node groupBy){
        List<JsonNode> rtrn = new ArrayList<>();
        NativeQuery query = (NativeQuery) em.createNativeQuery(
                """
                with recursive tree(id,node_id,root_id,idx,data) as (
                  select v.id,v.node_id,ve.source_id as root_id,v.idx,v.data from value_edge ve left join value v on ve.value_id = v.id where ve.source_id in (select id from value where node_id = :nodeId)
                  union
                  select v.id,v.node_id,t.root_id,v.idx,v.data from value v join value_edge ve on v.id = ve.value_id join tree t on ve.source_id = t.id
                ), bynode as (
                  select node_id,root_id,json_group_array(json(data)) as data from tree group by node_id,root_id order by idx
                )
                select json_group_object(n.name,json( ( case when json_array_length(b.data) > 1 then b.data else b.data->0 end))) as data from bynode b join node n on b.node_id = n.id group by root_id; 
                """
        ).setParameter("nodeId",groupBy.id);
        List<JsonNode> found = query
                .unwrap(NativeQuery.class)
                .addScalar("data", JsonBinaryType.INSTANCE)
                .list();
        rtrn.addAll(found);
        return rtrn;
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
                     SELECT ve.value_id from value_edge ve where ve.source_id in (select v.id from value v where v.node_id = :nodeId)
                     UNION ALL
                     SELECT ve.value_id from value_edge ve JOIN sourceRecursive sr ON ve.source_id = sr.v_id
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
                    SELECT ve.value_id from value_edge ve where ve.source_id = :rootId OR ve.value_id = :rootId
                    UNION ALL
                    SELECT ve.value_id from value_edge ve JOIN sourceRecursive sr
                    ON ve.source_id = sr.v_id
                )
                SELECT distinct * FROM value v JOIN sourceRecursive sr ON v.id = sr.v_id WHERE v.node_id = :nodeId
                """, Value.class
        ).setParameter("rootId", root.id).setParameter("nodeId",node.id).getResultList());
        return rtrn;
    }

    @Transactional
    public List<Value> getValues(Node node){
        return Value.find("node.id",node.id).list();
    }


    //TODO concerned about writeToFile using "'s to wrap strings
    @Transactional
    public void writeToFile(long valueId,String filePath){
        Session session = em.unwrap(Session.class);
        session.doWork(conn -> {
            try(PreparedStatement statement = conn.prepareStatement("select data from value where id=?")){
                statement.setLong(1,valueId);
                try(ResultSet rs = statement.executeQuery()){
                    try (InputStream inputStream = rs.getBinaryStream(1);
                         OutputStream outputStream = new FileOutputStream(filePath)) {

                        long bytesCopied = inputStream.transferTo(outputStream);
                        if(bytesCopied < 1){
                            System.out.println("failed to transfer data for value="+valueId);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
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
