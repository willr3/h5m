package io.hyperfoil.tools.h5m.entity;

import com.fasterxml.jackson.annotation.*;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.provided.H5mEntityListener;
import io.hyperfoil.tools.h5m.queue.KahnDagSort;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.persistence.CascadeType;
import org.hibernate.annotations.*;
import org.hibernate.dialect.PostgreSQLDialect;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Entity(name = "node")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@EntityListeners( H5mEntityListener.class )
@DiscriminatorColumn(name = "type", discriminatorType =  DiscriminatorType.STRING)
public abstract class NodeEntity extends PanacheEntity implements Comparable<NodeEntity> {

    public static String FQDN_SEPARATOR = ":";
    public static String NAME_SEPARATOR = "=";

    public static enum MultiIterationType { Length, NxN}
    public static enum ScalarVariableMethod { First, All}

    public String name;

    @Column(columnDefinition = "TEXT")
    public String operation;
    public MultiIterationType multiType = MultiIterationType.Length;
    public ScalarVariableMethod scalarMethod = ScalarVariableMethod.First;

    @ManyToOne(cascade = { CascadeType.PERSIST }, fetch = FetchType.LAZY )
    @JoinColumn(name = "group_id")
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "group_id")
//    @JsonIdentityReference(alwaysAsId = true)
    @JsonBackReference
    public NodeGroupEntity group;

    //making this eager causes too many joins
    //cannot cascade delete because this entity "owns" the reference to the parent values
    @ManyToMany(cascade = { CascadeType.PERSIST }, fetch = FetchType.LAZY )
    @JoinTable(
            name="node_edge",
            joinColumns = @JoinColumn(name = "child_id"),
            inverseJoinColumns = @JoinColumn(name = "parent_id", updatable = false, insertable = false),
            uniqueConstraints = @UniqueConstraint(columnNames = {"child_id", "parent_id", "depth"})
            //indexes = @Index(name = "idx_node_edge_parent", columnList = "parent_id")
    )
    @OrderColumn(name = "idx")
    @SQLInsert(sql="""
        WITH insert_edge (child_id,idx,parent_id) AS (VALUES(?,?,?)),
        found AS (
            SELECT l.parent_id, r.child_id, l.depth + r.depth + 1 AS depth, r.count AS count, r.idx AS idx
            FROM
                ( SELECT ne.parent_id, ne.depth FROM node_edge ne JOIN insert_edge ie ON ne.child_id = ie.parent_id) l
            CROSS JOIN
                ( SELECT ne.child_id, ne.depth, ie.idx, ne.count FROM node_edge ne JOIN insert_edge ie ON ne.parent_id = ie.child_id) r
        )
        INSERT INTO node_edge (parent_id,child_id,idx,depth,count)
        SELECT parent_id,child_id,idx,depth,count FROM found
        WHERE true
        ON CONFLICT (child_id, parent_id, depth)
        DO UPDATE SET count = node_edge.count + 1
        """)

    @SQLJoinTableRestriction("depth = 1")
    @SQLDeleteAll(sql = """
        delete from node_edge where child_id = ? and child_id != parent_id
        """)
    @SQLDelete(sql= """
        with from_hibernate_delete (child_id,idx) as ( values(?,?) ),
        target_edge as (select ne.child_id,ne.parent_id,ne.depth,ne.count from node_edge ne join from_hibernate_delete fh on ne.child_id = fh.child_id and ne.idx = fh.idx where ne.depth = 1),
        selected_edge as (
          select l.parent_id as parent_id, r.child_id as child_id, l.depth + r.depth + 1 as depth
          from ( select ne.* from node_edge ne join target_edge te on ne.child_id = te.parent_id) l
          cross join
          ( select ne.* from node_edge ne join target_edge te on ne.parent_id = te.child_id) r
        )
        update node_edge set count = count - 1 where (parent_id,child_id,depth) in (select parent_id, child_id, depth from selected_edge);
        """
    )
    @SQLUpdate(
        table = "node_edge",
        sql= """
        with from_hibernate_update (parent_id,child_id,idx) as (values(?,?,?)),
        existing (parent_id,child_id,idx) as (select ne.parent_id,ne.child_id,ne.idx from node_edge ne join from_hibernate_update hu on ne.child_id = hu.child_id and ne.idx = hu.idx where ne.depth = 1),
        --delete the existing 
        delete_target_edge as (select ne.child_id,ne.parent_id,ne.depth,ne.count from node_edge ne join existing e on ne.child_id = e.child_id and ne.idx = e.idx where ne.depth = 1),
        delete_selected_edge as (
          select l.parent_id as parent_id, r.child_id as child_id, l.depth + r.depth + 1 as depth
          from ( select ne.* from node_edge ne join delete_target_edge te on ne.child_id = te.parent_id) l
          cross join
          ( select ne.* from node_edge ne join delete_target_edge te on ne.parent_id = te.child_id) r
        ),
        delete_rows (parent_id,child_id,idx,depth,count) as (select parent_id,child_id,idx,depth,count-1 as count from node_edge where (parent_id,child_id,depth) in (select parent_id, child_id, depth from delete_selected_edge)),
        insert_found (parent_id,child_id,idx,depth,count) AS (
            SELECT l.parent_id, r.child_id, r.idx AS idx, l.depth + r.depth + 1 AS depth, r.count AS count
            FROM
                ( SELECT ne.parent_id, ne.depth FROM node_edge ne JOIN from_hibernate_update fh ON ne.child_id = fh.parent_id) l
            CROSS JOIN
                ( SELECT ne.child_id, ne.depth, fh.idx, ne.count FROM node_edge ne JOIN from_hibernate_update fh ON ne.parent_id = fh.child_id) r
        )
        INSERT INTO node_edge (parent_id,child_id,idx,depth,count)
            SELECT 
                coalesce(d.parent_id,i.parent_id) as parent_id ,
                coalesce(d.child_id,i.child_id) as child_id ,
                coalesce(d.idx,i.idx) as idx,
                coalesce(d.depth,i.depth) as depth, 
                case when d.count is null then i.count when i.count is null then d.count when d.count = i.count then d.count else -1 end as count 
            from delete_rows d full outer join insert_found i on d.parent_id = i.parent_id and d.child_id = i.child_id and d.depth = i.depth           
        WHERE case when d.count is null then i.count when i.count is null then d.count when d.count = i.count then d.count else -1 end >= 0
        ON CONFLICT (child_id, parent_id, depth)
        DO UPDATE SET count = EXCLUDED.count
    """)
    @DialectOverride.SQLUpdate(
            dialect = PostgreSQLDialect.class,
            override = @SQLUpdate(
                    table = "node_edge",
                    sql= """
        with from_hibernate_update (parent_id,child_id,idx) as (values(?,?,?)),
        existing (parent_id,child_id,idx) as (select ne.parent_id,ne.child_id,ne.idx from node_edge ne join from_hibernate_update hu on ne.child_id = hu.child_id and ne.idx = hu.idx where ne.depth = 1),
        --delete the existing 
        delete_target_edge as (select ne.child_id,ne.parent_id,ne.depth,ne.count from node_edge ne join existing e on ne.child_id = e.child_id and ne.idx = e.idx where ne.depth = 1),
        delete_selected_edge as (
          select l.parent_id as parent_id, r.child_id as child_id, l.depth + r.depth + 1 as depth
          from ( select ne.* from node_edge ne join delete_target_edge te on ne.child_id = te.parent_id) l
          cross join
          ( select ne.* from node_edge ne join delete_target_edge te on ne.parent_id = te.child_id) r
        ),
        delete_rows (parent_id,child_id,idx,depth,count) as (select parent_id,child_id,idx,depth,count-1 as count from node_edge where (parent_id,child_id,depth) in (select parent_id, child_id, depth from delete_selected_edge)),
        insert_found (parent_id,child_id,idx,depth,count) AS (
            SELECT l.parent_id, r.child_id, r.idx AS idx, l.depth + r.depth + 1 AS depth, r.count AS count
            FROM
                ( SELECT ne.parent_id, ne.depth FROM node_edge ne JOIN from_hibernate_update fh ON ne.child_id = fh.parent_id) l
            CROSS JOIN
                ( SELECT ne.child_id, ne.depth, fh.idx, ne.count FROM node_edge ne JOIN from_hibernate_update fh ON ne.parent_id = fh.child_id) r
        )
        INSERT INTO node_edge (parent_id,child_id,idx,depth,count)
            SELECT 
                coalesce(d.parent_id,i.parent_id) as parent_id ,
                coalesce(d.child_id,i.child_id) as child_id ,
                coalesce(d.idx,i.idx) as idx,
                coalesce(d.depth,i.depth) as depth, 
                case when d.count is null then i.count when i.count is null then d.count when d.count = i.count then d.count else -1 end as count 
            from delete_rows d full outer join insert_found i on d.parent_id = i.parent_id and d.child_id = i.child_id and d.depth = i.depth           
        WHERE true
        ON CONFLICT (child_id, parent_id, depth)
        DO UPDATE SET count = EXCLUDED.count
            """)
    )
    public List<NodeEntity> sources;

    public List<NodeEntity> getSources() {return sources;}

    @Transient
    @JsonIgnore
    private transient Set<Long> ancestorCache;


    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "previous_version_id")
    NodeEntity previousVersion;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "original_node_id")
    NodeEntity originalNode;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "original_group_id")
    NodeGroupEntity originalGroup;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "target_group_id")
    NodeGroupEntity targetGroup;

    public NodeGroupEntity getOriginalGroup() {
        return originalGroup;
    }

    public void setOriginalGroup(NodeGroupEntity originalGroup) {
        this.originalGroup = originalGroup;
    }

    public NodeEntity getOriginalNode() {
        return originalNode;
    }

    public void setOriginalNode(NodeEntity originalNode) {
        this.originalNode = originalNode;
    }

    public NodeEntity getPreviousVersion() {
        return previousVersion;
    }

    public void setPreviousVersion(NodeEntity previousVersion) {
        this.previousVersion = previousVersion;
    }

    public NodeEntity(){
        this.sources = new ArrayList<>();
    }
    public NodeEntity(String name){
        this.sources = new ArrayList<>();
        this.name = name;
    }
    public NodeEntity(String name,String operation){
        this.sources = new ArrayList<>();
        this.name = name;
        this.operation = operation;
    }
    public NodeEntity(String name,String operation,List<NodeEntity> sources){
        this.sources = new ArrayList<>(sources);
        this.name = name;
        this.operation = operation;
    }

    public Long getId(){return this.id;}

    public String getFqdn(){
        return (group!=null?group.name+FQDN_SEPARATOR:"")+(originalGroup!=null?originalGroup.name+FQDN_SEPARATOR:"")+name;
    }
    public String getOperationEncoding(){
        return operation;
    }

    protected abstract NodeEntity shallowCopy();

    public abstract NodeType type();

    public boolean isDetection() {
        return this instanceof io.hyperfoil.tools.h5m.entity.node.DetectionNode;
    }

    public boolean hasNonRootSource(){
        return sources.stream().anyMatch(s-> s.type() != NodeType.ROOT);
    }

    public NodeEntity copy(){
        NodeEntity rtrn = shallowCopy();
        rtrn.sources = sources.stream().map(NodeEntity::shallowCopy).toList();
        return rtrn;
    }

    @Override
    public String toString(){
        return getClass().getSimpleName()+"< name="+name+", op="+operation+", id="+id+", source.ids=["+sources.stream().map(s->""+s.id).collect(Collectors.joining(" "))+"]>";
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof NodeEntity){
            NodeEntity n = (NodeEntity)o;
            if(id == null && n.id == null){
                if(!Objects.equals(name,n.name) || !Objects.equals(operation,n.operation) || sources.size() != n.sources.size()){
                    return false;
                }
                // compare sources by id (one level deep, no recursion)
                for(int i = 0, size = sources.size(); i < size; i++){
                    NodeEntity s1 = sources.get(i);
                    NodeEntity s2 = n.sources.get(i);
                    if(!Objects.equals(s1.id, s2.id)){
                        return false;
                    }
                }
                return true;
            } else if (id != null && n.id != null){
                return n.id.equals(id);
            } else { //one is persisted and the other is not
                return false;
            }
        }
        return false;
    }

    @Override
    public int hashCode(){
        if(id != null){
            return Objects.hash(id, name, operation);
        }
        // hash source ids one level deep to avoid recursion on deep graphs
        int sourcesHash = 1;
        for(int i = 0, size = sources.size(); i < size; i++){
            Long sourceId = sources.get(i).id;
            sourcesHash = 31 * sourcesHash + (sourceId != null ? Long.hashCode(sourceId) : 0);
        }
        return Objects.hash(name, operation, sourcesHash);
    }

    public boolean dependsOn(NodeEntity source) {
        if (source == null || this.sources == null) return false;
        if (source.id != null) {
            if (ancestorCache == null) {
                ancestorCache = computeAncestorIds();
            }
            return ancestorCache.contains(source.id);
        }
        // fallback for unpersisted nodes (no id)
        Queue<NodeEntity> queue = new ArrayDeque<>(sources);
        Set<NodeEntity> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!queue.isEmpty()) {
            NodeEntity node = queue.poll();
            if (node.equals(source)) return true;
            if (visited.add(node)) {
                queue.addAll(node.sources);
            }
        }
        return false;
    }

    private Set<Long> computeAncestorIds() {
        if(sources == null || sources.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Long> ancestors = new HashSet<>();
        Queue<NodeEntity> queue = new ArrayDeque<>(sources);
        while (!queue.isEmpty()) {
            NodeEntity node = queue.poll();
            if (node.id != null && ancestors.add(node.id)) {
                queue.addAll(node.sources);
            }
        }
        return ancestors;
    }

    /**
     * copy a NodeGroupEntity into this node's NodeGroupEntity as though this node is the RootNode for the copied group.
     * @param group
     */
    public boolean loadGroup(NodeGroupEntity group){
        return loadGroup(group,this.group);
    }
    public boolean loadGroup(NodeGroupEntity group, NodeGroupEntity thisGroup){
        if(thisGroup == null) return false;
        //TODO do we track the sourceGroup when loading a group
        Map<NodeEntity, NodeEntity> fromGroupToThis = new HashMap<>();
        List<NodeEntity> clones = new  ArrayList<>();
        for( NodeEntity node : group.sources){
            NodeEntity cloned = node.shallowCopy();
            fromGroupToThis.put(node, cloned);
            List<NodeEntity> clonedSources = node.sources.stream().map(source->{
                if(source instanceof RootNode){
                    return this; //current node replaces the root node
                }else if (fromGroupToThis.containsKey(source)){
                    return fromGroupToThis.get(source);
                }else{
                    //sources should be in dependency order so the node missing is an error
                    System.err.println(source.name+" was missing from local copy of nodeGroup "+group.name+" under "+this.name);
                    return source.shallowCopy();
                }
            }).filter(Objects::nonNull).toList();
            node.sources = clonedSources;
            clones.add(cloned);
        }
        thisGroup.sources.addAll(clones);
        return true;
    }

    @Override
    public int compareTo(NodeEntity n1){
        int rtrn = 0;
        if(!n1.sources.isEmpty() && this.sources.isEmpty()){
            rtrn = -1;//o1 comes before this one
        }else if (!this.sources.isEmpty() && n1.sources.isEmpty()){
            rtrn = 1;
        }else if (n1.dependsOn(this)){//other object depends on this one
            rtrn = -1;
        }else if (this.dependsOn(n1)){
            rtrn = 1;
        }else if (this.sources.size() > n1.sources.size()){
            rtrn = 1;
        }else if (n1.sources.size() > this.sources.size()){
            rtrn = -1;
        }else {
            //unable to compare at this time, return stable number
        }
        return rtrn;
    }


    //based on https://github.com/williamfiset/Algorithms/blob/master/src/main/java/com/williamfiset/algorithms/graphtheory/Kahns.java
    public static List<NodeEntity> kahnDagSort(NodeEntity... nodes){
        return kahnDagSort(Arrays.asList(nodes));
    }
    public static List<NodeEntity> kahnDagSort(List<NodeEntity> nodes){
        return KahnDagSort.sort(nodes, NodeEntity::getSources);
    }
    public static List<NodeEntity> old_kahnDagSort(List<NodeEntity> nodes){
        Map<String, AtomicInteger> inDegrees = new HashMap<>();
        if(nodes == null || nodes.isEmpty()){
            return nodes;
        }
        nodes.forEach(n->{
            inDegrees.put(n.getFqdn(),new AtomicInteger(0));
        });
        nodes.forEach(n->{
            n.sources.stream()
                    .forEach(s->{
                        if(inDegrees.containsKey(s.getFqdn())){
                            inDegrees.get(s.getFqdn()).incrementAndGet();
                        }
                    });
        });
        Queue<NodeEntity> q = new ArrayDeque<>();
        //using reveresed to preserve order
        nodes.reversed().forEach(n->{
            if(inDegrees.get(n.getFqdn()).get()==0){
                q.offer(n);
            }
        });
        List<NodeEntity> rtrn = new ArrayList<>();
        while(!q.isEmpty()){
            NodeEntity n = q.poll();
            rtrn.add(n);
            n.sources.stream()
                    .forEach(s->{
                        if(inDegrees.containsKey(s.getFqdn())){
                            int newDegree = inDegrees.get(s.getFqdn()).decrementAndGet();
                            if(newDegree == 0){
                                q.offer(s);
                            }
                        }
                    });
        }
        int sum = inDegrees.values().stream().map(AtomicInteger::get).reduce(Integer::sum).orElse(0);
        if(sum > 0){
            //this means there are loops!!
            //using reversed to preserve order
            nodes.reversed().forEach(n->{
                if(inDegrees.get(n.getFqdn()).get() > 0){
                    rtrn.add(0,n);//they will then go to the back
                }
            });
        }
        //reverse because of graph direction
        Collections.reverse(rtrn);
        return rtrn;
    }

    public boolean isCircular(){
        return KahnDagSort.isCircular(this, NodeEntity::getSources);
    }


}
