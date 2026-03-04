package io.hyperfoil.tools.h5m.entity;

import com.fasterxml.jackson.annotation.*;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.queue.KahnDagSort;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import org.hibernate.annotations.BatchSize;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Entity(name = "node")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "type", discriminatorType =  DiscriminatorType.STRING)
public abstract class NodeEntity extends PanacheEntity implements Comparable<NodeEntity> {

    public static String FQDN_SEPARATOR = ":";
    public static String NAME_SEPARATOR = "=";

    public static enum MultiIterationType { Length, NxN}
    public static enum ScalarVariableMethod { First, All}

    @Column(insertable=false, updatable=false)
    @JsonIgnore
    public String type; //maybe we want access to the type?
    public String name;

    @Column(columnDefinition = "TEXT")
    public String operation;
    public MultiIterationType multiType = MultiIterationType.Length;
    public ScalarVariableMethod scalarMethod = ScalarVariableMethod.First;

    @ManyToOne(cascade = { CascadeType.PERSIST, CascadeType.MERGE }, fetch = FetchType.LAZY )
    @JoinColumn(name = "group_id")
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "group_id")
//    @JsonIdentityReference(alwaysAsId = true)
    @JsonBackReference
    public NodeGroupEntity group;

    //making this eager causes too many joins
    //cannot cascade delete because this entity "owns" the reference to the parent values
    @ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.MERGE }, fetch = FetchType.LAZY )
    @JoinTable(
            name="node_edge",
            joinColumns = @JoinColumn(name = "child_id"),
            inverseJoinColumns = @JoinColumn(name = "parent_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"child_id", "parent_id"})
    )
    @OrderColumn(name = "idx")
    @BatchSize(size = 25)
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

    @PreUpdate
    @PrePersist
    public void sortSources(){
        this.sources = KahnDagSort.sort(sources, NodeEntity::getSources);
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

    public boolean hasNonRootSource(){
        return sources.stream().anyMatch(s->!s.type.equals("root"));
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
