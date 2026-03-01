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

@Entity
@Table(
        name = "node"

)
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "type", discriminatorType =  DiscriminatorType.STRING)
public abstract class Node extends PanacheEntity implements Comparable<Node> {

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
    public NodeGroup group;

    //making this eager causes too many joins
    //cannot cascade delete because this entity "owns" the reference to the parent values
    @ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REFRESH }, fetch = FetchType.LAZY )
    @JoinTable(
            name="node_edge",
            joinColumns = @JoinColumn(name = "child_id"),
            inverseJoinColumns = @JoinColumn(name = "parent_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"child_id", "parent_id"})
    )
    @OrderColumn(name = "idx")
    @BatchSize(size = 25)
    public List<Node> sources;

    public List<Node> getSources() {return sources;}

    @Transient
    @JsonIgnore
    private transient Set<Long> ancestorCache;


    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "previous_version_id")
    Node previousVersion;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "original_node_id")
    Node originalNode;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "original_group_id")
    NodeGroup originalGroup;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "target_group_id")
    NodeGroup targetGroup;

    public NodeGroup getOriginalGroup() {
        return originalGroup;
    }

    public void setOriginalGroup(NodeGroup originalGroup) {
        this.originalGroup = originalGroup;
    }

    public Node getOriginalNode() {
        return originalNode;
    }

    public void setOriginalNode(Node originalNode) {
        this.originalNode = originalNode;
    }

    public Node getPreviousVersion() {
        return previousVersion;
    }

    public void setPreviousVersion(Node previousVersion) {
        this.previousVersion = previousVersion;
    }

    @PreUpdate
    @PrePersist
    public void sortSources(){
        this.sources = KahnDagSort.sort(sources,Node::getSources);
    }


    public Node(){
        this.sources = new ArrayList<>();
    }
    public Node(String name){
        this.sources = new ArrayList<>();
        this.name = name;
    }
    public Node(String name,String operation){
        this.sources = new ArrayList<>();
        this.name = name;
        this.operation = operation;
    }
    public Node(String name,String operation,List<Node> sources){
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

    protected abstract Node shallowCopy();

    public boolean hasNonRootSource(){
        return sources.stream().anyMatch(s->!s.type.equals("root"));
    }

    public Node copy(){
        Node rtrn = shallowCopy();
        rtrn.sources = sources.stream().map(Node::shallowCopy).toList();
        return rtrn;
    }

    @Override
    public String toString(){
        return getClass().getSimpleName()+"< name="+name+", op="+operation+", id="+id+", source.ids=["+sources.stream().map(s->""+s.id).collect(Collectors.joining(" "))+"]>";
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Node){
            Node n = (Node)o;
            if(id == null && n.id == null){
                if(!Objects.equals(name,n.name) || !Objects.equals(operation,n.operation) || sources.size() != n.sources.size()){
                    return false;
                }
                // compare sources by id (one level deep, no recursion)
                for(int i = 0, size = sources.size(); i < size; i++){
                    Node s1 = sources.get(i);
                    Node s2 = n.sources.get(i);
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

    public boolean dependsOn(Node source) {
        if (source == null || this.sources == null) return false;
        if (source.id != null) {
            if (ancestorCache == null) {
                ancestorCache = computeAncestorIds();
            }
            return ancestorCache.contains(source.id);
        }
        // fallback for unpersisted nodes (no id)
        Queue<Node> queue = new ArrayDeque<>(sources);
        Set<Node> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!queue.isEmpty()) {
            Node node = queue.poll();
            if (node.equals(source)) return true;
            if (visited.add(node)) {
                queue.addAll(node.sources);
            }
        }
        return false;
    }

    private Set<Long> computeAncestorIds() {
        System.out.println(this.name+"("+this.id+").computeAncestorIds()\n  sources="+sources);
        if(sources == null || sources.isEmpty()) {
            return Collections.emptySet();
        }

        Set<Long> ancestors = new HashSet<>();
        Queue<Node> queue = new ArrayDeque<>(sources);
        while (!queue.isEmpty()) {
            Node node = queue.poll();
            if (node.id != null && ancestors.add(node.id)) {
                queue.addAll(node.sources);
            }
        }
        return ancestors;
    }

    /**
     * copy a NodeGroup into this node's NodeGroup as though this node is the RootNode for the copied group.
     * @param group
     */
    public boolean loadGroup(NodeGroup group){
        return loadGroup(group,this.group);
    }
    public boolean loadGroup(NodeGroup group, NodeGroup thisGroup){
        if(thisGroup == null) return false;
        //TODO do we track the sourceGroup when loading a group
        Map<Node,Node> fromGroupToThis = new HashMap<>();
        List<Node> clones = new  ArrayList<>();
        for( Node node : group.sources){
            Node cloned = node.shallowCopy();
            fromGroupToThis.put(node, cloned);
            List<Node> clonedSources = node.sources.stream().map(source->{
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
    public int compareTo(Node n1){
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
    public static List<Node> kahnDagSort(Node... nodes){
        return kahnDagSort(Arrays.asList(nodes));
    }
    public static List<Node> kahnDagSort(List<Node> nodes){
        return KahnDagSort.sort(nodes,Node::getSources);
    }
    public static List<Node> old_kahnDagSort(List<Node> nodes){
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
        Queue<Node> q = new ArrayDeque<>();
        //using reveresed to preserve order
        nodes.reversed().forEach(n->{
            if(inDegrees.get(n.getFqdn()).get()==0){
                q.offer(n);
            }
        });
        List<Node> rtrn = new ArrayList<>();
        while(!q.isEmpty()){
            Node n = q.poll();
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
        return KahnDagSort.isCircular(this,Node::getSources);
    }


}
