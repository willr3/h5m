package exp.entity;

import exp.entity.node.RelativeDifference;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import org.hibernate.annotations.BatchSize;
import org.hibernate.annotations.Immutable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Entity
@Table(
        name = "work"
)
@DiscriminatorColumn(name = "type")
@DiscriminatorValue("node")
@Immutable
//cross test comparison could use sourceNodes and not have an activeNode?
//custom post nodegroup actions could have sourceNodes without activeNode

public class Work  extends PanacheEntity implements Comparable<Work>{

    @BatchSize(size=10)
    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE}, fetch = FetchType.LAZY)
    @JoinTable(
            name="work_values",
            joinColumns = @JoinColumn(name = "work_id"),
            inverseJoinColumns = @JoinColumn(name = "value_id")
    )
    public List<Value> sourceValues;//multiple values could happen for cross test comparisons and

    @BatchSize(size=10)
    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE}, fetch = FetchType.LAZY)
    @JoinTable(
            name="work_nodes",
            joinColumns = @JoinColumn(name = "work_id"),
            inverseJoinColumns = @JoinColumn(name = "node_id")
    )
    public List<Node> sourceNodes; //what is going to use a list of sources that are not already listed for the activeNode?

    public int retryCount;

    @ManyToOne
    @JoinColumn(name = "active_node_id")
    public Node activeNode;

    boolean cumulative = false;


    public Work(){
        retryCount = 0;
    }
    public Work(Node activeNode,List<Node> sourceNodes,List<Value> sourceValues){
        this();
        this.activeNode = activeNode;
        if(this.activeNode instanceof RelativeDifference){
            this.cumulative = true;
        }
        this.sourceValues = sourceValues == null ? Collections.emptyList() : new ArrayList(sourceValues);
        this.sourceNodes = sourceNodes == null ? Collections.emptyList() : new ArrayList(sourceNodes);
    }

    public Node getActiveNode() {
        return activeNode;
    }

    public void setActiveNode(Node activeNode) {
        this.activeNode = activeNode;
        if(activeNode instanceof RelativeDifference){
            this.cumulative = true;
        }else {
            this.cumulative = false;
        }
    }

    //work A depends on work B if A.activeNode depends on B.activeNode or
    //needs reviewing for the value dependency
    public boolean dependsOn(Work work){

        if(work == null || work.activeNode == null){
            return false;
        }
        boolean activeNode = this.activeNode !=null && this.activeNode.dependsOn(work.activeNode);
        boolean sameValue = sourceValues.stream().anyMatch(v->work.sourceValues.contains(v));
        boolean dependentValue = sourceValues.stream().anyMatch(sourceValue ->
                sourceValue.node.dependsOn(work.activeNode) && work.sourceValues.stream().anyMatch(sourceValue::dependsOn)) ;
        boolean foundNode = sourceNodes.stream().anyMatch(sourceNode ->
                sourceNode.dependsOn(work.activeNode));

        return dependentValue || (activeNode && (sameValue || cumulative || sourceValues.isEmpty()));
    }

    /**
     * filters works for any Work that are in the dependsOn path for this
     * @param works
     * @return
     */
    public List<Work> getAncestorWorks(List<Work> works){
        List<Work> rtrn = works.stream().filter(this::dependsOn).toList();
        return rtrn;
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof Work){
            Work work = (Work)o;
            boolean sameNode = Objects.equals(this.activeNode, work.activeNode);
            if(!sameNode){
                return false;
            }
            boolean sameSources = activeNode!=null ||
                (this.sourceNodes.size()==work.sourceNodes.size() &&
                    IntStream.range(0,sourceNodes.size()).allMatch(i->sourceNodes.get(i).equals(work.sourceNodes.get(i)))
                );
            if(!sameSources){
                return false;
            }
            if(cumulative){
                return true;
            }
            boolean sameValues = this.sourceValues.size()==work.sourceValues.size() && (
                    IntStream.range(0,sourceValues.size()).allMatch(i->sourceValues.get(i).equals(work.sourceValues.get(i)))
                    );
            return sameValues;
        }
        return false;
    }
    @Override
    public int hashCode(){
        List<Object> param = new ArrayList<>();
        param.add(activeNode);
        param.addAll(sourceNodes);
        if(!cumulative) {
            param.addAll(sourceValues);
        }
        return Objects.hash(param);
    }

    @Override
    public int compareTo(Work o) {
        if(this.dependsOn(o)){
            return 1;
        } else if (o.dependsOn(this)){
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "Work<id="+id+" activeNode="+activeNode+
                " sourceNodes="+sourceNodes.stream().map(n->""+n.getId()).collect(Collectors.joining(","))+
                " sourceValues="+sourceValues.stream().map(v->""+v.getId()).collect(Collectors.joining(","))+
                " retry="+retryCount+
                " hashCode="+hashCode()+" >";
    }
}
