package exp.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import org.hibernate.annotations.BatchSize;
import org.hibernate.annotations.Immutable;

import java.util.*;

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
    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinTable(
            name="work_values",
            joinColumns = @JoinColumn(name = "work_id"),
            inverseJoinColumns = @JoinColumn(name = "value_id")
    )
    public List<Value> sourceValues;//multiple values could happen for cross test comparisons and

    @BatchSize(size=10)
    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
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
        this.sourceValues = sourceValues == null ? Collections.emptyList() : new ArrayList(sourceValues);
        this.sourceNodes = sourceNodes == null ? Collections.emptyList() : new ArrayList(sourceNodes);
    }

    public Node getActiveNode() {
        return activeNode;
    }

    public void setActiveNode(Node activeNode) {
        this.activeNode = activeNode;
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
    public int compareTo(Work o) {
        if(this.dependsOn(o)){
            return 1;
        } else if (o.dependsOn(this)){
            return -1;
        } else {
            return 0;
        }
    }
}
