package io.hyperfoil.tools.h5m.entity.work;

import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.RelativeDifference;
import io.hyperfoil.tools.h5m.svc.WorkService;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.persistence.*;
import org.hibernate.annotations.BatchSize;

import java.util.*;
import java.util.stream.Collectors;


@Entity(name = "work")
@DiscriminatorColumn(name = "type")
@DiscriminatorValue("node")
//cross test comparison could use sourceNodes and not have an activeNode?
//custom post nodegroup actions could have sourceNodes without activeNode
public class Work  extends PanacheEntity implements Runnable, Comparable<Work>{

    @BatchSize(size=10)
    @ManyToMany(cascade = {CascadeType.PERSIST}, fetch = FetchType.LAZY)
    @JoinTable(
            name="work_values",
            joinColumns = @JoinColumn(name = "work_id"),
            inverseJoinColumns = @JoinColumn(name = "value_id"),
            indexes = {
                    @Index(name = "work_values_work_id", columnList = "work_id")
            }
    )
    public List<ValueEntity> sourceValues;//multiple values could happen for cross test comparisons and

    @BatchSize(size=10)
    @ManyToMany(cascade = {CascadeType.PERSIST}, fetch = FetchType.LAZY)
    @JoinTable(
            name="work_source_nodes",
            joinColumns = @JoinColumn(name = "work_id"),
            inverseJoinColumns = @JoinColumn(name = "node_id"),
            indexes = {
                    @Index(name = "work_nodes_work_id", columnList = "work_id")
            }
    )
    public List<NodeEntity> sourceNodes; //what is going to use a list of sources that are not already listed for the activeNode?

    public int retryCount;

    @BatchSize(size=10)
    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE}, fetch = FetchType.EAGER)
    @JoinTable(
            name="work_active_nodes",
            joinColumns = @JoinColumn(name = "work_id"),
            inverseJoinColumns = @JoinColumn(name = "node_id")
    )
    public Set<NodeEntity> activeNodes;

    public boolean cumulative = false;


    public Work(){
        retryCount = 0;
    }
    public Work(NodeEntity activeNode,List<NodeEntity> sourceNodes,List<ValueEntity> sourceValues){
        this(Set.of(activeNode),sourceNodes,sourceValues);
    }
    public Work(Set<NodeEntity> activeNodes,List<NodeEntity> sourceNodes,List<ValueEntity> sourceValues){
        this();
        this.activeNodes = new HashSet<>(activeNodes); //so that it will be mutable
        if(activeNodes.stream().anyMatch(node -> node instanceof RelativeDifference)){
            this.cumulative = true;
        }
        this.sourceValues = sourceValues == null ? Collections.emptyList() : new ArrayList(sourceValues);
        this.sourceNodes = sourceNodes == null ? Collections.emptyList() : new ArrayList(sourceNodes);
    }

    public Set<NodeEntity> getActiveNodes() {
        return activeNodes;
    }

    public void setActiveNodes(Set<NodeEntity> activeNodes) {
        this.activeNodes = activeNodes;
        if(activeNodes.stream().anyMatch(node -> node instanceof RelativeDifference)){
            this.cumulative = true;
        }else{
            this.cumulative = false;
        }
    }

    //work A depends on work B if A.activeNode depends on B.activeNode or
    //needs reviewing for the value dependency
    public boolean dependsOn(Work work){

        if(work == null || work.activeNodes == null || work.activeNodes.isEmpty()){
            return false;
        }
        for (int i = 0, size = sourceValues.size(); i < size; i++) {
            ValueEntity sourceValue = sourceValues.get(i);
            if (work.activeNodes.stream().anyMatch(sourceValue.node::dependsOn)) {
                for (int j = 0, wSize = work.sourceValues.size(); j < wSize; j++) {
                    if (sourceValue.dependsOn(work.sourceValues.get(j))) {
                        return true;
                    }
                }
            }
        }
        if (this.activeNodes == null || this.activeNodes.isEmpty() || !this.activeNodes.stream().anyMatch(t->work.activeNodes.stream().anyMatch(t::dependsOn))) {
            return false;
        }
        if (cumulative || sourceValues.isEmpty()) {
            return true;
        }
        for (int i = 0, size = sourceValues.size(); i < size; i++) {
            if (work.sourceValues.contains(sourceValues.get(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * filters works for any Work that are in the dependsOn path for this
     * @param works
     * @return
     */
    public List<Work> getAncestorWorks(List<Work> works){
        List<Work> rtrn = new ArrayList<>();
        for (Work w : works) {
            if (this.dependsOn(w)) {
                rtrn.add(w);
            }
        }
        return rtrn;
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof Work){
            Work work = (Work)o;
            boolean sameNodes =  this.activeNodes.containsAll(work.activeNodes) && work.activeNodes.containsAll(this.activeNodes);  //Objects.equals(this.activeNode, work.activeNode);
            if(!sameNodes){
                return false;
            }
            if (this.sourceNodes.size() != work.sourceNodes.size()) {
                return false;
            }
            for (int i = 0; i < sourceNodes.size(); i++) {
                if (!sourceNodes.get(i).equals(work.sourceNodes.get(i))) {
                    return false;
                }
            }
            if(cumulative){
                return true;
            }
            if (this.sourceValues.size() != work.sourceValues.size()) {
                return false;
            }
            for (int i = 0; i < sourceValues.size(); i++) {
                if (!sourceValues.get(i).equals(work.sourceValues.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    @Override
    public int hashCode(){
        List<Object> param = new ArrayList<>();
        param.addAll(activeNodes);
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

    @Override public void run() {
        CDI.current().select(WorkService.class).get().execute(this);
    }

    @Override
    public String toString() {
        return "Work<id="+id+" activeNodes="+activeNodes+
                " sourceNodes="+sourceNodes.stream().map(n->""+n.getId()).collect(Collectors.joining(","))+
                " sourceValues="+sourceValues.stream().map(v->""+v.getId()).collect(Collectors.joining(","))+
                " retry="+retryCount+
                " hashCode="+hashCode()+" >";
    }
}
