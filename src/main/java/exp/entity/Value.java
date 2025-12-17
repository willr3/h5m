package exp.entity;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIdentityReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.databind.JsonNode;
import exp.pasted.JsonBinaryType;
import exp.queue.KahnDagSort;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;

import java.time.LocalDateTime;
import java.util.*;

@Entity
@Table(
        name = "value"
)
public class Value extends PanacheEntity {

    @Column(name = "data", columnDefinition = "JSON")
    @Type(JsonBinaryType.class)
    @Basic(fetch = FetchType.LAZY)
    public JsonNode data;

    //not yet used but the idea is to sort multiple values based on idx to preserve node output order for next nodes input
    @Column(name = "idx")
    public int idx;


    @ManyToOne(fetch = FetchType.LAZY   )
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "node_id")
    @JsonIdentityReference(alwaysAsId = true)
    public Node node;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "folder_id")
    @JsonIdentityReference(alwaysAsId = true)
    public Folder folder;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false) // updatable = false ensures it's set only once
    private LocalDateTime createdAt;

    public Long getId(){return id;}

    //cannot cascade delete becasue this entity "owns" the reference to the parent values
    @ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.MERGE }, fetch = FetchType.LAZY )
    @JoinTable(
            name="value_edge",
            joinColumns = @JoinColumn(name = "value_id"), // Custom join column referencing the Student entity
            inverseJoinColumns = @JoinColumn(name = "source_id")
    )
    @OrderColumn(name = "idx")
    public List<Value> sources;

    public Value(){
        this.sources = new ArrayList<>();
    }
    public Value(Folder folder, Node node){
        this();
        this.folder = folder;
        this.node = node;
    }
    public Value(Folder folder, Node node,JsonNode data){
        this();
        this.folder = folder;
        this.node = node;
        this.data = data;
    }


    @PreUpdate
    @PrePersist
    public void sortSources() {
        this.sources = KahnDagSort.sort(sources,Value::getSources);
    }

    public List<Value> getSources() {return this.sources;}

    @Override
    public String toString(){return "Value< id="+id+" node.id="+node.id+" idx="+idx+" >";}


    @Override
    public int hashCode(){
        return Objects.hash(id,node,sources,folder);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Value v){
            if(this.id!=null && v.id!=null) {
                return Objects.equals(v.id, this.id);
            }
            if(Objects.equals(this.node, v.node) && Objects.equals(this.folder, v.folder)  && Objects.equals(this.sources, v.sources)){
                return true;
            }
        }
        return false;
    }

    public boolean dependsOn(Value source){
        if(source == null) return false;
        Queue<Value> queue = new ArrayDeque<>(sources);
        boolean result = false;
        while(!queue.isEmpty() && !result){
            Value value = queue.poll();
            result = value.equals(source);
            if(!result){
                queue.addAll(value.sources);
            }
        }
        return result;
    }
}
