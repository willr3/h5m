package io.hyperfoil.tools.h5m.entity;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIdentityReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.persistence.*;
import org.hibernate.annotations.BatchSize;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Entity(name = "value")
public class ValueEntity extends PanacheEntity {

    @Column(columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    @Basic(fetch = FetchType.LAZY)
    public JsonNode data;

    //not yet used but the idea is to sort multiple values based on idx to preserve node output order for next nodes input
    public int idx;

    @ManyToOne(fetch = FetchType.LAZY   )
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "node_id")
    @JsonIdentityReference(alwaysAsId = true)
    public NodeEntity node;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "folder_id")
    @JsonIdentityReference(alwaysAsId = true)
    public FolderEntity folder;

    @CreationTimestamp
    @Column(updatable = false) // updatable = false ensures it's set only once
    private LocalDateTime createdAt;

    private LocalDateTime lastUpdated;


    public LocalDateTime getCreatedAt() {return createdAt;}

    public  LocalDateTime getLastUpdated() {return lastUpdated;}

    public Long getId(){return id;}

    //cannot cascade delete because this entity "owns" the reference to the parent values
    @ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.MERGE }, fetch = FetchType.LAZY )
    @JoinTable(
            name="value_edge",
            joinColumns = @JoinColumn(name = "child_id"),
            inverseJoinColumns = @JoinColumn(name = "parent_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"child_id", "parent_id"})
    )
    @OrderColumn(name = "idx")
    @BatchSize(size = 25)
    public List<ValueEntity> sources;

    public ValueEntity(){
        this.sources = new ArrayList<>();
    }
    public ValueEntity(FolderEntity folder, NodeEntity node){
        this();
        this.folder = folder;
        this.node = node;
    }
    public ValueEntity(FolderEntity folder, NodeEntity node,JsonNode data){
        this();
        this.folder = folder;
        this.node = node;
        this.data = data;
    }

    @RegisterForReflection
    public record DataProjection(JsonNode data) {} // field names must match with entity

    public String getPath(){
        String prefix = node.getId()+"="+idx;
        String suffix = (sources!=null ? ( ","+sources.stream().map(v->{
            return v.getPath();
        }).collect(Collectors.joining(","))) : "");
        return prefix+suffix;
    }

    @PreUpdate
    @PrePersist
    public void preUpdate() {
        this.lastUpdated =  LocalDateTime.now();
    }

    public List<ValueEntity> getSources() {return this.sources;}

    @Override
    public String toString(){return "ValueEntity< id="+id+" node.id="+node.id+" idx="+idx+" >";}


    @Override
    public int hashCode(){
        return Objects.hash(id,node,/*sources,*/folder);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof ValueEntity v){
            if(this.id!=null && v.id!=null) {
                return Objects.equals(v.id, this.id);
            }
            if(Objects.equals(this.node, v.node) && Objects.equals(this.folder, v.folder)  && Objects.equals(this.sources, v.sources)){
                return true;
            }
        }
        return false;
    }

    public boolean dependsOn(ValueEntity source){
        if(source == null) return false;
        Queue<ValueEntity> queue = new ArrayDeque<>(sources);
        boolean result = false;
        while(!queue.isEmpty() && !result){
            ValueEntity value = queue.poll();
            result = value.equals(source);
            if(!result){
                queue.addAll(value.sources);
            }
        }
        return result;
    }
}
