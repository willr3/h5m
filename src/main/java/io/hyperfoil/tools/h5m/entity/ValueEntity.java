package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import org.hibernate.annotations.BatchSize;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.Mutability;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.java.Immutability;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Entity(name = "value")
@Table(indexes = {
    @Index(name = "idx_value_node_id", columnList = "node_id"),
    @Index(name = "idx_value_folder_id", columnList = "folder_id")
})
@Immutable
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_ONLY)
public class ValueEntity extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    @Basic(fetch = FetchType.LAZY)
    @Mutability(Immutability.class)
    public JqValue data;

    //not yet used but the idea is to sort multiple values based on idx to preserve node output order for next nodes input
    public int idx;

    @ManyToOne(fetch = FetchType.LAZY   )
    @NotNull
    public NodeEntity node;

    @ManyToOne(fetch = FetchType.LAZY)
    public FolderEntity folder;

    @CreationTimestamp
    @Column(updatable = false) // updatable = false ensures it's set only once
    private LocalDateTime createdAt;

    private LocalDateTime lastUpdated;


    public LocalDateTime getCreatedAt() {return createdAt;}

    public  LocalDateTime getLastUpdated() {return lastUpdated;}

    public Long getId(){return id;}

    //cannot cascade delete because this entity "owns" the reference to the parent values
    @ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.MERGE }, fetch = FetchType.EAGER )
    @JoinTable(
            name="value_edge",
            joinColumns = @JoinColumn(name = "child_id"),
            inverseJoinColumns = @JoinColumn(name = "parent_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"child_id", "parent_id"}),
            indexes = @Index(name = "idx_value_edge_parent", columnList = "parent_id")
    )
    @OrderColumn(name = "idx")
    public List<ValueEntity> sources;

    public ValueEntity(){
        this.sources = new ArrayList<>();
    }
    public ValueEntity(FolderEntity folder, NodeEntity node){
        this();
        this.folder = folder;
        this.node = node;
    }
    public ValueEntity(FolderEntity folder, NodeEntity node, JqValue data){
        this();
        this.folder = folder;
        this.node = node;
        this.data = data;
    }
    public ValueEntity(FolderEntity folder, NodeEntity node, JqValue data, List<ValueEntity> sources){
        this.sources = new ArrayList<>(sources);
        this.folder = folder;
        this.node = node;
        this.data = data;
    }

    @RegisterForReflection
    public record DataProjection(JqValue data) {} // field names must match with entity

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
