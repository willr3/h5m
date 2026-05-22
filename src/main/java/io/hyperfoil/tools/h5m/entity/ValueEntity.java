package io.hyperfoil.tools.h5m.entity;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIdentityReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.provided.H5mEntityListener;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import org.hibernate.annotations.BatchSize;
import jakarta.persistence.CascadeType;
import org.hibernate.annotations.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.Mutability;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.java.Immutability;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Entity(name = "value")
@Table(indexes = {
    //@Index(name = "idx_value_node_id", columnList = "node_id"),
    //@Index(name = "idx_value_folder_id", columnList = "folder_id")
})
@EntityListeners( H5mEntityListener.class )
public class ValueEntity extends PanacheEntity {

    @Column(columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    @Basic(fetch = FetchType.LAZY)
    @Mutability(Immutability.class)
    public JsonNode data;

    //not yet used but the idea is to sort multiple values based on idx to preserve node output order for next nodes input
    public int idx;

    @ManyToOne(fetch = FetchType.LAZY   )
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "node_id")
    @JsonIdentityReference(alwaysAsId = true)
    @NotNull
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
            uniqueConstraints = @UniqueConstraint(columnNames = {"child_id", "parent_id", "depth"})
            //indexes = @Index(name = "idx_value_edge_parent", columnList = "parent_id")
    )
    @OrderColumn(name = "idx")
    @SQLInsert(sql="""
        WITH insert_edge (child_id,idx,parent_id) AS (VALUES(?,?,?)),
        found AS (
            SELECT l.parent_id, r.child_id, l.depth + r.depth + 1 AS depth, r.count AS count, r.idx AS idx
            FROM
                ( SELECT ve.parent_id, ve.depth FROM value_edge ve JOIN insert_edge ie ON ve.child_id = ie.parent_id) l
            CROSS JOIN
                ( SELECT ve.child_id, ve.depth, ie.idx, ve.count FROM value_edge ve JOIN insert_edge ie ON ve.parent_id = ie.child_id) r
        )
        INSERT INTO value_edge (parent_id,child_id,idx,depth,count)
        SELECT parent_id,child_id,idx,depth,count FROM found
        WHERE true
        ON CONFLICT (child_id, parent_id, depth)
        DO UPDATE SET count = value_edge.count + 1
        """)
    @SQLDeleteAll(sql = """
        delete from value_edge where child_id = ? and parent_id != child_id
        """)
    @SQLDelete(sql = """
        with from_hibernate_delete (child_id,idx) as ( values(?,?) ),
        target_edge as (select ve.child_id,ve.parent_id,ve.depth,ve.count from value_edge ve join from_hibernate_delete fh on ve.child_id = fh.child_id and ve.idx = fh.idx where ve.depth = 1),
        selected_edge as (
          select l.parent_id as parent_id, r.child_id as child_id, l.depth + r.depth + 1 as depth
          from ( select ve.* from value_edge ve join target_edge te on ve.child_id = te.parent_id) l
          cross join
          ( select ve.* from value_edge ve join target_edge te on ve.parent_id = te.child_id) r
        )
        update value_edge set count = count - 1 where (parent_id,child_id,depth) in (select parent_id, child_id, depth from selected_edge);
    """)
    @SQLUpdate(sql= """
        with from_hibernate_update (parent_id,child_id,idx) as (values(?,?,?)),
        existing (parent_id,child_id,idx) as (select ne.parent_id,ne.child_id,ne.idx from vale_edge ne join from_hibernate_update hu on ne.child_id = hu.child_id and ne.idx = hu.idx where ne.depth = 1),
        --delete the existing 
        delete_target_edge as (select ne.child_id,ne.parent_id,ne.depth,ne.count from value_edge ne join existing e on ne.child_id = e.child_id and ne.idx = e.idx where ne.depth = 1),
        delete_selected_edge as (
          select l.parent_id as parent_id, r.child_id as child_id, l.depth + r.depth + 1 as depth
          from ( select ne.* from value_edge ne join delete_target_edge te on ne.child_id = te.parent_id) l
          cross join
          ( select ne.* from value_edge ne join delete_target_edge te on ne.parent_id = te.child_id) r
        ),
        delete_rows (parent_id,child_id,idx,depth,count) as (select parent_id,child_id,idx,depth,count-1 as count from value_edge where (parent_id,child_id,depth) in (select parent_id, child_id, depth from delete_selected_edge)),
        insert_found (parent_id,child_id,idx,depth,count) AS (
            SELECT l.parent_id, r.child_id, r.idx AS idx, l.depth + r.depth + 1 AS depth, r.count AS count
            FROM
                ( SELECT ne.parent_id, ne.depth FROM value_edge ne JOIN from_hibernate_update fh ON ne.child_id = fh.parent_id) l
            CROSS JOIN
                ( SELECT ne.child_id, ne.depth, fh.idx, ne.count FROM value_edge ne JOIN from_hibernate_update fh ON ne.parent_id = fh.child_id) r
        )
        INSERT INTO value_edge (parent_id,child_id,idx,depth,count)
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
    @SQLJoinTableRestriction("depth = 1")
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
    public ValueEntity(FolderEntity folder, NodeEntity node,JsonNode data,List<ValueEntity> sources){
        this.sources = new ArrayList<>(sources);
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
