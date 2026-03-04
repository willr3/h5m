package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;

@Entity(name="folder")
public class FolderEntity extends PanacheEntity {

    @Column(unique = true)
    public String name;

    @OneToOne(cascade = {CascadeType.ALL})
    public NodeGroupEntity group;

    public FolderEntity(){}
    public FolderEntity(String name){
        this.name = name;
        //TODO do we auto-create a nodeGroup?
        this.group = new NodeGroupEntity(name);
    }
    public FolderEntity(String name, NodeGroupEntity group){
        this.name = name;
        this.group = group;
    }

    @Override
    public String toString() {
        return "FolderEntity<"+id+">[ name="+name+" ]";

    }
}
