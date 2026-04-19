package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;

@Entity(name="folder")
public class FolderEntity extends PanacheEntity {

    @Column(unique = true)
    public String name;

    @OneToOne(cascade = {CascadeType.ALL})
    public NodeGroupEntity group;

    @ManyToOne(fetch = FetchType.LAZY)
    public Team team;

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof FolderEntity that)) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "FolderEntity<"+id+">[ name="+name+" ]";
    }
}
