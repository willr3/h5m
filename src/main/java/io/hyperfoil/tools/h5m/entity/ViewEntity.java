package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

@Entity(name = "folder_view")
@Table(uniqueConstraints = @UniqueConstraint(columnNames = {"folder_id", "name"}))
public class ViewEntity extends PanacheEntity {

    @NotNull
    public String name;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "folder_id")
    public FolderEntity folder;

    @OneToMany(mappedBy = "view", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("headerOrder ASC")
    public List<ViewComponentEntity> components = new ArrayList<>();

    public ViewEntity() {}

    public ViewEntity(String name, FolderEntity folder) {
        this.name = name;
        this.folder = folder;
    }

    @Override
    public String toString() {
        return "ViewEntity<" + id + ">[ name=" + name + ", components=" + components.size() + " ]";
    }
}
