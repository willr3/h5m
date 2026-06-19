package io.hyperfoil.tools.h5m.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

@Entity(name = "folder_view_component")
@Table(uniqueConstraints = @UniqueConstraint(columnNames = {"view_id", "header_name"}))
public class ViewComponentEntity extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "view_id")
    public ViewEntity view;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "node_id")
    public NodeEntity node;

    @NotNull
    @Column(name = "header_name")
    public String headerName;

    @Column(name = "header_order")
    public int headerOrder;

    public ViewComponentEntity() {}

    public ViewComponentEntity(ViewEntity view, NodeEntity node, String headerName, int headerOrder) {
        this.view = view;
        this.node = node;
        this.headerName = headerName;
        this.headerOrder = headerOrder;
    }

    @Override
    public String toString() {
        return "ViewComponentEntity<" + id + ">[ headerName=" + headerName
            + ", headerOrder=" + headerOrder
            + ", nodeId=" + (node != null ? node.id : null) + " ]";
    }
}
