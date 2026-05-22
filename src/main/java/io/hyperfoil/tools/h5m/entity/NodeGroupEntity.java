package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.entity.validation.ValidNode;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@Entity(name = "node_group")
public class NodeGroupEntity extends PanacheEntity {

    public String name;

    @OneToOne(cascade = {CascadeType.ALL}, fetch = FetchType.LAZY)
    @NotNull
    public NodeEntity root;

    @OneToMany(cascade = { CascadeType.PERSIST,
            CascadeType.MERGE }, fetch = FetchType.LAZY, orphanRemoval = false, mappedBy = "group")
    //@OnDelete(action = OnDeleteAction.CASCADE)
    public List<@NotNull @ValidNode NodeEntity> sources;

    public NodeGroupEntity(){
        this.sources = new ArrayList<>();
        this.root = new RootNode();
        //this.root.group = this;

    }
    public NodeGroupEntity(String name){
        this();
        this.name = name;
    }


    public void addNode(NodeEntity node){
        if(!sources.contains(node)){
            sources.add(node);
            node.group = this;
        }
    }


    public List<NodeEntity> getTopLevelNodes(){
        return sources.stream().filter(node -> node.sources.size() == 1 && node.sources.contains(root)).collect(Collectors.toList());
    }

    /**
     * this would check that the fully qualified names of the group's nodes do not conflict with the current list of nodes.
     * We do not use this atm becasue unique names is not a strict requirement. The UI would have to get users to pick the correct node
     * if a name conflict exists.
     * @param group
     * @return
     */
    public boolean canLoad(NodeGroupEntity group){
        Set<String> fqdn = sources.stream().map(n->n.getFqdn()).collect(Collectors.toSet());
        return group.sources.stream().noneMatch(n->fqdn.contains(n.getFqdn()));
    }

    /**
     * Loads the node group into the current group using this groups root as the replacement for the root in the copied group
     * @param group
     */
    public void loadGroup(NodeGroupEntity group){
        root.loadGroup(group);
    }

    @PreUpdate
    @PrePersist
    public void checkNodes(){

        //ensure all nodes reference the root for this group
        sources.forEach(n->{
            if(!n.sources.isEmpty()){
                for(int i=0; i<n.sources.size(); i++){
                    if(n.sources.get(i) instanceof RootNode && !n.sources.get(i).equals(root)){
                        n.sources.remove(i);
                        n.sources.add(i,root);
                    }
                }
            }
        });
        boolean hasSources = sources.stream().anyMatch(n->!n.sources.isEmpty());
        if(hasSources){
            this.sources = NodeEntity.kahnDagSort(sources);
        }
    }


}
