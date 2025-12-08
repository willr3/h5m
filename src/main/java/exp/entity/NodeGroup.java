package exp.entity;

import exp.entity.node.RootNode;
import exp.valid.ValidNode;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@Entity
@Table(
        name = "nodegroup"
)
public class NodeGroup extends PanacheEntity {

    public String name;

    @OneToOne(cascade = {CascadeType.ALL}, fetch = FetchType.LAZY)
    @NotNull
    public Node root;

    @OneToMany(cascade = { CascadeType.PERSIST,
            CascadeType.MERGE }, fetch = FetchType.LAZY, orphanRemoval = false, mappedBy = "group")
    public List<@NotNull @ValidNode Node> sources;

    public NodeGroup(){
        this.sources = new ArrayList<>();
        this.root = new RootNode();
        this.root.group = this;

    }
    public NodeGroup(String name){
        this();
        this.name = name;
    }

    /**
     * this would check that the fully qualified names of the group's nodes do not conflict with the current list of nodes.
     * We do not use this atm becasue unique names is not a strict requirement. The UI would have to get users to pick the correct node
     * if a name conflict exists.
     * @param group
     * @return
     */
    public boolean canLoad(NodeGroup group){
        Set<String> fqdn = sources.stream().map(n->n.getFqdn()).collect(Collectors.toSet());
        return group.sources.stream().noneMatch(n->fqdn.contains(n.getFqdn()));
    }

    /**
     * Loads the node group into the current group using this groups root as the replacement for the root in the copied group
     * @param group
     */
    public void loadGroup(NodeGroup group){
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
            this.sources = Node.kahnDagSort(sources);
        }
    }


}
