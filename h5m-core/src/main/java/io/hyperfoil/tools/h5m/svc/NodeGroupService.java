package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.entity.NodeGroup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class NodeGroupService {

    @Inject
    EntityManager em;

    @Transactional
    public long create(NodeGroup group){
        if(!group.isPersistent()){
            group.id = null;
            em.persist(group);
            em.flush();
        }
        return group.id;
    }

    @Transactional
    public NodeGroup read(long id){
        return NodeGroup.findById(id);
    }

    @Transactional
    public NodeGroup byName(String name){
        return (NodeGroup) NodeGroup.find("name",name).firstResult();
    }

    @Transactional
    public long update(NodeGroup group){
        if(group.id == null || group.id == -1){

        }else{
            group.persist();
        }
        return group.id;
    }

    @Transactional
    public void delete(NodeGroup group){
        if(group.id != null){
            group.delete();
        }
    }
}
