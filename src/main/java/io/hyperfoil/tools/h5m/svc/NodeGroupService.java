package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class NodeGroupService {

    @Inject
    EntityManager em;

    @Transactional
    public long create(NodeGroupEntity group){
        if(!group.isPersistent()){
            group.id = null;
            em.persist(group);
            em.flush();
        }
        return group.id;
    }

    @Transactional
    public NodeGroupEntity read(long id){
        return NodeGroupEntity.findById(id);
    }

    @Transactional
    public NodeGroupEntity byName(String name){
        return (NodeGroupEntity) NodeGroupEntity.find("name",name).firstResult();
    }

    @Transactional
    public long update(NodeGroupEntity group){
        if(group.id == null || group.id == -1){

        }else{
            group.persist();
        }
        return group.id;
    }

    @Transactional
    public void delete(NodeGroupEntity group){
        if(group.id != null){
            group.delete();
        }
    }
}
