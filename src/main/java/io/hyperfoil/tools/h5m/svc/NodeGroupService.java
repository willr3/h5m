package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.NodeGroup;
import io.hyperfoil.tools.h5m.api.svc.NodeGroupServiceInterface;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.mapper.CycleAvoidingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class NodeGroupService implements NodeGroupServiceInterface {

    @Inject
    EntityManager em;

    @Inject
    ApiMapper apiMapper;

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

    @Override
    @Transactional
    public NodeGroup byId(Long id){
        return apiMapper.toNodeGroup(NodeGroupEntity.findById(id), new CycleAvoidingContext());
    }

    @Override
    @Transactional
    public NodeGroup byName(String name){
        return apiMapper.toNodeGroup(NodeGroupEntity.find("name",name).firstResult(), new CycleAvoidingContext());
    }

    @Transactional
    public long update(NodeGroupEntity group){
        if(group.id == null || group.id == -1){

        }else{
            group.persist();
        }
        return group.id;
    }
    @Override
    @Transactional
    public void delete(Long groupId){
        if(groupId != null){
            NodeGroupEntity.deleteById(groupId);
        }
    }
}
