package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.NodeEntity;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue("user")
public class UserInputNode extends NodeEntity {

    public UserInputNode(){}
    public UserInputNode(String name,String operation){
        super(name,operation);
    }

    @Override
    protected NodeEntity shallowCopy() {
        return new UserInputNode(name,operation);
    }
}
