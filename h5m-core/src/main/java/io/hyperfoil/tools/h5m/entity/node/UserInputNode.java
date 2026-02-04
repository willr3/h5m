package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.Node;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue("user")
public class UserInputNode extends Node {

    public UserInputNode(){}
    public UserInputNode(String name,String operation){
        super(name,operation);
    }

    @Override
    protected Node shallowCopy() {
        return new UserInputNode(name,operation);
    }
}
