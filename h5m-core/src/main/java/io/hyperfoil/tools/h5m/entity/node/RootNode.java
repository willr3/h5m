package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.Node;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue("root")
public class RootNode extends Node {

    public RootNode() {
        super();
        this.type="root";
    }
    //The root node does not shallow copy
    @Override
    protected Node shallowCopy() {
        return this;
    }
}
