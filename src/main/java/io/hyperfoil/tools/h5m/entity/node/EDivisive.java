package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue("ed")
public class EDivisive extends NodeEntity {
    @Override
    protected NodeEntity shallowCopy() {
        return new EDivisive();
    }

    @Override
    public NodeType type() {
        return NodeType.EDIVISIVE;
    }


}
