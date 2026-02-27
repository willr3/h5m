package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.Node;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue("ed")
public class EDivisive extends Node {
    @Override
    protected Node shallowCopy() {
        return new EDivisive();
    }
}
