package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.NodeEntity;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

import java.util.List;


/**
 * a node that stores the unique value combination of the source nodes. This node is not created by users but is used by change detection.
 */
@Entity
@DiscriminatorValue("fp")
public class FingerprintNode extends NodeEntity {

    public FingerprintNode() {

    }
    public FingerprintNode(String name, String operation){
        super(name,operation);
    }
    public FingerprintNode(String name, String operation, List<NodeEntity> sources) {
        super(name,operation,sources);
    }

    @Override
    protected NodeEntity shallowCopy() {
        return new FingerprintNode(name,operation);
    }
}
