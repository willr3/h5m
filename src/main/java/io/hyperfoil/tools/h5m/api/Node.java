package io.hyperfoil.tools.h5m.api;

import java.util.List;
import java.util.Objects;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "A transformation node in the DAG pipeline")
public record Node(
        @Schema(description = "Unique node ID") Long id,
        @Schema(description = "Node name") String name,
        @Schema(description = "Fully qualified domain name") String fqdn,
        @Schema(description = "Node type") NodeType type,
        @Schema(description = "Parent node group") NodeGroup group,
        @Schema(description = "Node operation (jq filter, JS function, etc.)") String operation,
        @Schema(description = "Source dependency nodes") List<Node> sources) {

    @Override
    public boolean equals(Object o) {
        if (o instanceof Node n) {
            if (id != null && n.id != null) {
                return id.equals(n.id);
            }
            return Objects.equals(name, n.name)
                    && Objects.equals(type, n.type)
                    && Objects.equals(operation, n.operation)
                    && Objects.equals(group != null ? group.id() : null, n.group != null ? n.group.id() : null);
        }
        return false;
    }

    @Override
    public int hashCode(){
        return Objects.hash(id,name,type,group!=null ? group.id() : null,operation,sources);
    }

}
