package io.hyperfoil.tools.h5m.api;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "A single column definition in a view")
public record ViewComponent(
        @Schema(description = "Unique component ID") Long id,
        @Schema(description = "Node ID whose values this column displays") Long nodeId,
        @Schema(description = "Node name") String nodeName,
        @Schema(description = "Node type") String nodeType,
        @Schema(description = "Column header display text") String headerName,
        @Schema(description = "Column display position (lower = left)") int headerOrder) {
}
