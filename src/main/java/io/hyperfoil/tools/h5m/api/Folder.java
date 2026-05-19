package io.hyperfoil.tools.h5m.api;

import jakarta.validation.constraints.NotEmpty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "A folder containing uploaded data")
public record Folder(
        @Schema(description = "Unique folder ID") Long id,
        @Schema(description = "Folder name") @NotEmpty String name,
        @Schema(description = "Node group ID") Long groupId) {
}
