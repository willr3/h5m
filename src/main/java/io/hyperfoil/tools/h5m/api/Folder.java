package io.hyperfoil.tools.h5m.api;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "A folder containing uploaded data")
public record Folder(
        @Schema(description = "Unique folder ID") Long id,
        @Schema(description = "Folder name") @NotEmpty
        // Input hint only: the backend may still send reserved 'h5m.' names.
        @Pattern(regexp = ReservedNamespace.ALLOWED_NAME_PATTERN, message = "names starting with 'h5m.' are reserved for internal use") String name,
        @Schema(description = "Node group ID") Long groupId,
        @Schema(description = "Team Id") Long teamId,
        @Schema(description = "Team Name") String teamName){
}
