package io.hyperfoil.tools.h5m.rest;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.FolderSummary;
import io.hyperfoil.tools.h5m.api.RecalculationStatus;
import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.h5m.api.svc.ValueServiceInterface;
import io.hyperfoil.tools.h5m.svc.RecalculationService;
import io.hyperfoil.tools.h5m.svc.RecalculationTracker;
import io.quarkus.security.Authenticated;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import java.util.Map;

@Path("/api/folder")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Folder", description = "Manage folders for uploaded data")
public class FolderResource {

    @Inject
    FolderServiceInterface folderService;

    @Inject
    ValueServiceInterface valueService;

    @Inject
    RecalculationService recalculationService;

    @GET
    @PermitAll
    @Operation(description = "Retrieve the list of all the folders")
    public @NotNull List<Folder> listFolders() {
        return folderService.list();
    }

    @GET
    @Path("dashboard")
    @PermitAll
    @Operation(description = "Get dashboard summaries for all folders")
    public List<FolderSummary> getDashboardSummaries() {
        return folderService.getDashboardSummaries();
    }

    @GET
    @Path("{name}")
    @PermitAll
    @Operation(description = "Retrieve a folder by its name")
    public Folder byFolderName(@PathParam("name") String name) {
        return folderService.byName(name);
    }

    @GET
    @Path("count")
    @PermitAll
    @Operation(description = "Get the upload count for all folders")
    public Map<String, Integer> getFolderUploadCount() {
        return folderService.getFolderUploadCount();
    }

    @POST
    @Path("{name}")
    @Authenticated
    @Operation(description = "Create a new folder")
    public long createFolder(@PathParam("name") String name) {
        return folderService.create(name);
    }

    @DELETE
    @Path("{name}")
    @Authenticated
    @Operation(description = "Delete a folder by its name")
    public long deleteFolder(@PathParam("name") String name) {
        return folderService.delete(name);
    }

    @POST
    @Path("{name}/upload")
    @Authenticated
    @Operation(description = "Upload JSON data to a folder. Returns immediately with an uploadId.")
    public long upload(
            @PathParam("name") String name,
            @QueryParam("path") @Parameter(description = "Path within the folder") String path,
            JqValue data) {
        if (data == null) {
            throw new BadRequestException("Missing request body");
        }
        return folderService.upload(name, path, data).uploadId;
    }

    @GET
    @Path("/recalculation/{id}")
    @Authenticated
    @Operation(description = "Get the progress of a recalculation operation.")
    public RecalculationStatus getRecalculationStatus(@PathParam("id") String id) {
        RecalculationTracker tracker = recalculationService.get(id);
        if (tracker == null) {
            throw new NotFoundException("Recalculation not found: " + id);
        }
        return tracker.toStatus();
    }

    @GET
    @Path("{name}/structure")
    @PermitAll
    @Operation(description = "Get the structural representation of a folder")
    public JqValue structure(@PathParam("name") String name) {
        return folderService.structure(name);
    }

    @GET
    @Path("{id}/labelValues")
    @PermitAll
    @Operation(description = "Get metrics labels Values")
    public List<JqValue>getLabelValues(
                    @PathParam("id") Long folderId,
                    @QueryParam("groupById") Long groupById,
                    @QueryParam("nodeIds") List<Long> nodeIds,
                    @QueryParam("sortById") Long sortById)
            {
                return valueService.getLabelValues(folderId, groupById,nodeIds,sortById);
            }
}
