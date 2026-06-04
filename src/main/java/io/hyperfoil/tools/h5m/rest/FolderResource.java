package io.hyperfoil.tools.h5m.rest;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.FolderSummary;
import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.yaup.json.Json;
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
    public Folder byName(@PathParam("name") String name) {
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
    public long create(@PathParam("name") String name) {
        return folderService.create(name);
    }

    @DELETE
    @Path("{name}")
    @Authenticated
    @Operation(description = "Delete a folder by its name")
    public long delete(@PathParam("name") String name) {
        return folderService.delete(name);
    }

    @POST
    @Path("{name}/upload")
    @Authenticated
    @Operation(description = "Upload JSON data to a folder")
    public void upload(
            @PathParam("name") String name,
            @QueryParam("path") @Parameter(description = "Path within the folder") String path,
            JsonNode data) {
        folderService.upload(name, path, data);
        // The upload() now returns a CompletableFuture, but the REST endpoint
        // doesn't need to wait for it — the caller can poll for results.
        // Future enhancement: return CompletionStage<Void> for async response.
    }

    @POST
    @Path("{name}/recalculate")
    @Authenticated
    @Operation(description = "Recalculate all values in a folder")
    public void recalculate(@PathParam("name") String name) {
        folderService.recalculate(name);
    }

    @GET
    @Path("{name}/structure")
    @PermitAll
    @Operation(description = "Get the structural representation of a folder")
    public Json structure(@PathParam("name") String name) {
        return folderService.structure(name);
    }
}
