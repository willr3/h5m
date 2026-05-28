package io.hyperfoil.tools.h5m.rest;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.api.View;
import io.hyperfoil.tools.h5m.api.svc.ViewServiceInterface;
import io.quarkus.security.Authenticated;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;

@Path("/api/folder/{name}/view")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "View", description = "Manage views for folder data presentation")
public class ViewResource {

    @Inject
    ViewServiceInterface viewService;

    @GET
    @Path("/")
    @PermitAll
    @Operation(description = "List all views for a folder")
    public List<View> getViews(@PathParam("name") String folderName) {
        return viewService.getViews(folderName);
    }

    @GET
    @Path("/{viewId}")
    @PermitAll
    @Operation(description = "Get a view definition")
    public View getView(@PathParam("name") String folderName, @PathParam("viewId") Long viewId) {
        return viewService.getView(viewId);
    }

    @POST
    @Authenticated
    @Operation(description = "Create a new view for a folder")
    public View createView(@PathParam("name") String folderName, View view) {
        return viewService.createView(folderName, view);
    }

    @PUT
    @Path("/{viewId}")
    @Authenticated
    @Operation(description = "Update a view")
    public View updateView(@PathParam("name") String folderName, @PathParam("viewId") Long viewId, View view) {
        return viewService.updateView(viewId, view);
    }

    @DELETE
    @Path("/{viewId}")
    @Authenticated
    @Operation(description = "Delete a view (cannot delete the Default view)")
    public void deleteView(@PathParam("name") String folderName, @PathParam("viewId") Long viewId) {
        viewService.deleteView(viewId);
    }

    @GET
    @Path("/{viewId}/data")
    @PermitAll
    @Operation(description = "Get filtered pivoted data for a view")
    public List<JsonNode> getViewData(@PathParam("name") String folderName, @PathParam("viewId") Long viewId) {
        return viewService.getViewData(folderName, viewId);
    }
}
