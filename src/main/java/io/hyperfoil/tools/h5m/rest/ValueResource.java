package io.hyperfoil.tools.h5m.rest;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.api.Value;
import io.hyperfoil.tools.h5m.api.svc.ValueServiceInterface;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;

@Path("/api/value")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Value", description = "Manage computed values produced by nodes")
public class ValueResource {

    @Inject
    ValueServiceInterface valueService;

    @DELETE
    @RolesAllowed("admin")
    @Operation(description = "Purge all values")
    public void purgeValues() {
        valueService.purgeValues();
    }

    @GET
    @Path("node/{nodeId}/descendants")
    @PermitAll
    @Operation(description = "Get descendant values of a specific node")
    public List<Value> getNodeDescendantValues(@PathParam("nodeId") Long nodeId) {
        return valueService.getNodeDescendantValues(nodeId);
    }

    @GET
    @Path("node/{nodeId}/grouped")
    @PermitAll
    @Operation(description = "Get grouped values for a specific node")
    public List<JsonNode> getGroupedValues(@PathParam("nodeId") Long nodeId) {
        return valueService.getGroupedValues(nodeId);
    }
}
