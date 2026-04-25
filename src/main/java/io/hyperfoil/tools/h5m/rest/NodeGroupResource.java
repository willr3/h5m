package io.hyperfoil.tools.h5m.rest;

import io.hyperfoil.tools.h5m.api.NodeGroup;
import io.hyperfoil.tools.h5m.api.svc.NodeGroupServiceInterface;
import io.quarkus.security.Authenticated;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/group")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "NodeGroup", description = "Manage node groups (transformation pipelines)")
public class NodeGroupResource {

    @Inject
    NodeGroupServiceInterface nodeGroupService;

    @GET
    @Path("{name}")
    @PermitAll
    @Operation(description = "Retrieve a node group by its name")
    public NodeGroup byName(@PathParam("name") String groupName) {
        return nodeGroupService.byName(groupName);
    }

    @DELETE
    @Path("{id}")
    @Authenticated
    @Operation(description = "Delete a node group by its ID")
    public void delete(@PathParam("id") Long groupId) {
        nodeGroupService.delete(groupId);
    }
}
