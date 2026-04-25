package io.hyperfoil.tools.h5m.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.api.svc.NodeServiceInterface;
import io.quarkus.security.Authenticated;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;

@Path("/api/node")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Node", description = "Manage transformation nodes in the DAG pipeline")
public class NodeResource {

    @Inject
    NodeServiceInterface nodeService;

    @POST
    @Authenticated
    @Operation(description = "Create a new node with an operation")
    public Long create(
            @QueryParam("name") @NotEmpty String name,
            @QueryParam("groupId") @NotNull Long groupId,
            @QueryParam("type") @NotNull NodeType type,
            @QueryParam("operation") String operation) {
        return nodeService.create(name, groupId, type, operation);
    }

    @POST
    @Path("configured")
    @Authenticated
    @Operation(description = "Create a new node with sources and configuration")
    public Long createConfigured(
            @QueryParam("name") @NotEmpty String name,
            @QueryParam("groupId") @NotNull Long groupId,
            @QueryParam("type") @NotNull NodeType type,
            @QueryParam("sources") @NotNull @NotEmpty List<Long> sources,
            Object configuration) {
        try {
            return nodeService.createConfigured(name, groupId, type, sources, configuration);
        } catch (JsonProcessingException e) {
            throw new BadRequestException("Invalid node configuration payload", e);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("Invalid node configuration request: " + e.getMessage(), e);
        }
    }

    @DELETE
    @Path("{id}")
    @Authenticated
    @Operation(description = "Delete a node by its ID")
    public void delete(@PathParam("id") Long nodeId) {
        nodeService.delete(nodeId);
    }

    @GET
    @Path("find")
    @PermitAll
    @Operation(description = "Find nodes by FQDN within a specific group")
    public List<Node> findNodeByFqdn(
            @QueryParam("name") @Parameter(description = "FQDN of the node") String name,
            @QueryParam("groupId") @Parameter(description = "Group ID to search within") Long groupId) {
        return nodeService.findNodeByFqdn(name, groupId);
    }
}
