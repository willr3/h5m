package io.hyperfoil.tools.h5m.rest;

import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.api.RecalculationStatus;
import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.h5m.api.svc.NodeServiceInterface;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ProcessingTrackerEntity;
import io.hyperfoil.tools.h5m.api.ProcessingType;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.h5m.svc.ValueService;
import io.quarkus.security.Authenticated;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import java.util.Objects;

@Path("/api/node")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Node", description = "Manage transformation nodes in the DAG pipeline")
public class NodeResource {

    @Inject
    NodeServiceInterface nodeService;

    @Inject
    NodeService nodeServiceImpl; // for update() which is not on the interface

    @Inject
    FolderServiceInterface folderService;

    @Inject
    ValueService valueService;

    @POST
    @Authenticated
    @Operation(description = "Create a new node with an operation")
    public Long createNode(
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
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("Invalid node configuration request: " + e.getMessage(), e);
        }
    }

    public record NodeUpdateRequest(String name, String operation) {}
    public record NodeUpdateResponse(long nodeId, RecalculationStatus recalculation) {}

    @PUT
    @Path("{id}")
    @Authenticated
    @Transactional
    @Operation(description = "Update a node's name and/or operation. If the operation changes, triggers selective recalculation.")
    public NodeUpdateResponse update(@PathParam("id") Long id, NodeUpdateRequest request) {
        NodeEntity existing = NodeEntity.findById(id);
        if (existing == null) {
            throw new NotFoundException("Node not found: " + id);
        }
        boolean operationChanged = request.operation != null && !Objects.equals(existing.operation, request.operation);

        // Apply updates to the existing entity
        if (request.name != null) {
            existing.name = request.name;
        }
        if (request.operation != null) {
            existing.operation = request.operation;
        }
        nodeServiceImpl.update(existing);

        // Trigger recalculation if the operation changed. recalculateNode()
        // opens a requiringNew transaction that only reads the node's group
        // and folder structure — not the operation. The actual operation is
        // read later by async Work executors, after this @Transactional
        // method returns and the update is committed.
        RecalculationStatus status = null;
        if (operationChanged && existing.group != null) {
            status = folderService.recalculateNode(id).toStatus();
        }

        return new NodeUpdateResponse(id, status);
    }

    @POST
    @Path("{id}/recalculate")
    @Authenticated
    @Operation(description = "Recalculate a specific node and its dependents. Returns immediately with a status for progress polling.")
    public RecalculationStatus recalculateNode(@PathParam("id") Long nodeId) {
        return folderService.recalculateNode(nodeId).toStatus();
    }

    @DELETE
    @Path("{id}")
    @Authenticated
    @Operation(description = "Delete a node by its ID")
    public void deleteNode(@PathParam("id") Long nodeId) {
        nodeService.delete(nodeId);
    }

    @PUT
    @Path("{id}/ephemeral")
    @Authenticated
    @Transactional
    @Operation(description = "Set ephemeral mode: DISCARD=discard data, KEEP=keep data, AUTO=system decides based on children")
    public void setEphemeral(
            @PathParam("id") Long nodeId,
            @QueryParam("mode") @Parameter(description = "DISCARD, KEEP, or AUTO") @DefaultValue("DISCARD") String mode) {
        NodeEntity node = NodeEntity.findById(nodeId);
        if (node == null) {
            throw new NotFoundException("Node not found: " + nodeId);
        }
        EphemeralMode ephemeralMode;
        try {
            ephemeralMode = EphemeralMode.valueOf(mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("Invalid mode: " + mode + ". Use DISCARD, KEEP, or AUTO");
        }
        if (ephemeralMode == EphemeralMode.DISCARD && (node.isDetection() || node.type() == NodeType.ROOT)) {
            throw new BadRequestException("Detection and root nodes cannot be set to ephemeral");
        }
        node.ephemeral = ephemeralMode;
        if (ephemeralMode == EphemeralMode.DISCARD) {
            // Only nullify existing data if no uploads are in progress for this folder
            FolderEntity folder = FolderEntity.find("group.id", node.group.id).firstResult();
            long inFlight = folder != null
                    ? ProcessingTrackerEntity.count("type = ?1 and folderId = ?2 and completed = false", ProcessingType.UPLOAD, folder.id)
                    : 0;
            if (inFlight == 0) {
                valueService.nullifyNodeData(nodeId);
            }
            // If uploads are in progress, data will be nullified when they complete
            // via nullifyEphemeralData() in FolderService.upload() whenComplete callback
        }
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
