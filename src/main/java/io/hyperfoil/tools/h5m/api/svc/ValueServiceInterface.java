package io.hyperfoil.tools.h5m.api.svc;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.api.Value;

import java.util.List;

/**
 * Service interface for managing Values.
 */
public interface ValueServiceInterface {

    /**
     * Purges all values.
     */
    void purgeValues();

    /**
     * Retrieves the descendant values of a specific node.
     *
     * @param nodeId The ID of the node.
     * @return A list of descendant values for the given node.
     */
    List<Value> getNodeDescendantValues(Long nodeId);

    /**
     * Retrieves grouped values for a specific node.
     *
     * @param nodeId The ID of the node.
     * @return A list of JSON nodes representing the grouped values.
     */
    List<JsonNode> getGroupedValues(Long nodeId);

    /**
     * Retrieves grouped values for a specific node, optionally filtered to specific node IDs.
     *
     * @param nodeId The ID of the root node.
     * @param filterNodeIds Optional list of node IDs to include. If null, all nodes are included.
     * @return A list of JSON nodes representing the grouped values.
     */
    List<JsonNode> getGroupedValues(Long nodeId, List<Long> filterNodeIds);

    /**
     * Retrieves all values produced by a specific node.
     *
     * @param nodeId The ID of the node.
     * @return A list of values for the given node.
     */
    List<Value> getNodeValues(Long nodeId);

}
