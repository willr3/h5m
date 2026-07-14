package io.hyperfoil.tools.h5m.api.svc;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.h5m.api.Value;

import java.util.List;
import java.util.Map;

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
    List<JqValue> getGroupedValues(Long nodeId);

    /**
     * Retrieves grouped values for a specific node, optionally filtered to specific node IDs.
     *
     * @param nodeId The ID of the root node.
     * @param filterNodeIds Optional list of node IDs to include. If null, all nodes are included.
     * @return A list of JSON nodes representing the grouped values.
     */
    List<JqValue> getGroupedValues(Long nodeId, List<Long> filterNodeIds);

    /**
     * Retrieves all values produced by a specific node.
     *
     * @param nodeId The ID of the node.
     * @return A list of values for the given node.
     */
    List<Value> getNodeValues(Long nodeId);

    /**
         * Returns one row per upload for a folder, with each row containing the values of the requested nodes.
         * groupByNodeId and sortByNodeId are always included in every row regardless of nodeIds.
         * @param folderId Folder Id to query.
         * @param nodeIds Node IDs to include as columns (null/empty list = all nodes).
         * @param groupByNodeId Node whose value identifies the series (e.g. config fingerprint).
         * @param sortByNodeId  Node whose value orders the rows (acts as X-axis).
       */
      List<JqValue> getLabelValues(Long folderId,Long groupByNodeId, List<Long> nodeIds, Long sortByNodeId);

      List<JqValue> getGroupedValues(Long nodeId, List<Long> filterNodeIds, Map<Long,JqValue> fingerprints, Long sortByNodeId);


}
