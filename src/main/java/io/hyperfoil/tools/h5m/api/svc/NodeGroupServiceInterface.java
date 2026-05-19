package io.hyperfoil.tools.h5m.api.svc;

import io.hyperfoil.tools.h5m.api.NodeGroup;

/**
 * Service interface for managing NodeGroups.
 */
public interface NodeGroupServiceInterface {

    /**
     * Retrieves a node group by its ID.
     *
     * @param id The ID of the node group.
     * @return The node group with the given ID.
     */
    NodeGroup byId(Long id);

    /**
     * Retrieves a node group by its name.
     *
     * @param groupName The name of the node group.
     * @return The node group with the given name.
     */
    NodeGroup byName(String groupName);

    /**
     * Deletes a node group by its ID.
     *
     * @param groupId The ID of the node group to delete.
     */
    void delete(Long groupId);

}
