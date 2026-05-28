package io.hyperfoil.tools.h5m.api.svc;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.api.View;

import java.util.List;

/**
 * Service interface for managing Views.
 */
public interface ViewServiceInterface {

    /**
     * Lists all views for a folder.
     *
     * @param folderName The folder name.
     * @return A list of views for the folder.
     */
    List<View> getViews(String folderName);

    /**
     * Gets a single view by ID.
     *
     * @param viewId The view ID.
     * @return The view, or null if not found.
     */
    View getView(Long viewId);

    /**
     * Creates a new view for a folder.
     *
     * @param folderName The folder name.
     * @param view The view to create.
     * @return The created view with generated ID.
     */
    View createView(String folderName, View view);

    /**
     * Updates an existing view.
     *
     * @param viewId The view ID.
     * @param view The updated view data.
     * @return The updated view.
     */
    View updateView(Long viewId, View view);

    /**
     * Deletes a view. The "Default" view cannot be deleted.
     *
     * @param viewId The view ID.
     */
    void deleteView(Long viewId);

    /**
     * Gets the filtered pivoted data for a view.
     * Returns one row per upload with only the nodes referenced by the view's components.
     *
     * @param folderName The folder name.
     * @param viewId The view ID.
     * @return A list of JSON objects, one per upload.
     */
    List<JsonNode> getViewData(String folderName, Long viewId);
}
