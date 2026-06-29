package io.hyperfoil.tools.h5m.api.svc;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.FolderSummary;
import io.hyperfoil.tools.h5m.svc.RecalculationTracker;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service interface for managing Folders.
 */
public interface FolderServiceInterface {

    /**
     * Retrieves all the folders;
     *
     * @return A list of all the folders.
     */
    List<Folder> list();

    /**
     * Retrieves a folder by its name.
     *
     * @param name The name of the folder.
     * @return The folder with the given name.
     */
    Folder byName(String name);

    /**
     * Gets the upload count for all folders.
     *
     * @return A map of folder names to their upload counts.
     */
    Map<String, Integer> getFolderUploadCount();

    /**
     * Creates a new folder with the given name.
     *
     * @param name The name of the folder to create.
     * @return The ID of the created folder.
     */
    long create(String name);

    /**
     * Deletes a folder by its name.
     *
     * @param name The name of the folder to delete.
     * @return The ID of the deleted folder.
     */
    long delete(String name);

    /**
     * Uploads data to a specific path within a folder.
     * Returns a CompletableFuture that completes when all processing
     * (including cascaded work) finishes for this upload.
     *
     * @param name The name of the folder.
     * @param path The path within the folder.
     * @param data The JSON data to upload.
     * @return A future that completes when all work for this upload is done.
     */
    CompletableFuture<Void> upload(String name, String path, JqValue data);

    /**
     * Recalculates all values in the folder by reprocessing each root value
     * through the node graph. Returns a status tracker with progress information.
     *
     * @param name The name of the folder to recalculate.
     * @return tracker with progress and completion future
     */
    RecalculationTracker recalculate(String name);

    /**
     * Selectively recalculates values for a specific node and its dependents.
     *
     * @param nodeId The ID of the node to recalculate.
     * @return tracker with progress and completion future
     */
    RecalculationTracker recalculateNode(long nodeId);

    /**
     * Retrieves the structural representation of a folder.
     *
     * @param name The name of the folder.
     * @return The JSON representation of the folder's structure.
     */
    JqValue structure(String name);

    /**
     * Retrieves dashboard summaries for all folders.
     *
     * @return A list of folder summaries with upload counts, node counts, and change counts.
     */
    List<FolderSummary> getDashboardSummaries();

    /**
     * Exports a folder's node graph to a JSON file.
     *
     * @param folderName The folder to export.
     * @param outputPath Path to write the JSON file.
     */
    void export(String folderName, Path outputPath) throws IOException;

    /**
     * Imports a folder and its node graph from a JSON file.
     *
     * @param inputPath Path to the JSON file.
     * @param overwrite If true, delete existing folder before importing.
     * @return The folder name that was imported.
     */
    String importFolder(Path inputPath, boolean overwrite) throws IOException;

}
