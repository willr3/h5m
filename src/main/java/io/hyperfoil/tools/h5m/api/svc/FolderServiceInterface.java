package io.hyperfoil.tools.h5m.api.svc;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.FolderSummary;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
    Folder find(String name);

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
     * @return The created folder.
     */
    Folder create(String name);

    /**
     * Creates a new folder with the given name, assigned to a team.
     *
     * @param name The name of the folder to create.
     * @param teamId The Id of the team to assign.
     * @return The created folder.
     */
    Folder create(String name, Long teamId);

    /**
     * Deletes a folder by its ID.
     *
     * @param id The ID of the folder to delete.
     */
    void delete(long id);

    /**
     * Retrieves the structural representation of a folder.
     *
     * @param folderId The ID of the folder.
     * @return The JSON representation of the folder's structure.
     */
    JqValue structure(long folderId);

    /**
     * Retrieves dashboard summaries for all folders.
     *
     * @return A list of folder summaries with upload counts, node counts, and change counts.
     */
    List<FolderSummary> getDashboardSummaries();

    /**
     * Exports a folder's node graph to a JSON file.
     *
     * @param folderId The folder ID.
     * @param outputPath Path to write the JSON file.
     */
    void export(long folderId, Path outputPath) throws IOException;

    /**
     * Imports a folder and its node graph from a JSON file.
     *
     * @param inputPath Path to the JSON file.
     * @param overwrite If true, delete existing folder before importing.
     * @return The imported folder.
     */
    Folder importFolder(Path inputPath, boolean overwrite) throws IOException;

}
