package io.hyperfoil.tools.h5m.rest;

import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.FolderSummary;
import io.hyperfoil.tools.h5m.api.Processing;
import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.h5m.api.svc.ValueServiceInterface;
import io.hyperfoil.tools.h5m.api.svc.ProcessingServiceInterface;
import io.hyperfoil.tools.h5m.api.svc.UserServiceInterface;
import io.quarkus.runtime.configuration.MemorySize;
import io.quarkus.security.Authenticated;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static jakarta.ws.rs.core.MediaType.MULTIPART_FORM_DATA;
import static java.nio.file.Files.readAllBytes;

@Path("/folder")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Folder", description = "Manage folders for uploaded data")
public class FolderResource {

    @ConfigProperty(name = "quarkus.http.limits.max-body-size")
    MemorySize maxBodySize;

    @Inject
    FolderServiceInterface folderService;

    @Inject
    ValueServiceInterface valueService;

    @Inject
    ProcessingServiceInterface processingService;

    @Inject
    UserServiceInterface userService;

    @GET
    @PermitAll
    @Operation(description = "Retrieve the list of all the folders")
    public @NotNull List<Folder> listFolders() {
        return folderService.list();
    }

    @GET
    @Path("dashboard")
    @PermitAll
    @Operation(description = "Get dashboard summaries for all folders")
    public List<FolderSummary> getDashboardSummaries() {
        return folderService.getDashboardSummaries();
    }

    @GET
    @Path("find")
    @PermitAll
    @Operation(description = "Retrieve a folder by its name")
    public Folder findFolder(@QueryParam("name") String name) {
        return folderService.find(name);
    }

    @GET
    @Path("count")
    @PermitAll
    @Operation(description = "Get the upload count for all folders")
    public Map<String, Integer> getFolderUploadCount() {
        return folderService.getFolderUploadCount();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Authenticated
    @Operation(description = "Create a new folder")
    public Folder createFolder(@Valid @NotNull Folder folder) {
        if (folder.teamId() != null) {
            if (!userService.isMemberOf(folder.teamId())) {
                throw new ForbiddenException("You are not allowed to perform this action");
            }
            return folderService.create(folder.name(), folder.teamId());
        }
        return folderService.create(folder.name());
    }

    @DELETE
    @Path("{id}")
    @Authenticated
    @Operation(description = "Delete a folder by its ID")
    public void deleteFolder(@PathParam("id") long id) {
        folderService.delete(id);
    }

    @POST
    @Path("{id}/upload")
    @Consumes(MULTIPART_FORM_DATA)
    @Authenticated
    @Operation(description = "Upload JSON data to a folder. Returns immediately with an uploadId.")
    @APIResponse(responseCode = "200", description = "Upload successful, returns uploadId")
    @APIResponse(responseCode = "400", description = "Request received but content is not valid JSON or URL scheme is not http/https")
    public long upload(
            @PathParam("id") long id,
            @RestForm("raw") String raw,
            @RestForm("url") URL url,
            @RestForm("file") FileUpload file) {

        if (Stream.of(url, raw == null || raw.isBlank() ? null : raw, file).filter(Objects::nonNull).count() != 1) {
            throw new BadRequestException("Provide exactly one of 'file', 'raw', or 'url'");
        }

        byte[] bytes;

        try {
            if (url != null) {
                if (!Set.of("http", "https").contains(url.getProtocol())) {
                    throw new BadRequestException("Only http/https URLs are allowed");
                }
                URLConnection connection = url.openConnection();
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(30000);
                int readLimit = (int) Math.min(maxBodySize.asLongValue() + 1, Integer.MAX_VALUE);
                try (var inputStream = connection.getInputStream()) {
                    bytes = inputStream.readNBytes(readLimit);
                }
                if (bytes.length > maxBodySize.asLongValue()) {
                    throw new BadRequestException("Content at '" + url + "' exceeds the maximum upload size");
                }
            } else if (file != null) {
                bytes = readAllBytes(file.uploadedFile());
            } else {
                bytes = raw.getBytes(StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new BadRequestException("Failed to read upload data: " + e.getMessage());
        }

        try {
            return valueService.createRootValue(id, JqValues.parse(bytes));
        } catch (Exception e) {
            throw new BadRequestException("Invalid JSON: " + e.getMessage());
        }
    }

    @GET
    @Path("{id}/structure")
    @PermitAll
    @Operation(description = "Get the structural representation of a folder")
    public JqValue structure(@PathParam("id") long id) {
        return folderService.structure(id);
    }

    @GET
    @Path("{id}/labelValues")
    @PermitAll
    @Operation(description = "Get metrics labels Values")
    public List<JqValue>getLabelValues(
                    @PathParam("id") Long folderId,
                    @QueryParam("groupById") Long groupById,
                    @QueryParam("nodeIds") List<Long> nodeIds,
                    @QueryParam("sortById") Long sortById)
            {
                return valueService.getLabelValues(folderId, groupById,nodeIds,sortById);
            }
}
