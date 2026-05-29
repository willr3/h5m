package io.hyperfoil.tools.h5m.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.WorkService;
import io.restassured.specification.RequestSpecification;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class RestEndpointTest extends FreshDb {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Inject
    TransactionManager tm;

    @Inject
    WorkService workService;

    @Inject
    FolderService folderService;

    private void createFolder(String name) {
        given()
                .contentType(MediaType.APPLICATION_JSON)
                .when().post("/api/folder/" + name)
                .then()
                .statusCode(200);
    }

    private Long getGroupId(String name) {
        return given()
                .when().get("/api/group/" + name)
                .then()
                .extract().jsonPath().getLong("id");
    }

    private Long createNode(Long groupId, String name, String operation) {
        return createNodeWithType(groupId, name, NodeType.JQ.name(), operation);
    }

    private Long createConfiguredNode(Long groupId, String name, String type, List<Long> sources, String configBody) {
        RequestSpecification request = given()
                .contentType(MediaType.APPLICATION_JSON)
                .queryParam("name", name)
                .queryParam("groupId", groupId)
                .queryParam("type", type);
        for (Long sourceId : sources) {
            request = request.queryParam("sources", sourceId);
        }
        if (configBody != null) {
            request = request.body(configBody);
        }
        return request
                .when().post("/api/node/configured")
                .then()
                .statusCode(200)
                .extract().as(Long.class);
    }

    private Long createNodeWithType(Long groupId, String name, String type, String operation) {
        return given()
                .contentType(MediaType.APPLICATION_JSON)
                .queryParam("name", name)
                .queryParam("groupId", groupId)
                .queryParam("type", type)
                .queryParam("operation", operation)
                .when().post("/api/node")
                .then()
                .statusCode(200)
                .extract().as(Long.class);
    }

    // -- Folder endpoints --

    @Test
    public void folder_create_and_get() {
        createFolder("test-folder");

        given()
                .when().get("/api/folder/test-folder")
                .then()
                .statusCode(200)
                .body("name", equalTo("test-folder"))
                .body("id", notNullValue());
    }

    @Test
    public void folder_list_empty() {
        given()
                .when().get("/api/folder")
                .then()
                .statusCode(200)
                .body("size()", equalTo(0));
    }

    @Test
    public void folder_list_with_folder() {
        createFolder("list-test");

        given()
                .when().get("/api/folder")
                .then()
                .statusCode(200)
                .body("size()", equalTo(1))
                .body("name", hasItem("list-test"));
    }

    @Test
    public void folder_get_upload_count_empty() {
        given()
                .when().get("/api/folder/count")
                .then()
                .statusCode(200)
                .body("size()", equalTo(0));
    }

    @Test
    public void folder_get_upload_count_with_folder() {
        createFolder("count-test");

        given()
                .when().get("/api/folder/count")
                .then()
                .statusCode(200)
                .body("'count-test'", equalTo(0));
    }

    @Test
    public void folder_upload_and_structure() {
        createFolder("upload-test");

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .queryParam("path", "results.json")
                .body("{\"key\": \"value\"}")
                .when().post("/api/folder/upload-test/upload")
                .then()
                .statusCode(204);

        given()
                .when().get("/api/folder/upload-test/structure")
                .then()
                .statusCode(200)
                .body(notNullValue());
    }

    @Test
    public void folder_delete() {
        createFolder("delete-me");

        given()
                .when().delete("/api/folder/delete-me")
                .then()
                .statusCode(200);

        // null return becomes 204 No Content
        given()
                .when().get("/api/folder/delete-me")
                .then()
                .statusCode(204);
    }

    @Test
    public void folder_recalculate() {
        createFolder("recalc-test");

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .when().post("/api/folder/recalc-test/recalculate")
                .then()
                .statusCode(204);
    }

    // -- Node endpoints --

    @Test
    public void node_create_and_find() {
        createFolder("node-test");
        Long groupId = getGroupId("node-test");
        createNode(groupId, "jq-node", ".foo");

        // use asString() since Node->NodeGroup->List<Node> has circular refs (see #47)
        String response = given()
                .queryParam("name", "jq-node")
                .queryParam("groupId", groupId)
                .when().get("/api/node/find")
                .then()
                .statusCode(200)
                .extract().asString();

        assertTrue(response.contains("jq-node"), "Response should contain the node name");
    }

    @Test
    public void node_delete() {
        createFolder("node-del-test");
        Long groupId = getGroupId("node-del-test");
        Long nodeId = createNode(groupId, "to-delete", ".bar");

        given()
                .when().delete("/api/node/" + nodeId)
                .then()
                .statusCode(204);

        given()
                .queryParam("name", "to-delete")
                .queryParam("groupId", groupId)
                .when().get("/api/node/find")
                .then()
                .statusCode(200)
                .body("size()", equalTo(0));
    }

    // -- NodeGroup endpoints --

    @Test
    public void group_get_by_name() {
        createFolder("group-test");

        given()
                .when().get("/api/group/group-test")
                .then()
                .statusCode(200)
                .body("name", equalTo("group-test"))
                .body("id", notNullValue());
    }

    @Test
    public void group_get_nonexistent() {
        given()
                .when().get("/api/group/nonexistent")
                .then()
                .statusCode(204);
    }

    // -- Value endpoints --

    @Test
    public void value_get_descendants_empty() throws Exception {
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        tm.commit();

        given()
                .when().get("/api/value/node/" + rootNode.id + "/descendants")
                .then()
                .statusCode(200)
                .body("size()", equalTo(0));
    }

    @Test
    public void value_get_descendants_with_data() throws Exception {
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        JqNode jqNode = new JqNode("child", ".foo");
        jqNode.sources = List.of(rootNode);
        jqNode.persist();
        ValueEntity rootValue = new ValueEntity(null, rootNode, mapper.readTree("{\"foo\": \"bar\"}"));
        rootValue.persist();
        ValueEntity childValue = new ValueEntity(null, jqNode, mapper.readTree("\"bar\""));
        childValue.sources = List.of(rootValue);
        childValue.persist();
        tm.commit();

        given()
                .when().get("/api/value/node/" + rootNode.id + "/descendants")
                .then()
                .statusCode(200)
                .body("size()", greaterThanOrEqualTo(1));
    }

    @Test
    public void value_purge() throws Exception {
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        ValueEntity rootValue = new ValueEntity(null, rootNode, mapper.readTree("{\"a\": 1}"));
        rootValue.persist();
        tm.commit();

        given()
                .when().delete("/api/value")
                .then()
                .statusCode(204);

        given()
                .when().get("/api/value/node/" + rootNode.id + "/descendants")
                .then()
                .statusCode(200)
                .body("size()", equalTo(0));
    }

    // -- Node createConfigured endpoint --

    @Test
    public void node_create_configured_fixed_threshold() {
        createFolder("ft-test");
        Long groupId = getGroupId("ft-test");
        Long rangeNodeId = createNode(groupId, "range", ".y");
        Long fpNodeId = createNode(groupId, "fingerprint", ".fp");

        Long nodeId = createConfiguredNode(groupId, "threshold", NodeType.FIXED_THRESHOLD.name(),
                List.of(fpNodeId, rangeNodeId),
                """
                {"min": 10.0, "max": 100.0, "minInclusive": true, "maxInclusive": true, "fingerprintFilter": null}
                """);

        assertTrue(nodeId > 0, "should return a valid node ID");
    }

    @Test
    public void node_create_configured_relative_difference() {
        createFolder("rd-test");
        Long groupId = getGroupId("rd-test");
        Long rangeNodeId = createNode(groupId, "range", ".y");
        Long fpNodeId = createNode(groupId, "fingerprint", ".fp");

        Long nodeId = createConfiguredNode(groupId, "reldiff", NodeType.RELATIVE_DIFFERENCE.name(),
                List.of(fpNodeId, rangeNodeId),
                """
                {"filter": "max", "threshold": 0.2, "window": 5, "minPrevious": 3, "fingerprintFilter": null}
                """);

        assertTrue(nodeId > 0, "should return a valid node ID");
    }

    @Test
    public void node_create_configured_fingerprint() {
        createFolder("fp-test");
        Long groupId = getGroupId("fp-test");
        Long jqNodeId = createNode(groupId, "source", ".foo");

        Long nodeId = createConfiguredNode(groupId, "fp-node", NodeType.FINGERPRINT.name(),
                List.of(jqNodeId), null);

        assertTrue(nodeId > 0, "should return a valid node ID");
    }

    // -- Node create with other types --

    @Test
    public void node_create_jsonata() {
        createFolder("jsonata-test");
        Long groupId = getGroupId("jsonata-test");

        Long nodeId = createNodeWithType(groupId, "jsonata-node", NodeType.JSONATA.name(), "$sum(foo)");

        assertTrue(nodeId > 0, "should return a valid node ID");
    }

    @Test
    public void node_create_sql_jsonpath() {
        createFolder("sql-test");
        Long groupId = getGroupId("sql-test");

        Long nodeId = createNodeWithType(groupId, "sql-node", NodeType.SQL_JSONPATH_NODE.name(), "$.foo.bar");

        assertTrue(nodeId > 0, "should return a valid node ID");
    }

    @Test
    public void node_create_sql_jsonpath_all() {
        createFolder("sql-all-test");
        Long groupId = getGroupId("sql-all-test");

        Long nodeId = createNodeWithType(groupId, "sql-all-node", NodeType.SQL_JSONPATH_ALL_NODE.name(), "$.foo[*]");

        assertTrue(nodeId > 0, "should return a valid node ID");
    }

    // -- End-to-end: upload + computed values --

    @Test
    public void upload_and_verify_jq_values_via_rest() throws InterruptedException {
        createFolder("e2e-test");
        Long groupId = getGroupId("e2e-test");
        Long jqNodeId = createNode(groupId, "extract", ".key");

        // Upload data via REST
        given()
                .contentType(MediaType.APPLICATION_JSON)
                .queryParam("path", "$")
                .body("{\"key\": \"hello\"}")
                .when().post("/api/folder/e2e-test/upload")
                .then()
                .statusCode(204);

        // Wait for the work queue to finish processing the upload
        long deadline = System.currentTimeMillis() + 10_000;
        while (!workService.isIdle() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(workService.isIdle(), "Work queue should be idle after processing");

        // Verify the JQ node computed a value from the uploaded data via REST
        given()
                .when().get("/api/value/node/" + jqNodeId)
                .then()
                .statusCode(200)
                .body("size()", greaterThan(0))
                .body("[0].data", equalTo("hello"));
    }

    // -- Dashboard summaries --

    @Test
    public void dashboard_returns_empty_list_when_no_folders() {
        given()
                .when().get("/api/folder/dashboard")
                .then()
                .statusCode(200)
                .body("size()", equalTo(0));
    }

    @Test
    public void dashboard_returns_folder_summary_with_rhivos_upload() throws Exception {
        // Import rhivos node graph and upload a run
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        try (InputStream is = getClass().getResourceAsStream("/rhivos/40375.json")) {
            JsonNode runData = mapper.readTree(is);
            folderService.upload("rhivos-perf-comprehensive", "$", runData);
        }

        long deadline = System.currentTimeMillis() + 30_000;
        while (!workService.isIdle() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        given()
                .when().get("/api/folder/dashboard")
                .then()
                .statusCode(200)
                .body("size()", equalTo(1))
                .body("[0].name", equalTo("rhivos-perf-comprehensive"))
                .body("[0].uploadCount", equalTo(1))
                .body("[0].nodeCount", greaterThan(100))
                .body("[0].lastUpload", notNullValue());
    }

    @Test
    public void dashboard_counts_multiple_uploads() throws Exception {
        // Import rhivos node graph and upload two runs
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        for (String runFile : List.of("/rhivos/40375.json", "/rhivos/40376.json")) {
            try (InputStream is = getClass().getResourceAsStream(runFile)) {
                JsonNode runData = mapper.readTree(is);
                folderService.upload("rhivos-perf-comprehensive", "$", runData);
            }

            long deadline = System.currentTimeMillis() + 30_000;
            while (!workService.isIdle() && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
        }

        given()
                .when().get("/api/folder/dashboard")
                .then()
                .statusCode(200)
                .body("[0].uploadCount", equalTo(2));
    }

    // -- Value data endpoint --

    @Test
    public void value_data_returns_json_for_uploaded_rhivos_run() throws Exception {
        // Import rhivos node graph and upload a run
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        try (InputStream is = getClass().getResourceAsStream("/rhivos/40375.json")) {
            JsonNode runData = mapper.readTree(is);
            folderService.upload("rhivos-perf-comprehensive", "$", runData);
        }

        long deadline = System.currentTimeMillis() + 30_000;
        while (!workService.isIdle() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        // Find the root value ID — the upload itself
        tm.begin();
        FolderEntity folder = FolderEntity.find("name", "rhivos-perf-comprehensive").firstResult();
        List<ValueEntity> rootValues = ValueEntity.find("node.id", folder.group.root.id).list();
        Long valueId = rootValues.get(0).id;
        tm.commit();

        // Verify the endpoint returns the uploaded JSON data
        given()
                .when().get("/api/value/" + valueId)
                .then()
                .statusCode(200)
                // rhivos runs have metadata with user and uuid fields
                .body("metadata.user", notNullValue());
    }

    @Test
    public void value_data_returns_404_for_nonexistent_value() {
        given()
                .when().get("/api/value/999999")
                .then()
                .statusCode(404);
    }

    // -- Views --

    @Test
    public void view_default_created_for_new_folder() {
        createFolder("view-default");

        given()
                .when().get("/api/folder/view-default/view/")
                .then()
                .statusCode(200)
                .body("size()", equalTo(1))
                .body("[0].name", equalTo("Default"))
                .body("[0].components.size()", equalTo(0));
    }

    @Test
    public void view_default_created_on_import_empty() throws Exception {
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        // Default view should exist but be empty (users configure it)
        given()
                .when().get("/api/folder/rhivos-perf-comprehensive/view/")
                .then()
                .statusCode(200)
                .body("size()", equalTo(1))
                .body("[0].name", equalTo("Default"))
                .body("[0].components.size()", equalTo(0));
    }

    @Test
    public void view_create_and_retrieve() throws Exception {
        // Import rhivos nodes so we have real node IDs to reference
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        // Find node IDs for "user" and "uuid" by querying the group
        tm.begin();
        FolderEntity folder = FolderEntity.find("name", "rhivos-perf-comprehensive").firstResult();
        Long userNodeId = folder.group.sources.stream()
                .filter(n -> "user".equals(n.name)).findFirst().get().id;
        Long uuidNodeId = folder.group.sources.stream()
                .filter(n -> "uuid".equals(n.name)).findFirst().get().id;
        tm.commit();

        // Create a view via REST
        String viewJson = mapper.writeValueAsString(new io.hyperfoil.tools.h5m.api.View(
                null, "test-view", null,
                List.of(
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, userNodeId, null, null, "User", 0),
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, uuidNodeId, null, null, "UUID", 1)
                )
        ));

        Long viewId = given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(viewJson)
                .when().post("/api/folder/rhivos-perf-comprehensive/view")
                .then()
                .statusCode(200)
                .body("name", equalTo("test-view"))
                .body("components.size()", equalTo(2))
                .body("components[0].headerName", equalTo("User"))
                .body("components[1].headerName", equalTo("UUID"))
                .extract().jsonPath().getLong("id");

        // Retrieve it
        given()
                .when().get("/api/folder/rhivos-perf-comprehensive/view/" + viewId)
                .then()
                .statusCode(200)
                .body("name", equalTo("test-view"))
                .body("components.size()", equalTo(2));

        // List should contain Default + test-view
        given()
                .when().get("/api/folder/rhivos-perf-comprehensive/view/")
                .then()
                .statusCode(200)
                .body("size()", equalTo(2));
    }

    @Test
    public void view_update_changes_components() throws Exception {
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        tm.begin();
        FolderEntity folder = FolderEntity.find("name", "rhivos-perf-comprehensive").firstResult();
        Long userNodeId = folder.group.sources.stream()
                .filter(n -> "user".equals(n.name)).findFirst().get().id;
        Long uuidNodeId = folder.group.sources.stream()
                .filter(n -> "uuid".equals(n.name)).findFirst().get().id;
        Long descNodeId = folder.group.sources.stream()
                .filter(n -> "description".equals(n.name)).findFirst().get().id;
        tm.commit();

        // Create view with user only
        String createJson = mapper.writeValueAsString(new io.hyperfoil.tools.h5m.api.View(
                null, "updatable", null,
                List.of(new io.hyperfoil.tools.h5m.api.ViewComponent(null, userNodeId, null, null, "User", 0))
        ));

        Long viewId = given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(createJson)
                .when().post("/api/folder/rhivos-perf-comprehensive/view")
                .then()
                .statusCode(200)
                .extract().jsonPath().getLong("id");

        // Update to uuid + description
        String updateJson = mapper.writeValueAsString(new io.hyperfoil.tools.h5m.api.View(
                null, "updatable-renamed", null,
                List.of(
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, uuidNodeId, null, null, "UUID", 0),
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, descNodeId, null, null, "Description", 1)
                )
        ));

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(updateJson)
                .when().put("/api/folder/rhivos-perf-comprehensive/view/" + viewId)
                .then()
                .statusCode(200)
                .body("name", equalTo("updatable-renamed"))
                .body("components.size()", equalTo(2))
                .body("components[0].headerName", equalTo("UUID"));
    }

    @Test
    public void view_delete_works() throws Exception {
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        tm.begin();
        FolderEntity folder = FolderEntity.find("name", "rhivos-perf-comprehensive").firstResult();
        Long userNodeId = folder.group.sources.stream()
                .filter(n -> "user".equals(n.name)).findFirst().get().id;
        tm.commit();

        String createJson = mapper.writeValueAsString(new io.hyperfoil.tools.h5m.api.View(
                null, "deletable", null,
                List.of(new io.hyperfoil.tools.h5m.api.ViewComponent(null, userNodeId, null, null, "User", 0))
        ));

        Long viewId = given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(createJson)
                .when().post("/api/folder/rhivos-perf-comprehensive/view")
                .then()
                .statusCode(200)
                .extract().jsonPath().getLong("id");

        // Delete it
        given()
                .when().delete("/api/folder/rhivos-perf-comprehensive/view/" + viewId)
                .then()
                .statusCode(204);

        // Custom view should be gone, only Default remains
        given()
                .when().get("/api/folder/rhivos-perf-comprehensive/view/")
                .then()
                .statusCode(200)
                .body("size()", equalTo(1))
                .body("[0].name", equalTo("Default"));
    }

    @Test
    public void view_delete_default_rejected() throws Exception {
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        // Find the auto-created Default view's ID
        Long viewId = given()
                .when().get("/api/folder/rhivos-perf-comprehensive/view/")
                .then()
                .statusCode(200)
                .body("[0].name", equalTo("Default"))
                .extract().jsonPath().getLong("[0].id");

        // Attempting to delete "Default" should fail
        given()
                .when().delete("/api/folder/rhivos-perf-comprehensive/view/" + viewId)
                .then()
                .statusCode(anyOf(equalTo(400), equalTo(500)));
    }

    @Test
    public void view_data_returns_filtered_rhivos_values() throws Exception {
        // Import rhivos nodes and upload a run
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        try (InputStream is = getClass().getResourceAsStream("/rhivos/40375.json")) {
            JsonNode runData = mapper.readTree(is);
            folderService.upload("rhivos-perf-comprehensive", "$", runData);
        }

        long deadline = System.currentTimeMillis() + 30_000;
        while (!workService.isIdle() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertTrue(workService.isIdle(), "Work queue should be idle");

        // Find node IDs for nodes that produce values with rhivos data
        tm.begin();
        FolderEntity folder = FolderEntity.find("name", "rhivos-perf-comprehensive").firstResult();
        Long startTimeNodeId = folder.group.sources.stream()
                .filter(n -> "start_time".equals(n.name)).findFirst().get().id;
        Long endTimeNodeId = folder.group.sources.stream()
                .filter(n -> "end_time".equals(n.name)).findFirst().get().id;
        tm.commit();

        // Create a view with start_time and end_time
        String viewJson = mapper.writeValueAsString(new io.hyperfoil.tools.h5m.api.View(
                null, "data-view", null,
                List.of(
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, startTimeNodeId, null, null, "Start Time", 0),
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, endTimeNodeId, null, null, "End Time", 1)
                )
        ));

        Long viewId = given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(viewJson)
                .when().post("/api/folder/rhivos-perf-comprehensive/view")
                .then()
                .statusCode(200)
                .extract().jsonPath().getLong("id");

        // Get view data — should return filtered results with only start_time and end_time columns
        given()
                .when().get("/api/folder/rhivos-perf-comprehensive/view/" + viewId + "/data")
                .then()
                .statusCode(200)
                .body("size()", greaterThan(0))
                // Each row should have the selected node columns
                .body("[0].start_time", notNullValue())
                .body("[0].end_time", notNullValue());
    }

    @Test
    public void view_data_with_multiple_uploads() throws Exception {
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        // Upload two runs
        for (String runFile : List.of("/rhivos/40375.json", "/rhivos/40376.json")) {
            try (InputStream is = getClass().getResourceAsStream(runFile)) {
                JsonNode runData = mapper.readTree(is);
                folderService.upload("rhivos-perf-comprehensive", "$", runData);
            }
            long deadline = System.currentTimeMillis() + 30_000;
            while (!workService.isIdle() && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
        }

        // Create a view with start_time
        tm.begin();
        FolderEntity folder = FolderEntity.find("name", "rhivos-perf-comprehensive").firstResult();
        Long startTimeNodeId = folder.group.sources.stream()
                .filter(n -> "start_time".equals(n.name)).findFirst().get().id;
        tm.commit();

        String viewJson = mapper.writeValueAsString(new io.hyperfoil.tools.h5m.api.View(
                null, "multi-upload-view", null,
                List.of(new io.hyperfoil.tools.h5m.api.ViewComponent(null, startTimeNodeId, null, null, "Start Time", 0))
        ));

        Long viewId = given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(viewJson)
                .when().post("/api/folder/rhivos-perf-comprehensive/view")
                .then()
                .statusCode(200)
                .extract().jsonPath().getLong("id");

        // View data should have one row per upload
        given()
                .when().get("/api/folder/rhivos-perf-comprehensive/view/" + viewId + "/data")
                .then()
                .statusCode(200)
                .body("size()", equalTo(2))
                .body("[0].start_time", notNullValue())
                .body("[1].start_time", notNullValue());
    }

    @Test
    public void view_component_ordering() throws Exception {
        folderService.importFolder(Path.of("src/test/resources/rhivos/nodes.json"), false);

        tm.begin();
        FolderEntity folder = FolderEntity.find("name", "rhivos-perf-comprehensive").firstResult();
        Long startTimeNodeId = folder.group.sources.stream()
                .filter(n -> "start_time".equals(n.name)).findFirst().get().id;
        Long endTimeNodeId = folder.group.sources.stream()
                .filter(n -> "end_time".equals(n.name)).findFirst().get().id;
        tm.commit();

        // Create view with end_time at order 0 and start_time at order 1 (reversed)
        String viewJson = mapper.writeValueAsString(new io.hyperfoil.tools.h5m.api.View(
                null, "ordered-view", null,
                List.of(
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, endTimeNodeId, null, null, "End", 0),
                        new io.hyperfoil.tools.h5m.api.ViewComponent(null, startTimeNodeId, null, null, "Start", 1)
                )
        ));

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(viewJson)
                .when().post("/api/folder/rhivos-perf-comprehensive/view")
                .then()
                .statusCode(200)
                // Components should be ordered by headerOrder
                .body("components[0].headerName", equalTo("End"))
                .body("components[0].headerOrder", equalTo(0))
                .body("components[1].headerName", equalTo("Start"))
                .body("components[1].headerOrder", equalTo(1));
    }

    // -- OpenAPI spec --

    @Test
    public void openapi_spec_available() {
        given()
                .when().get("/q/openapi")
                .then()
                .statusCode(200)
                .body(containsString("/api/folder"))
                .body(containsString("/api/node"))
                .body(containsString("/api/group"))
                .body(containsString("/api/value"));
    }

}
