# Views Feature Design

## Overview

A **View** defines which node values to display as columns when browsing a folder's uploads.
Each folder has at least one view (the auto-created "Default"). Users can create additional
views to focus on different subsets of nodes.

This is the h5m equivalent of Horreum's Views feature, adapted for h5m's node-based model.

## Horreum Context

In Horreum, a View is a user-configured presentation layer for dataset tables:
- ViewComponents reference **labels by name** and define column headers + ordering
- Optional JavaScript render functions format cell content (evaluated client-side)
- A materialized `dataset_view` table pre-computes the pivot for performance
- Each Test auto-creates a "Default" view that cannot be deleted
- Views are managed via the UIService REST API

Key differences in h5m:
- h5m nodes are the equivalent of Horreum labels (direct 1:1 mapping)
- ViewComponents reference **nodes by FK** (not by name) since nodes are stable entities
- No materialization initially (can add later if performance requires it)
- The existing `getGroupedValues()` query provides the data pivot primitive
- No `render` field (see rationale below)

## No Render Functions

Horreum's ViewComponent has an optional `render` field — a JavaScript function like
`value => value + " req/s"` that formats cell content, evaluated client-side via
`new Function()`.

h5m does **not** include a render field. The reasons:

- **Formatting belongs in the UI layer.** Units, number formatting, link rendering, and
  styling are presentation concerns best handled by frontend components, not JS strings
  stored in the database.
- **Multi-value combination is a node concern.** Horreum needs render functions partly
  because ViewComponents can reference multiple labels and need JS to combine them. In h5m,
  you create a JQ/JS node that combines values as part of the processing pipeline, then
  add that single node to the view.
- **Security.** Evaluating user-provided JavaScript (even client-side) introduces XSS risk
  and requires careful sandboxing.
- **Complexity.** Error handling, evaluation context, and the frontend `new Function()`
  machinery add significant complexity for limited benefit.

If a need for cell formatting arises later, it can be added as a simple string field
without breaking the existing model.

## Data Model

### ViewEntity

```java
@Entity
@Table(name = "folder_view",
    uniqueConstraints = @UniqueConstraint(columnNames = {"folder_id", "name"}))
public class ViewEntity extends PanacheEntity {
    @NotNull String name;
    @ManyToOne(fetch = LAZY) @JoinColumn(name = "folder_id") FolderEntity folder;
    @OneToMany(mappedBy = "view", cascade = ALL, orphanRemoval = true)
    @OrderBy("headerOrder ASC") List<ViewComponentEntity> components;
}
```

### ViewComponentEntity

```java
@Entity
@Table(name = "folder_view_component",
    uniqueConstraints = @UniqueConstraint(columnNames = {"view_id", "header_name"}))
public class ViewComponentEntity extends PanacheEntity {
    @ManyToOne(fetch = LAZY) @JoinColumn(name = "view_id") ViewEntity view;
    @ManyToOne(fetch = LAZY) @JoinColumn(name = "node_id") NodeEntity node;
    @NotNull String headerName;   // display name (defaults to node.name)
    int headerOrder;              // column position (lower = left)
}
```

### FolderEntity relationship

```java
@OneToMany(mappedBy = "folder", cascade = ALL, orphanRemoval = true)
List<ViewEntity> views;
```

### API records

```java
record View(Long id, String name, Long folderId, List<ViewComponent> components)

record ViewComponent(Long id, Long nodeId, String nodeName, String nodeType,
                     String headerName, int headerOrder)
```

The view data endpoint returns `List<JsonNode>` directly (same format as the existing
`getGroupedValues()` query). Column metadata is available from the `View` record's
components list — no need for a separate response wrapper.

## Default View Auto-Creation

When a folder is created, auto-create an empty "Default" view. Users configure which
nodes appear as columns via the REST API or the Configure View Modal (Phase 5b).
This matches Horreum's approach where the Default view starts empty.

The "Default" view cannot be deleted (same protection as Horreum).

## Filtered Grouped Values Query

The view data endpoint returns pivoted values filtered to the view's component nodes.
This is a modified version of `ValueService.getGroupedValues()` with a single
`WHERE node_id IN (:nodeIds)` filter in the `bynode` CTE:

```sql
WITH RECURSIVE tree(id, node_id, root_id, idx, data) AS (
    -- walk the full value DAG from root values
    SELECT v.id, v.node_id, ve.parent_id AS root_id, v.idx, v.data
    FROM value_edge ve
    LEFT JOIN value v ON ve.child_id = v.id
    WHERE ve.parent_id IN (SELECT id FROM value WHERE node_id = :rootNodeId)
    UNION
    SELECT v.id, v.node_id, t.root_id, v.idx, v.data
    FROM value v
    JOIN value_edge ve ON v.id = ve.child_id
    JOIN tree t ON ve.parent_id = t.id
), bynode AS (
    SELECT node_id, root_id, jsonb_agg(to_jsonb(data)) AS data
    FROM tree
    WHERE node_id IN (:nodeIds)     -- VIEW FILTER: only include view's nodes
    GROUP BY node_id, root_id, idx
    ORDER BY idx
)
SELECT jsonb_object_agg(n.name, to_jsonb(
    CASE WHEN jsonb_array_length(b.data) > 1 THEN b.data ELSE b.data->0 END
)) AS data
FROM bynode b
JOIN node n ON b.node_id = n.id
GROUP BY root_id
```

The recursive CTE still walks the full value DAG (needed for correctness), but
the aggregation only includes the requested nodes, reducing the response size.

## REST Endpoints

New `ViewResource` at `/api/folder/{name}/view`:

| HTTP   | Path                                    | Auth           | Description                    |
|--------|-----------------------------------------|----------------|--------------------------------|
| GET    | `/api/folder/{name}/views`              | @PermitAll     | List views for folder          |
| GET    | `/api/folder/{name}/view/{viewId}`      | @PermitAll     | Get view definition            |
| POST   | `/api/folder/{name}/view`               | @Authenticated | Create view                    |
| PUT    | `/api/folder/{name}/view/{viewId}`      | @Authenticated | Update view                    |
| DELETE | `/api/folder/{name}/view/{viewId}`      | @Authenticated | Delete view (protect "Default")|
| GET    | `/api/folder/{name}/view/{viewId}/data` | @PermitAll     | Get filtered pivoted data      |

## Web UI

Add a **"Data"** tab to FolderPage:
1. Fetch views for the folder
2. Show a view selector dropdown (defaulting to "Default")
3. Fetch view data for the selected view
4. Render a Carbon DataTable with columns from view components and rows from pivoted data
5. Provide a "Configure View" modal for editing component selection, ordering, and headers

## Implementation Order

### Phase 1: Data Model
- Create `ViewEntity` and `ViewComponentEntity`
- Add `views` relationship to `FolderEntity`
- Create API records: `View`, `ViewComponent`, `ViewData`
- Add mapper methods to `ApiMapper`
- Update `FreshDb` to truncate new tables

### Phase 2: Service & REST
- Create `ViewService` with CRUD + filtered query
- Create `ViewServiceInterface`
- Create `ViewResource` REST endpoints
- Add `getViewData()` to `ValueService` (filtered grouped values query)

### Phase 3: Default View
- Auto-create "Default" view in `FolderService.create()`
- Auto-create "Default" view in `FolderService.importFolder()`
- Protect "Default" from deletion

### Phase 4: Tests
- Default view auto-created on folder creation (empty, users configure via REST/UI)
- CRUD operations on views (create, read, update, delete; "Default" protected)
- View data endpoint returns correct filtered columns
- View data with multiple uploads (one row per upload)
- View component ordering (columns in headerOrder order)
- Import/export preserves views (views included in folder export JSON)

### Phase 5: Web UI (separate PR)

#### TypeScript Client Regeneration
The TypeScript client is regenerated automatically during `mvn package` / `mvn install`:
1. Quarkus augmentation generates `openapi.yaml` into `src/main/webui/`
   (configured via `quarkus.smallrye-openapi.store-schema-directory`)
2. Quinoa runs `npm run build` which calls `openapi-ts` before `tsc` and `vite build`

For local development, run `npm run openapi` in `src/main/webui/` to regenerate
the client types for IDE autocompletion without doing a full Maven build.

#### 5a. Data Tab
- New `DataTab` component (`src/main/webui/src/app/components/DataTab.tsx`)
  - View selector dropdown (Carbon `Dropdown`), defaults to "Default" view
  - Carbon `DataTable` with columns from view components (headerName, headerOrder)
  - Rows from `GET /api/folder/{name}/view/{viewId}/data` (one per upload)
  - Cell values keyed by node name from each row's JSON object
  - Empty state when no uploads, loading skeleton during fetch
- Add "Data" as first tab in FolderPage (`TAB_ANCHORS = ['data', 'nodes', 'graph']`)
- Tests for DataTab: renders selector, renders table, switching views refetches

#### 5b. Configure View Modal (follow-up PR)
- New `ViewConfigModal` component (`src/main/webui/src/app/components/ViewConfigModal.tsx`)
  - View name text input
  - Multi-select node picker from folder's group nodes
  - Header name override per selected node
  - Drag-to-reorder for column ordering
  - Save (create/update), Delete (with confirmation, disabled for Default), Cancel
- "Configure" button next to the view dropdown in DataTab

## File Changes

| File | Change |
|------|--------|
| `entity/ViewEntity.java` | **New** |
| `entity/ViewComponentEntity.java` | **New** |
| `entity/FolderEntity.java` | Add `views` relationship |
| `api/View.java` | **New** API record |
| `api/ViewComponent.java` | **New** API record |
| `api/svc/ViewServiceInterface.java` | **New** interface |
| `svc/ViewService.java` | **New** service |
| `rest/ViewResource.java` | **New** REST resource |
| `entity/mapper/ApiMapper.java` | Add `toView`, `toViewComponent` |
| `svc/FolderService.java` | Auto-create default view in `create()` |
| `svc/ValueService.java` | Add `getViewData()` filtered query |
| `FreshDb.java` | Add truncation for new tables |
| `svc/ViewServiceTest.java` | **New** tests |
| `rest/ViewResourceTest.java` | **New** REST tests |

## Future Considerations

- **Materialization**: If `getViewData()` becomes slow with many uploads, add a
  `folder_view_data` table (like Horreum's `dataset_view`) that pre-computes the
  pivot on upload. Invalidate on view definition changes.
- **CLI integration**: The `list value from <group> by <node>` command could accept
  a `--view` flag to use a named view instead of showing all nodes.
- **Render functions**: If cell formatting needs arise that can't be handled in the
  frontend component layer, a `render` string field can be added to ViewComponentEntity
  without breaking the existing model.
