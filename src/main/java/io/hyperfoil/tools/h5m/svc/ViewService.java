package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.api.View;
import io.hyperfoil.tools.h5m.api.ViewComponent;
import io.hyperfoil.tools.h5m.api.svc.ViewServiceInterface;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ViewComponentEntity;
import io.hyperfoil.tools.h5m.entity.ViewEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.NotFoundException;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class ViewService implements ViewServiceInterface {

    @Inject
    EntityManager em;

    @Inject
    ApiMapper apiMapper;

    @Inject
    ValueService valueService;

    @Override
    @Transactional
    public List<View> getViews(String folderName) {
        FolderEntity folder = findFolder(folderName);
        return folder.views.stream().map(apiMapper::toView).toList();
    }

    @Override
    @Transactional
    public View getView(Long viewId) {
        ViewEntity view = ViewEntity.findById(viewId);
        if (view == null) {
            throw new NotFoundException("View not found: " + viewId);
        }
        return apiMapper.toView(view);
    }

    @Override
    @Transactional
    public View createView(String folderName, View view) {
        FolderEntity folder = findFolder(folderName);

        ViewEntity entity = new ViewEntity(view.name(), folder);
        entity.components = new ArrayList<>();

        if (view.components() != null) {
            for (int i = 0; i < view.components().size(); i++) {
                ViewComponent vc = view.components().get(i);
                NodeEntity node = NodeEntity.findById(vc.nodeId());
                if (node == null) {
                    throw new NotFoundException("Node not found: " + vc.nodeId());
                }
                ViewComponentEntity component = new ViewComponentEntity(
                    entity, node,
                    vc.headerName() != null ? vc.headerName() : node.name,
                    vc.headerOrder() > 0 ? vc.headerOrder() : i
                );
                entity.components.add(component);
            }
        }

        entity.persist();
        folder.views.add(entity);
        return apiMapper.toView(entity);
    }

    @Override
    @Transactional
    public View updateView(Long viewId, View view) {
        ViewEntity entity = ViewEntity.findById(viewId);
        if (entity == null) {
            throw new NotFoundException("View not found: " + viewId);
        }

        entity.name = view.name();
        entity.components.clear();
        // Flush the deletes before inserting new components to avoid
        // unique constraint violations on (view_id, header_name)
        entity.flush();

        if (view.components() != null) {
            for (int i = 0; i < view.components().size(); i++) {
                ViewComponent vc = view.components().get(i);
                NodeEntity node = NodeEntity.findById(vc.nodeId());
                if (node == null) {
                    throw new NotFoundException("Node not found: " + vc.nodeId());
                }
                ViewComponentEntity component = new ViewComponentEntity(
                    entity, node,
                    vc.headerName() != null ? vc.headerName() : node.name,
                    vc.headerOrder() > 0 ? vc.headerOrder() : i
                );
                entity.components.add(component);
            }
        }

        entity.persist();
        return apiMapper.toView(entity);
    }

    @Override
    @Transactional
    public void deleteView(Long viewId) {
        ViewEntity entity = ViewEntity.findById(viewId);
        if (entity == null) {
            throw new NotFoundException("View not found: " + viewId);
        }
        if ("Default".equals(entity.name)) {
            throw new IllegalArgumentException("Cannot delete the Default view");
        }
        entity.delete();
    }

    @Override
    @Transactional
    public List<JsonNode> getViewData(String folderName, Long viewId) {
        ViewEntity view = em.createQuery(
            "SELECT v FROM folder_view v LEFT JOIN FETCH v.components c LEFT JOIN FETCH c.node WHERE v.id = :id",
            ViewEntity.class
        ).setParameter("id", viewId).getSingleResult();
        if (view == null) {
            throw new NotFoundException("View not found: " + viewId);
        }

        FolderEntity folder = findFolder(folderName);
        Long rootNodeId = folder.group.root.id;

        List<Long> nodeIds = view.components.stream()
            .map(c -> c.node.id)
            .toList();

        if (nodeIds.isEmpty()) {
            return List.of();
        }

        return valueService.getGroupedValues(rootNodeId, nodeIds);
    }

    private FolderEntity findFolder(String folderName) {
        FolderEntity folder = em.createQuery(
            "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.root WHERE f.name = :name",
            FolderEntity.class
        ).setParameter("name", folderName).getSingleResult();
        if (folder == null) {
            throw new NotFoundException("Folder not found: " + folderName);
        }
        // Initialize views and their components (avoids MultipleBagFetchException)
        folder.views.size();
        folder.views.forEach(v -> v.components.size());
        return folder;
    }
}
