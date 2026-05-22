package io.hyperfoil.tools.h5m.svc;

import jakarta.persistence.EntityManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared edge table queries for edge tables (node_edge, value_edge).
 */
class EdgeQueries {

    static long getParentCount(EntityManager em, String edgeTable, Long childId) {
        return ((Number) em.createNativeQuery(
                "SELECT COUNT(*) FROM " + edgeTable + " WHERE child_id = :childId and depth = 1"
        ).setParameter("childId", childId).getSingleResult()).longValue();
    }

    @SuppressWarnings("unchecked")
    static Map<Long, Long> getParentCounts(EntityManager em, String edgeTable, List<Long> childIds) {
        if (childIds.isEmpty()) return Map.of();
        List<Object[]> rows = em.createNativeQuery(
                "SELECT child_id, COUNT(*) FROM " + edgeTable + " WHERE child_id IN (:childIds) and depth = 1 GROUP BY child_id"
        ).setParameter("childIds", childIds).getResultList();
        Map<Long, Long> result = new HashMap<>();
        for (Object[] row : rows) {
            result.put(((Number) row[0]).longValue(), ((Number) row[1]).longValue());
        }
        return result;
    }

    /**
     * delete ALL the edges (regardless of count) where parentId is the parent
     * @param em
     * @param edgeTable
     * @param parentId
     */
    static void deleteParentEdges(EntityManager em, String edgeTable, Long parentId) {
        em.createNativeQuery(
                """
                delete from EDGE_TABLE where parent_id = :parent_id
                """.replaceAll("EDGE_TABLE",edgeTable)
                )
                .setParameter("parent_id", parentId)
                .executeUpdate();
    }

    /**
     * delete ALL the edges (regardless of count) where childId is the child
     * @param em
     * @param edgeTable
     * @param childId
     */
    static void deleteChildEdges(EntityManager em, String edgeTable, Long childId) {
        em.createNativeQuery(
                """
                delete from EDGE_TABLE where child_id = :child_id
                """.replaceAll("EDGE_TABLE",edgeTable)
                )
                .setParameter("child_id", childId)
                .executeUpdate();
    }
}
