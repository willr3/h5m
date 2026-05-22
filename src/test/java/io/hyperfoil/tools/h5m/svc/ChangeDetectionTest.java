package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.FixedThreshold;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.h5m.event.ChangeDetectedEvent;
import io.hyperfoil.tools.h5m.event.ChangeDetectedEventObserver;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ChangeDetectionTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    WorkService workService;

    @Inject
    ChangeDetectedEventObserver eventObserver;

    @BeforeEach
    public void clearEvents() {
        eventObserver.clear();
    }

    private record ThresholdFixture(FixedThreshold ft, ValueEntity rootValue) {}

    /**
     * Sets up a FixedThreshold node with min=10, max=100 and a single root value
     * with the given range value. Persists everything in one transaction.
     *
     * Nodes are persisted in order fingerprint, groupBy, range so that their IDs
     * match the positional order FixedThreshold.setNodes() expects — working around
     * a Hibernate @OrderColumn issue where sources load in ID order.
     */
    private ThresholdFixture setupThreshold(double rangeValue) throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity fingerprintNode = new JqNode("fingerprint", ".fingerprint");
        fingerprintNode.persist();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity rangeNode = new JqNode("range", ".y");
        rangeNode.persist();

        ValueEntity rootVal = new ValueEntity(null, rootNode, new TextNode("root1"));
        rootVal.persist();

        ValueEntity rv = new ValueEntity(null, rangeNode, DoubleNode.valueOf(rangeValue));
        rv.sources = List.of(rootVal);
        rv.persist();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode fpData = mapper.createObjectNode();
        fpData.put("env", "test");
        ValueEntity fpValue = new ValueEntity(null, fingerprintNode, fpData);
        fpValue.sources = List.of(rootVal);
        fpValue.persist();

        FixedThreshold ft = new FixedThreshold("ftNode", "");
        ft.setMin(10);
        ft.setMax(100);
        ft.setMinInclusive(true);
        ft.setMaxInclusive(true);
        ft.setNodes(fingerprintNode, rootNode, rangeNode);
        ft.persist();

        Work work = new Work(ft, new ArrayList<>(ft.sources), List.of(rootVal));
        work.persist();
        tm.commit();

        return new ThresholdFixture(ft, rootVal);
    }

    private Work loadWork(NodeEntity activeNode) throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Work work = Work.find("?1 member of activeNodes", activeNode).firstResult();
        work.sourceValues.size();
        work.sourceNodes.size();
        tm.commit();
        return work;
    }

    @Test
    public void event_fires_when_threshold_violated() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        // value 5.0 is below min=10 → violation
        ThresholdFixture fixture = setupThreshold(5.0);
        Work work = loadWork(fixture.ft());

        workService.execute(work);

        assertEquals(1, eventObserver.getEvents().size(), "should fire exactly one ChangeDetectedEvent");
        ChangeDetectedEvent event = eventObserver.getEvents().getFirst();
        assertEquals(fixture.ft().getId(), event.nodeId());
        assertEquals("ftNode", event.nodeName());
        assertFalse(event.valueIds().isEmpty(), "event should contain value IDs");
    }

    @Test
    public void event_does_not_fire_when_no_violation() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        // value 50.0 is within [10, 100] → no violation
        ThresholdFixture fixture = setupThreshold(50.0);
        Work work = loadWork(fixture.ft());

        workService.execute(work);

        assertEquals(0, eventObserver.getEvents().size(), "should not fire ChangeDetectedEvent when value is within threshold");
    }

    @Test
    public void recalculation_does_not_fire_when_value_unchanged() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        // first calculation: value 5.0 is below min=10 → violation, event fires
        ThresholdFixture fixture = setupThreshold(5.0);
        Work work = loadWork(fixture.ft());
        workService.execute(work);

        assertEquals(1, eventObserver.getEvents().size(), "first calculation should fire event");
        eventObserver.clear();

        // recalculation: same data, should produce identical value → deduplicated, no event
        tm.begin();
        FixedThreshold managedFt = FixedThreshold.findById(fixture.ft().getId());
        ValueEntity managedRoot = ValueEntity.findById(fixture.rootValue().id);
        Work recalc = new Work(managedFt, new ArrayList<>(managedFt.sources), List.of(managedRoot));
        recalc.persist();
        tm.commit();

        Work loadedRecalc = loadWork(managedFt);
        workService.execute(loadedRecalc);

        assertEquals(0, eventObserver.getEvents().size(), "recalculation with identical value should not fire event");
    }

    @Test
    public void event_does_not_fire_for_non_detection_node() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        JqNode jqNode = new JqNode("extract", ".y", rootNode);
        jqNode.persist();

        ValueEntity rootValue = new ValueEntity(null, rootNode, new TextNode("{\"y\": 42}"));
        rootValue.persist();

        Work work = new Work(jqNode, new ArrayList<>(jqNode.sources), List.of(rootValue));
        work.persist();
        tm.commit();

        Work loaded = loadWork(jqNode);
        workService.execute(loaded);

        assertEquals(0, eventObserver.getEvents().size(), "should not fire ChangeDetectedEvent for non-detection nodes");
    }
}
