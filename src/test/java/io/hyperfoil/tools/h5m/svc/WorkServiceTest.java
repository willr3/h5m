package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class WorkServiceTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    WorkService workService;

    @Test
    public void dependsOn_node_dependency_no_value() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity one = new JqNode("one");
        one.persist();
        NodeEntity two = new JqNode("two");
        two.sources = List.of(one);
        two.persist();

        Work wOne = new Work(one,null, null);
        wOne.persist();
        Work wTwo = new Work(two,null, null);
        wTwo.persist();
        tm.commit();

        assertTrue(workService.dependsOn(wTwo,wOne));
    }

    @Test
    public void dependsOn_same_work() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity one = new JqNode("one");
        one.persist();
        NodeEntity two = new JqNode("two");
        two.sources = List.of(one);
        two.persist();

        Work wOne = new Work(one,null, null);
        wOne.persist();
        tm.commit();

        assertFalse(workService.dependsOn(wOne,wOne));
    }

    @Test
    public void dependsOn_node_dependency_same_value() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new JqNode("root");
        root.persist();
        NodeEntity one = new JqNode("one");
        one.persist();
        NodeEntity two = new JqNode("two");
        two.sources = List.of(one);
        two.persist();

        ValueEntity value = new ValueEntity(null,root);
        value.persist();
        Work wOne = new Work(one,null, List.of(value));
        wOne.persist();
        Work wTwo = new Work(two,null, List.of(value));
        wTwo.persist();

        tm.commit();
        assertTrue(workService.dependsOn(wTwo,wOne));
    }

    @Test
    public void dependsOn_node_dependency_different_value() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new JqNode("root");
        root.persist();
        NodeEntity one = new JqNode("one");
        one.persist();
        NodeEntity two = new JqNode("two");
        two.sources = List.of(one);
        two.persist();
        ValueEntity value1 = new ValueEntity(null,root);
        value1.persist();
        ValueEntity value2 = new ValueEntity(null,one);
        value2.persist();

        Work wOne = new Work(one,null, List.of(value1));
        wOne.persist();
        Work wTwo = new Work(two,null, List.of(value2));
        wTwo.persist();

        tm.commit();

        assertFalse(workService.dependsOn(wTwo,wOne));
    }
}
