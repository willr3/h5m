package exp.svc;

import exp.FreshDb;
import exp.entity.Node;
import exp.entity.Value;
import exp.entity.Work;
import exp.entity.node.JqNode;
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
        Node one = new JqNode("one");
        one.persist();
        Node two = new JqNode("two");
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
        Node one = new JqNode("one");
        one.persist();
        Node two = new JqNode("two");
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
        Node root = new JqNode("root");
        root.persist();
        Node one = new JqNode("one");
        one.persist();
        Node two = new JqNode("two");
        two.sources = List.of(one);
        two.persist();

        Value value = new Value(null,root);
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
        Node root = new JqNode("root");
        root.persist();
        Node one = new JqNode("one");
        one.persist();
        Node two = new JqNode("two");
        two.sources = List.of(one);
        two.persist();
        Value value1 = new Value(null,root);
        value1.persist();
        Value value2 = new Value(null,one);
        value2.persist();

        Work wOne = new Work(one,null, List.of(value1));
        wOne.persist();
        Work wTwo = new Work(two,null, List.of(value2));
        wTwo.persist();

        tm.commit();

        Work found = Work.findById(wTwo.id);

        assertFalse(workService.dependsOn(wTwo,wOne));
    }

}