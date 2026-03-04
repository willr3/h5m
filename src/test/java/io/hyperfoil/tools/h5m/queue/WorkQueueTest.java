package io.hyperfoil.tools.h5m.queue;

import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RelativeDifference;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.svc.WorkService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class WorkQueueTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    WorkService workService;


    @Test
    public void reject_relativedifference_as_duplicate(){
        NodeEntity rootNode = new JqNode("root",".root");
        NodeEntity relativeDifference = new RelativeDifference();
        ValueEntity rootValue1 = new ValueEntity(null,rootNode,new TextNode("text1"));
        ValueEntity rootValue2 = new ValueEntity(null,rootNode,new TextNode("text2"));

        Work work1 = new Work(relativeDifference,List.of(rootNode),List.of(rootValue1));
        Work work2 = new Work(relativeDifference,List.of(rootNode),List.of(rootValue2));

        assertEquals(work1.hashCode(),work2.hashCode(),"both worth should have the same hashcode despite different values");

        WorkQueue q = new WorkQueue(null,null,null);

        boolean added = q.addWork(work1);
        assertTrue(added,"first work should be added");
        added = q.addWork(work2);
        assertTrue(q.isPending(work2),"work 2 should be pending since it matches work1");
        assertFalse(added,"seconds work should not be added because it should have the same hash");
    }

    @Test
    public void reject_duplicates() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        WorkQueue q = new WorkQueue(null,null,null);
        tm.begin();
        RootNode root = new RootNode();
        root.persist();
        NodeEntity aNode = new JqNode("a",".a",root);
        aNode.persist();
        ValueEntity rootValue = new ValueEntity(null,aNode,new TextNode("found"));
        rootValue.persist();


        Work first = new Work(aNode,aNode.sources,List.of(rootValue));
        first.persist();
        Work second = new Work(aNode,aNode.sources,List.of(rootValue));
        second.persist();

        assertEquals(first.hashCode(),second.hashCode(),"work with different id but same scope should have the same hash");
        q.addWork(first);
        boolean added = q.addWork(second);
        assertFalse(added,"second should not be added to the queue");

        tm.commit();
    }

    @Test
    public void poll_null_until_source_completes() throws InterruptedException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        WorkQueue q = new WorkQueue(null,null,null);

        tm.begin();
        NodeEntity aNode = new JqNode("a");
        aNode.persist();
        NodeEntity bNode = new JqNode("b");
        bNode.sources= List.of(aNode);
        bNode.persist();

        Work aWork = new Work(aNode,null,null);
        aWork.persist();
        Work bWork = new Work(bNode,null,null);
        bWork.persist();
        tm.commit();

        q.addWork(bWork);
        q.addWork(aWork);
        Runnable firstRunnable = q.take();

        assertNotNull(firstRunnable);

        assertFalse(q.isPending(aWork),"a should be removed from the q");
        assertTrue(q.isPending(bWork),"b should remain in the queue");

        Runnable polled = q.poll();

        assertNull(polled,"b should remain in q until a completes");

        q.decrement(aWork);//fake call to Runnable.run

        polled = q.poll();
        assertNotNull(polled,"b work should return now that a is complete");
    }

    @Test
    public void poll_return_first_non_blocked() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        WorkQueue q = new WorkQueue(null,null,null);

        tm.begin();
        NodeEntity aNode = new JqNode("a");
        aNode.persist();
        NodeEntity bNode = new JqNode("b");
        bNode.sources= List.of(aNode);
        bNode.persist();
        NodeEntity cNode = new JqNode("c");
        cNode.persist();

        Work aWork = new Work(aNode,null,null);
        aWork.persist();
        Work bWork = new Work(bNode,null,null);
        bWork.persist();
        Work cWork = new Work(cNode,null,null);
        cWork.persist();
        tm.commit();

        q.addWork(aWork);
        q.addWork(bWork);
        q.addWork(cWork);

        Runnable firstRunnable = q.poll();
        assertFalse(q.isPending(aWork),"a should be removed from the q");

        Runnable polled = q.poll();

        assertNotNull(polled,"poll should pull next runnable work");
        assertTrue(q.isPending(bWork),"b should remain in the queue");
        assertFalse(q.isPending(cWork),"c should not be in the queue");
    }


}
