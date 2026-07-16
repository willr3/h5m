package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.hyperfoil.tools.h5m.api.Upload;
import io.hyperfoil.tools.h5m.api.Value;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(WorkServiceTest.NoWorkers.class)
public class RecalculateWorkQueueTest {

    @Inject
    TransactionManager tm;

    @Inject
    WorkService workService;

    @Inject
    FolderService folderService;
    @Inject
    NodeService nodeService;
    @Inject
    ValueService valueService;

    private String printQueue(){
        return workService.getQueue().stream().toList().stream().map(Object::toString).collect(Collectors.joining("\n"));
    }

    @Test
    public void recalculateNode_before_first_upload_node() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        tm.begin();
        long folderId = folderService.create(testName);
        FolderEntity folder = folderService.read(folderId);

        assertNotNull(folder);

        NodeEntity a = new JqNode("a",".a",folder.group.root);
        a.group=folder.group;
        a.persist();
        NodeEntity b = new JqNode("b",".",a);
        b.group=folder.group;
        b.persist();
        tm.commit();

        Upload uploaded = folderService.upload(testName,"",JqValues.parse(
                """
                { "a" : 1  }
                """
        ));
        assertEquals(1,workService.getQueue().size(),"only a should be queued by upload\n"+printQueue());

        tm.begin();
        NodeEntity found = nodeService.read(a.id);
        found.operation = ".a + 1";
        found.persist();
        tm.commit();

        RecalculationTracker aTracker = folderService.recalculateNode(found.id);

        //TODO should aTracker be done because it doesn't queue any work or should it be one when upload is done?
        assertFalse(aTracker.getFuture().isDone());
        assertFalse(aTracker.getFuture().isCancelled());
        assertFalse(uploaded.future.isDone());
        assertFalse(uploaded.future.isCancelled());

        assertEquals(1,workService.getQueue().size(),"only a should be queued by upload\n"+printQueue());

        Runnable todo = workService.getQueue().poll();
        assertNotNull(todo);
        if(todo instanceof Work w){
            assertTrue(w.isDispatch(),"work should be dispatch due to upload");
        }else{
            fail("runnable should be instance of Work");
        }

        workService.execute((Work)todo);

        assertFalse(aTracker.getFuture().isDone());
        assertFalse(aTracker.getFuture().isCancelled());
        assertFalse(uploaded.future.isDone());
        assertFalse(uploaded.future.isCancelled());

        assertEquals(1,workService.getQueue().size(),"b should be queued by a\n"+printQueue());
        //check the value before it is removed by ephemeral
        List<Value> aValues = valueService.getNodeValues(a.id);
        assertEquals(1,aValues.size(),"a create 1 value:"+aValues);
        Value aValue = aValues.getFirst();
        assertNotNull(aValue);
        assertEquals(JqValues.parse("2"),aValue.data(),"value should reflect change to node that occurred while queued");


        todo = workService.getQueue().poll();
        assertNotNull(todo);
        if(todo instanceof Work w){
            assertTrue(w.isDispatch(),"work should be dispatch due to upload");
        }else{
            fail("runnable should be instance of Work");
        }
        workService.execute((Work)todo);
        assertEquals(0,workService.getQueue().size(),"the queue should be empty");

        assertTrue(aTracker.getFuture().isDone());
        assertFalse(aTracker.getFuture().isCancelled());
        assertTrue(uploaded.future.isDone());
        assertFalse(uploaded.future.isCancelled());


        List<Value> bValues = valueService.getNodeValues(b.id);
        assertEquals(1,bValues.size(),"b create 1 value:"+bValues);
        Value bValue = bValues.getFirst();
        assertNotNull(bValue);
        assertEquals(JqValues.parse("2"),bValue.data(),"value should reflect change to node that occurred while queued");

    }

    @Test
    //@Disabled("java.lang.IllegalStateException: Cannot recalculate while 1 upload(s) are in progress for folder recalculate_during_upload_uses_new_operation")
    public void recalculateNode_pending_node_during_upload() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        tm.begin();
        long folderId = folderService.create(testName);
        FolderEntity folder = folderService.read(folderId);

        assertNotNull(folder);

        NodeEntity a = new JqNode("a",".a",folder.group.root);
        a.group=folder.group;
        a.ephemeral=EphemeralMode.KEEP;
        a.persist();
        NodeEntity b = new JqNode("b",".",a);
        b.group=folder.group;
        b.persist();
        tm.commit();

        Upload uploaded = folderService.upload(testName,"",JqValues.parse(
            """
            { "a" : 1  }
            """
        ));
        assertEquals(1,workService.getQueue().size(),"only a should be queued by upload");
        Runnable todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);

        assertEquals(1,workService.getQueue().size(),"b should be queued by a");

        tm.begin();
        NodeEntity found = nodeService.read(b.id);
        found.operation = ". + 1";
        found.persist();
        tm.commit();

        RecalculationTracker bTracker = folderService.recalculateNode(found.id);
        //the changed node is already in the work queue and should not cause a new work to be queued
        assertEquals(1,workService.getQueue().size());
        todo = workService.getQueue().poll();
        assertNotNull(todo);
        if(todo instanceof Work w){
            assertTrue(w.isDispatch(),"work should be dispatch because of upload");
        }else{
            fail("the runnable should be an instance of Work");
        }
        workService.execute((Work)todo);
        assertEquals(0,workService.getQueue().size(),"the queue should be empty\n"+printQueue());

        List<Value> bValues = valueService.getNodeValues(b.id);
        assertEquals(1,bValues.size(),"b create 1 value:"+bValues);
        Value bValue = bValues.getFirst();
        assertNotNull(bValue);
        assertEquals(JqValues.parse("2"),bValue.data(),"value should reflect change to node that occurred while queued");
    }
    @Test
    //@Disabled("java.lang.IllegalStateException: Cannot recalculate while 1 upload(s) are in progress for folder recalculate_during_upload_uses_new_operation")
    public void recalculateNode_active_node_during_upload() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        tm.begin();
        long folderId = folderService.create(testName);
        FolderEntity folder = folderService.read(folderId);

        assertNotNull(folder);

        NodeEntity a = new JqNode("a",".a",folder.group.root);
        a.group=folder.group;
        a.ephemeral= EphemeralMode.KEEP;
        a.persist();
        NodeEntity b = new JqNode("b",".",a);
        b.group=folder.group;
        b.persist();
        tm.commit();

        Upload uploaded = folderService.upload(testName,"",JqValues.parse(
                """
                { "a" : 1  }
                """
        ));
        assertEquals(1,workService.getQueue().size(),"only a should be queued by upload");
        Runnable todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);

        assertEquals(1,workService.getQueue().size(),"b should be queued by a");

        todo = workService.getQueue().poll();
        assertEquals(0,workService.getQueue().size(),"the queue should be empty");
        assertNotNull(todo);

        assertEquals(0,workService.getQueue().size());

        tm.begin();
        NodeEntity found = nodeService.read(b.id);
        found.operation = ". + 1";
        found.persist();
        tm.commit();

        folderService.recalculateNode(found.id);

        assertEquals(0,workService.getQueue().size());
        //the changed node is already in the work queue and should not cause a new work to be queued
        if(todo instanceof Work w){
            assertTrue(w.isDispatch(),"work should be dispatch because of upload");
        }else{
            fail("the runnable should be an instance of Work");
        }
        workService.execute((Work)todo);
        assertEquals(0,workService.getQueue().size(),"the queue should be empty");

        List<Value> bValues = valueService.getNodeValues(b.id);
        assertEquals(1,bValues.size(),"b create 1 value:"+bValues);
        Value bValue = bValues.getFirst();
        assertNotNull(bValue);
        assertEquals(JqValues.parse("2"),bValue.data(),"value should reflect change to node that occurred while queued");
    }

    @Test
    //@Disabled("java.lang.IllegalStateException: Cannot recalculate while 1 upload(s) are in progress for folder recalculate_during_upload_uses_new_operation")
    public void recalculateNode_ancestor_of_active_upload_node() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        tm.begin();
        long folderId = folderService.create(testName);
        FolderEntity folder = folderService.read(folderId);

        assertNotNull(folder);

        NodeEntity a = new JqNode("a",".a",folder.group.root);
        a.group=folder.group;
        a.persist();
        NodeEntity b = new JqNode("b",".",a);
        b.group=folder.group;
        b.persist();
        tm.commit();

        Upload uploaded = folderService.upload(testName,"",JqValues.parse(
                """
                { "a" : 1  }
                """
        ));
        assertEquals(1,workService.getQueue().size(),"only a should be queued by upload");
        Runnable todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);
        assertEquals(1,workService.getQueue().size(),"b should be queued by a");
        todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        assertEquals(0,workService.getQueue().size(),"queue should be empty");

        tm.begin();
        NodeEntity found = nodeService.read(a.id);
        found.operation = ".a + 1";
        found.persist();
        tm.commit();

        folderService.recalculateNode(found.id);
        assertEquals(1,workService.getQueue().size(),"a should be queued");
        Runnable pending =  workService.getQueue().peek();
        assertNotNull(pending);
        if(pending instanceof Work w){
            assertNotNull(w.getActiveNodes());
            assertTrue(w.getActiveNodes().stream().anyMatch(n->n.name.equals(a.name)),"a should be pending");
        }else{
            fail("pending runnable should have been an instance of Work");
        }

        workService.execute((Work)todo);
        assertEquals(1,workService.getQueue().size(),"finishing b should not queue more work");

        List<Value> bValues = valueService.getNodeValues(b.id);
        assertEquals(1,bValues.size(),"b create 1 value:"+bValues);
        Value bValue = bValues.getFirst();
        assertNotNull(bValue);
        assertEquals(JqValues.parse("1"),bValue.data(),"value should use previous value from a");

        todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);
        assertEquals(1,workService.getQueue().size(),"b should be queued by a");

        todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);
        assertEquals(0,workService.getQueue().size(),"queue should be empty");


        bValues = valueService.getNodeValues(b.id);
        assertEquals(1,bValues.size(),"b create 1 value:"+bValues);
        bValue = bValues.getFirst();
        assertNotNull(bValue);
        assertEquals(JqValues.parse("2"),bValue.data(),"value should reflect change to node that occurred while queued");

    }

    @Test
    public void recalculateNode_ancestor_of_pending_recalculate_node() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        tm.begin();
        long folderId = folderService.create(testName);
        FolderEntity folder = folderService.read(folderId);

        assertNotNull(folder);

        NodeEntity a = new JqNode("a",".a",folder.group.root);
        a.group=folder.group;
        a.persist();
        NodeEntity b = new JqNode("b",".",a);
        b.group=folder.group;
        b.persist();
        tm.commit();

        assertTrue(nodeService.isEphemeral(a),"a should be ephemeral");

        Upload uploaded = folderService.upload(testName,"",JqValues.parse(
                """
                { "a" : 1  }
                """
        ));
        assertEquals(1,workService.getQueue().size(),"only a should be queued by upload");
        Runnable todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);
        assertEquals(1,workService.getQueue().size(),"b should be queued by a");
        todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);
        assertEquals(0,workService.getQueue().size(),"queue should be empty");

        tm.begin();
        NodeEntity found = nodeService.read(b.id);
        found.operation = ". + 1";
        found.persist();
        tm.commit();

        assertTrue(uploaded.future.isDone(),"upload should be complete");

        RecalculationTracker bTracker = folderService.recalculateNode(found.id);

        assertEquals(2,workService.getQueue().size(),"queue should have a and b because a is ephemeral:\n"+printQueue());
        todo = workService.getQueue().poll();
        assertNotNull(todo);
        if(todo instanceof Work w){
            assertFalse(w.isCascade(),"a should not cascade");
        }else{
            fail("runnable should be a work instance");
        }
        workService.execute((Work)todo);
        assertEquals(1,workService.getQueue().size(),"b should be queued");

        tm.begin();
        found = nodeService.read(a.id);
        found.operation = ".a + 1";
        found.persist();
        tm.commit();

        RecalculationTracker aTracker = folderService.recalculateNode(found.id);

        assertFalse(bTracker.getFuture().isDone());
        assertFalse(bTracker.getFuture().isCancelled());

        assertEquals(2,workService.getQueue().size(),"queue should have a and b:\n"+printQueue());

        todo = workService.getQueue().poll();
        if(todo instanceof Work w){
            assertNotNull(w.getActiveNodes());
            assertTrue(w.getActiveNodes().stream().anyMatch(n->n.name.equals(a.name)),"a should be selected: "+w.getActiveNodes());
        }else{
            fail("pending runnable should have been an instance of Work");
        }
        workService.execute((Work)todo);

        List<Value> aValues = valueService.getNodeValues(a.id);
        assertEquals(1,aValues.size(),"a create 1 value:"+aValues);
        Value aValue = aValues.getFirst();
        assertNotNull(aValue);
        assertEquals(JqValues.parse("2"),aValue.data(),"value should reflect change to node that occurred while queued");

        assertEquals(1,workService.getQueue().size(),"b should be queued\n"+printQueue());

        assertFalse(aTracker.getFuture().isDone());
        assertFalse(bTracker.getFuture().isDone());

        todo = workService.getQueue().poll();
        assertNotNull(todo);
        assertInstanceOf(Work.class, todo);
        workService.execute((Work)todo);

        assertEquals(0,workService.getQueue().size(),"queue should be empty");

        assertTrue(aTracker.getFuture().isDone());
        assertTrue(bTracker.getFuture().isDone());


        List<Value> bValues = valueService.getNodeValues(b.id);
        assertEquals(1,bValues.size(),"b create 1 value:"+bValues);
        Value bValue = bValues.getFirst();
        assertNotNull(bValue);
        assertEquals(JqValues.parse("3"),bValue.data(),"value should reflect change to node that occurred while queued");
    }

}
