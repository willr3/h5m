package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestProfile(TwoCoreThreads.class)
@QuarkusTest
public class WorkServiceConcurrencyTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    EntityManager em;

    @Inject
    WorkService workService;

    @Test()
    @Timeout(10)//fail after 10 seconds
    public void foo() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException, InterruptedException {

        tm.begin();
        NodeGroupEntity nodeGroup = new NodeGroupEntity("test");
        nodeGroup.persist();
        NodeEntity node1 = new JqNode("one",".one",nodeGroup.root);
        node1.persist();
        NodeEntity node2 = new JqNode("two",".two",nodeGroup.root);
        node2.persist();
        NodeEntity node3 = new JqNode("both",".", List.of(node1,node2));
        node3.persist();
        ObjectMapper mapper = new ObjectMapper();
        ValueEntity value = new ValueEntity(null,nodeGroup.root,mapper.readTree(
            """
            { "one" : "a", "two" : "b" }
            """
        ));
        value.persist();
        tm.commit();

        Work work1 = new Work(node1,node1.sources,List.of(value));
        Work work2 = new Work(node2,node2.sources,List.of(value));

        workService.create(List.of(work1,work2));
        while(!workService.isIdle()){
            Thread.sleep(1_000);
        }

        long workCount = Work.count();
        long valueCount = ValueEntity.count();

        assertEquals(4,valueCount,"the expected number of values should be created");
        assertEquals(0,workCount,"no additional work should be left in the db when workService.isIdle");

    }
}
