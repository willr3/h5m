package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.transaction.*;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(WorkServiceTest.NoWorkers.class)
public class WorkServiceTest extends FreshDb {

    public static class NoWorkers implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("h5m.worker.core", "0");
        }
    }

    @Inject
    TransactionManager tm;

    @Inject
    WorkService workService;



    @Test
    public void execute_not_cascade() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.persist();
        NodeEntity parent = new JqNode("parent",".a",root);
        parent.persist();
        NodeEntity child = new JqNode("child",".b",parent);
        child.persist();
        ValueEntity value = new ValueEntity(null,root, JqValues.parse("""
                { "a" : { "b" : "found" } }
                """));
        value.persist();

        tm.commit();

        Work parentWork = new Work(parent,parent.sources, List.of(value.id));
        parentWork.setCascade(false);
        workService.execute(parentWork);
        assertEquals(0,workService.getQueue().size(),"parent should not queue child work");
    }

}
