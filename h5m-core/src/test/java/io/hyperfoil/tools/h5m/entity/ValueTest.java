package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.util.List;

@QuarkusTest
public class ValueTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    EntityManager em;

    @Test
    public void sources() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        RootNode root = new RootNode();
        root.persist();

        Node node = new JqNode("foo");
        node.sources=List.of(root);
        node.persist();

        Node bar = new JqNode("bar");
        bar.sources=List.of(root,node);
        bar.persist();

        Value rootValue = new Value(null,root);
        rootValue.persist();

        Value nodeValue01 = new Value(null,node);
        nodeValue01.sources= List.of(rootValue);
        nodeValue01.persist();

        Value nodeValue02 = new Value(null,node);
        nodeValue02.sources= List.of(rootValue);
        nodeValue02.persist();

        Value barValue01 = new Value(null,bar);
        barValue01.sources= List.of(nodeValue01,nodeValue02);
        barValue01.persist();

        tm.commit();

        Value found = Value.findById(barValue01.id);

        System.out.println(found.sources);
    }
}
