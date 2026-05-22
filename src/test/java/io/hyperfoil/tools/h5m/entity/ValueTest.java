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

        NodeEntity node = new JqNode("foo");
        node.sources=List.of(root);
        node.persist();

        NodeEntity bar = new JqNode("bar");
        bar.sources=List.of(root,node);
        bar.persist();

        ValueEntity rootValue = new ValueEntity(null,root);
        rootValue.persist();

        ValueEntity nodeValue01 = new ValueEntity(null,node);
        nodeValue01.sources= List.of(rootValue);
        nodeValue01.persist();

        ValueEntity nodeValue02 = new ValueEntity(null,node);
        nodeValue02.sources= List.of(rootValue);
        nodeValue02.persist();

        ValueEntity barValue01 = new ValueEntity(null,bar);
        barValue01.sources= List.of(nodeValue01,nodeValue02);
        barValue01.persist();

        tm.commit();

        ValueEntity found = ValueEntity.findById(barValue01.id);

        System.out.println(found.sources);
    }
}
