package io.hyperfoil.tools.h5m.entity.node;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class JqNodeTest {

    @Test
    public void isNullInput(){
        assertTrue(JqNode.isNullInput("[inputs]"),"treating all inputs as an array should be a null input");
        assertFalse(JqNode.isNullInput(".inputs"),"accessing the inputs key should not trigger null input");
    }




}
