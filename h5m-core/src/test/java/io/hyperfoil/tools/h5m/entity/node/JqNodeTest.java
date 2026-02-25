package io.hyperfoil.tools.h5m.entity.node;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class JqNodeTest {

    @Test
    public void isNullInput(){
        assertTrue(JqNode.isNullInput("[inputs]"),"treating all inputs as an array should be a null input");
        assertTrue(JqNode.isNullInput("inputs"),"bare inputs should be a null input");
        assertTrue(JqNode.isNullInput("[inputs | .foo]"),"inputs in a pipeline should be a null input");
        assertFalse(JqNode.isNullInput(".inputs"),"accessing the inputs key should not trigger null input");
        assertFalse(JqNode.isNullInput(".foo.inputs"),"nested field access should not trigger null input");
        assertFalse(JqNode.isNullInput("myinputs"),"inputs as substring of another word should not trigger null input");
        assertFalse(JqNode.isNullInput("inputstream"),"inputs prefix of another word should not trigger null input");
    }




}
