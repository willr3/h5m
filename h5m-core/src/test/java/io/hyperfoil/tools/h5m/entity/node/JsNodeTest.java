package io.hyperfoil.tools.h5m.entity.node;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.Value;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class JsNodeTest {

    @Test
    public void isNullEmptyOrIdentityFunction(){
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("args => args"));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("(args)=> args"));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("( args ) => args"));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("( args ) => { console.log(args); return args; }"));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("function( args ){ console.log(args); return args; }"));
    }

    @Test
    public void getParameterNames_empty_named_star_function(){
        List<String> params = JsNode.getParameterNames("function* foo(){ yield [1,2,3]; yield 4;}");
        assertNotNull(params);
        assertTrue(params.isEmpty(),"expect to be empty:"+params);
    }
    @Test
    public void getParameterNames_named_star_function_no_space(){
        List<String> params = JsNode.getParameterNames("function* dataset({foo,  bar, biz}){\nyield foo;\nyield bar;\nyield biz;}");
        assertNotNull(params);
        assertEquals(3, params.size(),"expect 3 entries");
        assertEquals("foo",params.get(0));
        assertEquals("bar",params.get(1));
        assertEquals("biz",params.get(2));
    }
    @Test
    public void getParameterNames_named_star_function_multiline(){
        List<String> params = JsNode.getParameterNames("function* dataset({\nfoo,\n  bar,\n biz}){\nyield foo;\nyield bar;\nyield biz;}");
        assertNotNull(params);
        assertEquals(3, params.size(),"expect 3 entries");
        assertEquals("foo",params.get(0));
        assertEquals("bar",params.get(1));
        assertEquals("biz",params.get(2));
    }
    @Test
    public void getParameterNames_named_star_function_multiline_keep_spread(){
        List<String> params = JsNode.getParameterNames("function* dataset({\nfoo,\n  bar,\n biz}){\nyield foo;\nyield bar;\nyield biz;}",false);
        assertNotNull(params);
        assertEquals(3, params.size(),"expect 3 entries");
        assertEquals("{\nfoo",params.get(0));
        assertEquals("bar",params.get(1));
        assertEquals("biz}",params.get(2));
    }
    @Test
    public void getParameterNames_comment_and_default_values(){
        List<String> params = JsNode.getParameterNames("""
                /**
                 * Calculates the mean or coefficient of variation % (CV%) for a pre-filtered array of log entries.
                 *
                 * @param {Array<Object>} filteredArr - A pre-filtered array of log entry objects.
                 * @param {string} [logData="time"] - The property name of the value to be processed (e.g., "time", "activated").
                 * @param {boolean} [stddev=false] - If true, calculates the coefficient of variation (as a percentage) instead of the mean.
                 * @param {boolean} [usePopulation=false] - If true, uses the population formula for standard deviation (N), otherwise uses the sample formula (N-1).
                 * @returns {string} The calculated value formatted to three decimal places, or "NaN" if the input is invalid or empty.
                 */
                (filteredArr, logData = "time", stddev = false, usePopulation = false) => {
                }
                """);
        System.out.println(params.size());
        for(int i = 0; i < params.size(); i++){
            System.out.printf("%2d %s%n",i,params.get(i));
        }
    }

    @Test
    public void getParameterNames_empty(){
        List<String> params = JsNode.getParameterNames("");
        assertNull(params,"return should be null when input is not a function");
    }

    @Test
    public void getParameterNames_empty_function(){
        List<String> params = JsNode.getParameterNames("function(){return 42}");
        assertNotNull(params,"return should not be null");
        assertTrue(params.isEmpty(),"expect to be empty:"+params);
    }
    @Test
    public void getParameterNames_empty_arrow(){
        List<String> params = JsNode.getParameterNames("()=>42");
        assertNotNull(params,"return should not be null");
        assertTrue(params.isEmpty(),"expect to be empty:"+params);
    }
    @Test
    public void getParameterNames_multiple_function(){
        List<String> params = JsNode.getParameterNames("function(a,b, c , d){return 42}");
        assertNotNull(params,"return should not be null");
        assertEquals(4, params.size(),"expected 4 values: "+params);
        assertEquals("a", params.get(0),"expected a value");
        assertEquals("b", params.get(1),"expected b value");
        assertEquals("c", params.get(2),"expected c value");
        assertEquals("d", params.get(3),"expected d value");
    }
    @Test
    public void getParameterNames_multiple_arrow(){
        List<String> params = JsNode.getParameterNames("(a,b, c , d)=>42");
        assertNotNull(params,"return should not be null");
        assertEquals(4, params.size(),"expected 4 values: "+params);
        assertEquals("a", params.get(0),"expected a value");
        assertEquals("b", params.get(1),"expected b value");
        assertEquals("c", params.get(2),"expected c value");
        assertEquals("d", params.get(3),"expected d value");
    }
    @Test
    public void getParameterNames_one_arrow(){
        List<String> params = JsNode.getParameterNames("a=>42");
        assertNotNull(params,"return should not be null");
        assertEquals(1, params.size(),"expected 1 values: "+params);
        assertEquals("a", params.get(0),"expected a value");
    }

    @Test
    public void getParameterNames_ellipsis_function(){
        List<String> params = JsNode.getParameterNames("function(a,...b){return 42}");
        assertEquals(2, params.size(),"expected 4 values: "+params);
        assertEquals("a", params.get(0),"expected a value");
        assertEquals("b", params.get(1),"expected b value");
    }
    @Test
    public void getParameterNames_destructure_function(){
        List<String> params = JsNode.getParameterNames("function({a,b},c){return 42}");
        assertNotNull(params,"return should not be null");
        assertEquals(3, params.size(),"expected 4 values: "+params);
        assertEquals("a", params.get(0),"expected a value");
        assertEquals("b", params.get(1),"expected b value");
        assertEquals("c", params.get(2),"expected c value");
    }

    //we would need this to be fully compliant but this is not used in current Horreum
    @Test @Disabled
    public void getParameterNames_nested_destructure(){
        List<String> params = JsNode.getParameterNames("function({ user: { firstName, lastName }, address: { city, country } }) {");
        assertNotNull(params,"return should not be null");
        assertEquals(4, params.size(),"expected 4 values: "+params);
        assertEquals("firstName", params.get(0),"expected a value");
    }
    @Test @Disabled
    public void getParameterNames_nested_destructure_array(){
        List<String> params = JsNode.getParameterNames("function([studentName, [score1, score2, score3]]){}");
        assertNotNull(params,"return should not be null");
        assertEquals(4, params.size(),"expected 4 values: "+params);
    }
    @Test
    public void createParameters_destructure() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Value> values = Map.of(
                "a",new Value(null,null,mapper.readTree("1")),
                "b",new Value(null,null,mapper.readTree("2")),
                "c",new Value(null,null,mapper.readTree("3"))
        );
        List<JsonNode> params = JsNode.createParameters("function({a,b},c){}",values);
        assertNotNull(params,"return should not be null");
        assertEquals(2,params.size(),"expected 2 values: "+params);
        JsonNode node = params.get(0);
        assertNotNull(node,"return should not be null");
        assertInstanceOf(ObjectNode.class,node);
        ObjectNode objectNode = (ObjectNode)node;
        assertTrue(objectNode.has("a"),"expected a value: "+objectNode.toString());
        assertEquals("1",objectNode.get("a").toString(),"expected a value: "+objectNode.toString());
        assertTrue(objectNode.has("b"),"expected a value: "+objectNode.toString());
        assertEquals("2",objectNode.get("b").toString(),"expected b value: "+objectNode.toString());
        assertEquals("3",params.get(1).toString(),"expected b value: "+objectNode.toString());
    }
    @Test
    public void createParameters_single_value_different_name() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Value> values = Map.of(
                "a",new Value(null,null,mapper.readTree("1"))
        );
        List<JsonNode> params = JsNode.createParameters("function(v){return v;}",values);
        assertNotNull(params,"return should not be null");
        assertEquals(1,params.size(),"expected 2 values: "+params);
        JsonNode node = params.get(0);
        assertNotNull(node,"return should not be null");
        assertInstanceOf(IntNode.class,node);

    }

    @Test
    public void parse_sources(){
        Map<String, Node> existing = Map.of("a",new JqNode("a",".a"),
                "b",new JqNode("b",".b"));

        Function<String,List<Node>> getExisting = new Function<String,List<Node>>(){
            @Override
            public List<Node> apply(String s) {
                return existing.containsKey(s) ? List.of(existing.get(s)) : Collections.emptyList();
            }
        };
        JsNode node = JsNode.parse("node","(a,b)=>a+b",getExisting);
        assertNotNull(node,"node should not be null");
        List<Node> sources = node.sources;
        assertNotNull(sources,"sources should not be null");
        assertEquals(2,sources.size(),"expected 2 values: "+sources);
        assertEquals(existing.get("a"),sources.get(0),"expected a value");
        assertEquals(existing.get("b"),sources.get(1),"expected b value");
    }

}
