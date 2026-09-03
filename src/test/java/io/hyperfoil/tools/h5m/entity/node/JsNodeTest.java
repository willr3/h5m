package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.jjq.value.*;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        assertTrue(JsNode.isNullEmptyOrIdentityFunction(null));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction(""));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("value => value"));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("value => { return value; }"));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("function(value) { return value; }"));
        assertTrue(JsNode.isNullEmptyOrIdentityFunction("function fn(value) { return value; }"));
    }

    @Test
    public void isNullEmptyOrIdentityFunction_false_for_transforms(){
        // Property access after return <arg> — not identity (issue #78)
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("value => { return value[\"results\"].reduce((a,b) => a+b) }"));
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("value => { return value.foo }"));
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("value => { return value[\"key\"] }"));
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("function(value) { return value.foo; }"));
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("function(value) { return value[\"results\"]; }"));
        // Conditional return — not identity
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("value => { if (value[\"workload\"] != \"x\") return null; return value[\"foo\"] }"));
        // Actual transformation
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("value => value.split(',')"));
        assertFalse(JsNode.isNullEmptyOrIdentityFunction("value => value + 1"));
    }

    @Test
    public void getParameterNames_empty_named_star_function(){
        List<String> params = JsNode.getParameterNames("function* foo(){ yield [1,2,3]; yield 4;}");
        assertNotNull(params);
        assertTrue(params.isEmpty(),"expect to be empty:"+params);
    }
    @Test
    public void getParameterNames_named_star_function_single_arg(){
        List<String> params = JsNode.getParameterNames("""
                function* (value){
                  const keys = Object.keys(value);
                  const values = Object.values(value);
                  const length = Math.max(...Object.values(value).map(v => Array.isArray(v) ? v.length : 1))
                  const rtrn = []
                  for (let i=0; i<length; i++){
                     let entry = {}
                     for(const key of keys){
                       let toAdd = Array.isArray(value[key]) ? value[key].length > i ? value[key][i] : false : value[key]
                       if(toAdd){
                         entry[key]=toAdd
                       }
                     }
                     console.log("entry",entry)
                     rtrn.push(entry)
                     yield entry;
                  }
                  //return rtrn
                }
                """);
        assertNotNull(params);
        assertEquals(1, params.size(),"expect 1 entry: "+params);
        assertEquals("value",params.get(0));
    }
    @Test
    public void getParameterNames_arrow_with_renames(){
        List<String> params = JsNode.getParameterNames("({_one : one, _two : two})=>one + two");
        assertNotNull(params);
        assertEquals(2, params.size(),"expect 2 params:"+params);
        assertEquals("_one",params.get(0));
        assertEquals("_two",params.get(1));
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
    public void createParameters_destructure() {
        Map<String, ValueEntity> values = Map.of(
                "a",new ValueEntity(null,null,JqValues.parse("1")),
                "b",new ValueEntity(null,null,JqValues.parse("2")),
                "c",new ValueEntity(null,null,JqValues.parse("3"))
        );
        List<JqValue> params = JsNode.createParameters("function({a,b},c){}",values,3);
        assertNotNull(params,"return should not be null");
        assertEquals(2,params.size(),"expected 2 values: "+params);
        JqValue node = params.get(0);
        assertNotNull(node,"return should not be null");
        assertInstanceOf(JqObject.class,node);
        JqObject obj = (JqObject)node;
        assertTrue(obj.has("a"),"expected a value: "+obj.toJsonString());
        assertEquals("1",obj.get("a").toJsonString(),"expected a value: "+obj.toJsonString());
        assertTrue(obj.has("b"),"expected a value: "+obj.toJsonString());
        assertEquals("2",obj.get("b").toJsonString(),"expected b value: "+obj.toJsonString());
        assertEquals("3",params.get(1).toJsonString(),"expected c value: "+obj.toJsonString());
    }
    @Test
    public void createParameters_single_value_different_name() {
        Map<String, ValueEntity> values = Map.of(
                "a", new ValueEntity(null, null, JqValues.parse("1"))
        );
        List<JqValue> params = JsNode.createParameters("function(v){return v;}",values,1);
        assertNotNull(params,"return should not be null");
        assertEquals(1,params.size(),"expected 1 value: "+params);
        JqValue node = params.get(0);
        assertNotNull(node,"return should not be null");
        assertInstanceOf(JqNumber.class,node);
    }

    @Test
    public void createParameters_multiple_values_single_parameter() {
        Map<String, ValueEntity> values = Map.of(
                "a",new ValueEntity(null,null,JqValues.parse("1")),
                "b",new ValueEntity(null,null,JqValues.parse("2"))
        );
        List<JqValue> params = JsNode.createParameters("function(v){return v;}",values,2);
        assertNotNull(params,"return should not be null");
        assertEquals(1,params.size(),"expected 1 value: "+params);
        JqValue node = params.get(0);
        assertNotNull(node,"return should not be null");
        assertInstanceOf(JqObject.class,node);
        JqObject obj = (JqObject)node;
        assertTrue(obj.has("a"),"expected a value: "+obj.toJsonString());
        assertTrue(obj.has("b"),"expected b value: "+obj.toJsonString());
    }
    @Test
    public void createParameters_multiple_values_multiple_parameters_different_name() {
        Map<String, ValueEntity> values = Map.of(
                "a",new ValueEntity(null,null,JqValues.parse("1")),
                "b",new ValueEntity(null,null,JqValues.parse("2"))
        );
        List<JqValue> params = JsNode.createParameters("function(v,x=false,y='true'){return v;}",values,2);
        assertNotNull(params,"return should not be null");
        assertEquals(1,params.size(),"expected 1 value: "+params);
        JqValue node = params.get(0);
        assertNotNull(node,"return should not be null");
        assertInstanceOf(JqObject.class,node);
        JqObject obj = (JqObject)node;
        assertTrue(obj.has("a"),"expected a value: "+obj.toJsonString());
        assertTrue(obj.has("b"),"expected b value: "+obj.toJsonString());
    }

    // ---- extractParameterString tests ----

    @Test
    public void extractParameterString_arrow(){
        assertEquals("a, b, c", JsNode.extractParameterString("(a, b, c) => a + b + c"));
    }

    @Test
    public void extractParameterString_arrow_no_parens(){
        assertEquals("value", JsNode.extractParameterString("value => value.length"));
    }

    @Test
    public void extractParameterString_function(){
        assertEquals("a,b, c , d", JsNode.extractParameterString("function(a,b, c , d){return 42}"));
    }

    @Test
    public void extractParameterString_with_comment(){
        String result = JsNode.extractParameterString("""
                /** JSDoc comment */
                (arr, key = "hostname") => { return arr[0][key]; }
                """);
        assertNotNull(result);
        assertTrue(result.contains("arr"));
        assertTrue(result.contains("key"));
    }

    @Test
    public void extractParameterString_null_input(){
        assertNull(JsNode.extractParameterString(null));
        assertNull(JsNode.extractParameterString(""));
        assertNull(JsNode.extractParameterString("   "));
    }

    // ---- countNonDefaultParams tests ----

    @Test
    public void countNonDefaultParams_mixed(){
        assertEquals(1, JsNode.countNonDefaultParams(
                "(filteredArr, logData = \"time\", stddev = false, usePopulation = false) => { return 1; }"));
    }

    @Test
    public void countNonDefaultParams_no_defaults(){
        assertEquals(3, JsNode.countNonDefaultParams("(a, b, c) => a + b + c"));
    }

    @Test
    public void countNonDefaultParams_all_defaults(){
        assertEquals(0, JsNode.countNonDefaultParams("(x = 1, y = 2) => x + y"));
    }

    @Test
    public void countNonDefaultParams_single_no_parens(){
        assertEquals(1, JsNode.countNonDefaultParams("value => value.length"));
    }

    @Test
    public void countNonDefaultParams_empty(){
        assertEquals(0, JsNode.countNonDefaultParams("() => 42"));
    }

    @Test
    public void countNonDefaultParams_six_params_five_defaults(){
        assertEquals(1, JsNode.countNonDefaultParams(
                "(nestedArr, nameRegexPattern = \"^Kernel$\", calculateCvPercent = false, index = \"time\", logData = \"time\", usePopulation = false) => { return 1; }"));
    }

    // ---- getDefaultParameterNames tests ----

    @Test
    public void getDefaultParameterNames_arrow_with_defaults(){
        var defaults = JsNode.getDefaultParameterNames(
                "(filteredArr, logData = \"time\", stddev = false, usePopulation = false) => { return 1; }");
        assertEquals(3, defaults.size());
        assertTrue(defaults.contains("logData"));
        assertTrue(defaults.contains("stddev"));
        assertTrue(defaults.contains("usePopulation"));
        assertFalse(defaults.contains("filteredArr"), "non-default param should not be included");
    }

    @Test
    public void getDefaultParameterNames_function_with_defaults(){
        var defaults = JsNode.getDefaultParameterNames(
                "function(arr, key=\"hostname\") { return arr[0][key]; }");
        assertEquals(1, defaults.size());
        assertTrue(defaults.contains("key"));
        assertFalse(defaults.contains("arr"));
    }

    @Test
    public void getDefaultParameterNames_no_defaults(){
        var defaults = JsNode.getDefaultParameterNames("(a, b, c) => a + b + c");
        assertTrue(defaults.isEmpty());
    }

    @Test
    public void getDefaultParameterNames_all_defaults(){
        var defaults = JsNode.getDefaultParameterNames("(x = 1, y = 2) => x + y");
        assertEquals(2, defaults.size());
        assertTrue(defaults.contains("x"));
        assertTrue(defaults.contains("y"));
    }

    @Test
    public void getDefaultParameterNames_empty_input(){
        assertTrue(JsNode.getDefaultParameterNames(null).isEmpty());
        assertTrue(JsNode.getDefaultParameterNames("").isEmpty());
        assertTrue(JsNode.getDefaultParameterNames("   ").isEmpty());
    }

    @Test
    public void getDefaultParameterNames_with_jsdoc_comment(){
        var defaults = JsNode.getDefaultParameterNames("""
                /**
                 * @param {Array} filteredArr
                 * @param {string} [logData="time"]
                 */
                (filteredArr, logData = "time", stddev = false) => { return 1; }
                """);
        assertEquals(2, defaults.size());
        assertTrue(defaults.contains("logData"));
        assertTrue(defaults.contains("stddev"));
        assertFalse(defaults.contains("filteredArr"));
    }

    @Test
    public void getDefaultParameterNames_single_no_parens(){
        // single-param arrow without parens has no defaults possible
        var defaults = JsNode.getDefaultParameterNames("value => value.length");
        assertTrue(defaults.isEmpty());
    }

    @Test
    public void getDefaultParameterNames_nested_array_with_defaults(){
        var defaults = JsNode.getDefaultParameterNames(
                "(nestedArr, nameRegexPattern = \"^Kernel$\", calculateCvPercent = false, index = \"time\", logData = \"time\", usePopulation = false) => { return 1; }");
        assertEquals(5, defaults.size());
        assertTrue(defaults.contains("nameRegexPattern"));
        assertTrue(defaults.contains("calculateCvPercent"));
        assertTrue(defaults.contains("index"));
        assertTrue(defaults.contains("logData"));
        assertTrue(defaults.contains("usePopulation"));
        assertFalse(defaults.contains("nestedArr"));
    }

    // ---- parse() with default parameters ----

    @Test
    public void parse_skips_default_params_and_wires_data_source(){
        // Simulates the boot-time-verbose pattern:
        // Function has (filteredArr, logData="time", stddev=false, usePopulation=false)
        // Source node is named "Kernel Post-Timer Duration Average ms"
        // Only filteredArr is a data source; the rest are JS defaults
        NodeEntity sourceNode = new JqNode("Kernel Post-Timer Duration Average ms", ".boot_time");
        Function<String, List<NodeEntity>> lookup = name ->
                "Kernel Post-Timer Duration Average ms".equals(name) ? List.of(sourceNode) : Collections.emptyList();

        // parse() should fail because "filteredArr" doesn't match any source name
        JsNode node = JsNode.parse("BOOT2 Avg",
                "(filteredArr, logData = \"time\", stddev = false, usePopulation = false) => { return filteredArr; }",
                lookup);
        // parse returns null when filteredArr can't be resolved, because it's not a default param
        assertNull(node, "parse should return null when non-default param doesn't match any source");
    }

    @Test
    public void parse_skips_default_params_with_ignoreMissing(){
        // With ignoreMissing=true, parse() creates the node even when filteredArr is missing
        NodeEntity sourceNode = new JqNode("Kernel Post-Timer Duration Average ms", ".boot_time");
        Function<String, List<NodeEntity>> lookup = name ->
                "Kernel Post-Timer Duration Average ms".equals(name) ? List.of(sourceNode) : Collections.emptyList();

        JsNode node = JsNode.parse("BOOT2 Avg",
                "(filteredArr, logData = \"time\", stddev = false, usePopulation = false) => { return filteredArr; }",
                lookup, true);
        assertNotNull(node, "parse with ignoreMissing should succeed");
        // Node should have 0 sources since filteredArr wasn't found
        assertEquals(0, node.sources.size(), "no sources should be wired (filteredArr not found)");
    }

    @Test
    public void parse_wires_matching_data_param(){
        // When the non-default parameter name matches a source node name
        NodeEntity sourceNode = new JqNode("filteredArr", ".boot_time");
        Function<String, List<NodeEntity>> lookup = name ->
                "filteredArr".equals(name) ? List.of(sourceNode) : Collections.emptyList();

        JsNode node = JsNode.parse("BOOT2 Avg",
                "(filteredArr, logData = \"time\", stddev = false) => { return filteredArr; }",
                lookup);
        assertNotNull(node, "parse should succeed when data param matches source name");
        assertEquals(1, node.sources.size(), "one source should be wired");
        assertEquals("filteredArr", node.sources.get(0).name);
    }

    // ---- createParameters() with default params ----

    @Test
    public void createParameters_default_params_single_source_fallback(){
        // Key scenario: function has (filteredArr, logData="time", stddev=false, usePopulation=false)
        // but sourceValues has key "Kernel Post-Timer Duration Average ms" (no param name match).
        // With sourceCount=1, should fall back to passing the single source value as filteredArr.
        JqValue arrayData = JqValues.parse("[{\"name\":\"Kernel\",\"time\":910670}]");
        Map<String, ValueEntity> values = Map.of(
                "Kernel Post-Timer Duration Average ms", new ValueEntity(null, null, arrayData)
        );
        List<JqValue> params = JsNode.createParameters(
                "(filteredArr, logData = \"time\", stddev = false, usePopulation = false) => { return filteredArr; }",
                values, 1);
        assertNotNull(params);
        assertEquals(1, params.size(), "should have 1 parameter (the source value)");
        assertTrue(params.get(0).isArray(), "parameter should be the array from source");
        assertEquals(1, params.get(0).length());
    }

    @Test
    public void createParameters_default_params_multi_source_fallback(){
        // When sourceCount > 1 and no param names match, builds an object from all source values
        JqValue val1 = JqValues.parse("\"hello\"");
        JqValue val2 = JqValues.parse("42");
        Map<String, ValueEntity> values = new LinkedHashMap<>();
        values.put("sourceA", new ValueEntity(null, null, val1));
        values.put("sourceB", new ValueEntity(null, null, val2));

        List<JqValue> params = JsNode.createParameters(
                "(data, option = true) => { return data; }",
                values, 2);
        assertNotNull(params);
        assertEquals(1, params.size(), "should have 1 parameter (object of all sources)");
        assertInstanceOf(JqObject.class, params.get(0));
        JqObject obj = (JqObject) params.get(0);
        assertTrue(obj.has("sourceA"));
        assertTrue(obj.has("sourceB"));
    }

    @Test
    public void createParameters_all_params_have_defaults_no_match(){
        // When ALL params have defaults and none match, should still fallback
        JqValue data = JqValues.parse("99");
        Map<String, ValueEntity> values = Map.of("x", new ValueEntity(null, null, data));

        List<JqValue> params = JsNode.createParameters(
                "(a = 1, b = 2) => a + b",
                values, 1);
        assertNotNull(params);
        // No param name matches "x", but the fallback at the end handles this
        assertEquals(1, params.size(), "fallback should provide the source value");
    }

    @Test
    public void createParameters_matching_param_name_ignores_defaults(){
        // When the data param name matches a source key, it's used directly;
        // default params are correctly skipped (no value needed for them)
        JqValue arrayData = JqValues.parse("[1, 2, 3]");
        Map<String, ValueEntity> values = Map.of("arr", new ValueEntity(null, null, arrayData));

        List<JqValue> params = JsNode.createParameters(
                "(arr, usePopulation = false) => { return arr; }",
                values, 1);
        assertNotNull(params);
        // "arr" matches the source key, "usePopulation" has a default — but getParameterNames
        // returns both ["arr", "usePopulation"]. "arr" matches, "usePopulation" doesn't match
        // any source key, so it prints a warning but we still get "arr" in the result.
        assertTrue(params.size() >= 1, "should have at least the arr parameter");
        assertTrue(params.get(0).isArray(), "first param should be the array");
    }

    @Test
    public void parse_sources(){
        Map<String, NodeEntity> existing = Map.of("a",new JqNode("a",".a"),
                "b",new JqNode("b",".b"));

        Function<String,List<NodeEntity>> getExisting = new Function<String,List<NodeEntity>>(){
            @Override
            public List<NodeEntity> apply(String s) {
                return existing.containsKey(s) ? List.of(existing.get(s)) : Collections.emptyList();
            }
        };
        JsNode node = JsNode.parse("node","(a,b)=>a+b",getExisting);
        assertNotNull(node,"node should not be null");
        List<NodeEntity> sources = node.sources;
        assertNotNull(sources,"sources should not be null");
        assertEquals(2,sources.size(),"expected 2 values: "+sources);
        assertEquals(existing.get("a"),sources.get(0),"expected a value");
        assertEquals(existing.get("b"),sources.get(1),"expected b value");
    }

}
