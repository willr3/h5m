package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.jjq.value.JqNull;
import io.hyperfoil.tools.jjq.value.JqObject;
import io.hyperfoil.tools.jjq.value.JqValue;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

@Entity
@DiscriminatorValue("ecma")
public class JsNode extends NodeEntity {

    /*
     * function(a,b,c){}
     * (a,b,c)=>{}
     * a=>{}
     * a=>
     *
     *
     * this approach is matching a,b,c by node.fqdn (which includes name). This will work when created but changes to
     * source node names would stop working if this node is edited. The name a,b,c must exist whenever this node calculates
     * its sources.
     */
    public static JsNode parse(String name, String input, Function<String,List<NodeEntity>> nodeFn){
        return parse(name,input,nodeFn,false);
    }
    public static JsNode parse(String name, String input, Function<String,List<NodeEntity>> nodeFn,boolean ignoreMissing){
        if(input==null || input.isBlank()){
            System.err.println("missing js node input");
            return null;
        }
        JsNode rtrn = null;
        List<String> parameters = getParameterNames(input);
        if(parameters == null){
            System.err.println("unable to recognize javascript function from:\n"+input);
            return null;
        }
        // Identify which parameters have default values in the function signature.
        // Parameters with defaults (e.g., usePopulation = false) are JS-level config,
        // not data sources from the pipeline — missing lookups for these should not
        // cause the parse to fail.
        Set<String> defaultParams = getDefaultParameterNames(input);
        boolean ok = true;
        List<NodeEntity> sourceNodes = new ArrayList<>();
        for (String param : parameters) {
            List<NodeEntity> foundNodes = nodeFn.apply(param);
            if (foundNodes.isEmpty()) {
                if (defaultParams.contains(param)) {
                    // Parameter has a default value — not a data source, skip silently
                    continue;
                }
                ok = false;
            } else if (foundNodes.size() > 1) {
                System.err.println("Found more than one node matching " + param);
                for (int i = 0; i < foundNodes.size(); i++) {
                    System.err.println(i + " " + foundNodes.get(i).getFqdn());
                }
                ok = false;
            } else {
                sourceNodes.add(foundNodes.get(0));
            }
        }
        if(ok || ignoreMissing) {
            rtrn = new JsNode(name, input, sourceNodes);
        }
        return rtrn;
    }

    /**
     * Returns the number of parameters that do NOT have default values.
     * For example, {@code (filteredArr, logData = "time", stddev = false)} returns 1.
     * These are the actual data source parameters — parameters with defaults are
     * JS-level config.
     */
    public static int countNonDefaultParams(String input) {
        List<String> params = getParameterNames(input);
        if (params == null || params.isEmpty()) return 0;
        Set<String> defaults = getDefaultParameterNames(input);
        return (int) params.stream().filter(p -> !defaults.contains(p)).count();
    }

    public static List<JqValue> createParameters(String function, Map<String, ValueEntity> sourceValues,int sourceCount){
        List<String> params = JsNode.getParameterNames(function,false);
        List<JqValue> rtrn = new ArrayList<>();
        JqObject.Builder currentBuilder = null;
        int currentBuilderIndex = -1;
        for(String param : params){
            if(param.startsWith("{") && param.endsWith("}")){//sneaky but we don't care
                param = param.substring(1,param.length()-1).trim();
            }
            if(param.startsWith("{")){
                if(currentBuilder != null){
                    System.err.println("nesting destructured parameters is not supported\n"+function);
                }
                currentBuilder = JqObject.builder();
                rtrn.add(null); // placeholder — will be replaced when builder completes
                currentBuilderIndex = rtrn.size() - 1;
                param = param.substring(1).trim();
                if(sourceValues.containsKey(param)){
                    JqValue data = sourceValues.get(param).data;
                    currentBuilder.put(param, data != null ? data : JqNull.NULL);
                }else{
                    System.err.println("unable to find parameter value for " + param);
                }
            }else if(param.endsWith("}")){
                if(currentBuilder == null){
                    System.err.println("closing a nested destructured parameters is not supported\n"+function);
                }else{
                    param = param.substring(0,param.length()-1).trim();
                    if(!sourceValues.containsKey(param)){
                        System.err.println("unable to find parameter value for " + param);
                    }else{
                        JqValue data = sourceValues.get(param).data;
                        currentBuilder.put(param, data != null ? data : JqNull.NULL);
                    }
                    rtrn.set(currentBuilderIndex, currentBuilder.build());
                    currentBuilder = null;
                    currentBuilderIndex = -1;
                }
            }else{
                if(!sourceValues.containsKey(param)){
                    if(params.size() ==1 && !sourceValues.isEmpty()){
                        if(sourceCount == 1){
                            JqValue data = sourceValues.values().iterator().next().data;
                            rtrn.add(data != null ? data : JqNull.NULL);
                        }else{
                            JqObject.Builder obj = JqObject.builder();
                            sourceValues.forEach((k,v) -> {
                                JqValue d = v.data;
                                obj.put(k, d != null ? d : JqNull.NULL);
                            });
                            rtrn.add(obj.build());
                        }
                    }else{
                        System.err.println("unable to find parameter value for " + param);
                    }
                }else{
                    JqValue data = sourceValues.get(param).data;
                    if(currentBuilder != null){
                        currentBuilder.put(param, data != null ? data : JqNull.NULL);
                    }else{
                        rtrn.add(data != null ? data : JqNull.NULL);
                    }
                }
            }
        }
        //if no parameter names match but there are input values and params
        if(rtrn.isEmpty() && !sourceValues.isEmpty() && !params.isEmpty()){
            if(sourceValues.size()==1){
                JqValue data = sourceValues.get(sourceValues.keySet().iterator().next()).data;
                rtrn.add(data != null ? data : JqNull.NULL);
            }else{
                JqObject.Builder builder = JqObject.builder();
                sourceValues.forEach((k,v) -> {
                    JqValue d = v.data;
                    builder.put(k, d != null ? d : JqNull.NULL);
                });
                rtrn.add(builder.build());
            }
        }
        return rtrn;
    }

    public static boolean isNullEmptyOrIdentityFunction(String input){
        return
            input==null ||
            input.isEmpty() ||
            // arrow expression: value => value
            input.matches("\\s*\\(?\\s*(?<arg>[a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\)?\\s*=>\\s*\\k<arg>\\s*") ||
            // arrow block: value => { return value; } — require return <arg> followed by ; or }
            // to avoid false matches like: value => { return value["results"].reduce(...) }
            input.matches("\\s*\\(?\\s*(?<arg>[a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\)?\\s*=>.*?return\\s+\\k<arg>\\s*[;}].*") ||
            // traditional function: function(value) { return value; }
            input.matches("\\s*function\\*?\\s*\\w*\\s*\\(\\s*(?<arg>[a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\)\\s*\\{.*?return\\s+\\k<arg>\\s*[;}].*")
            ;
    }

    /**
     * Strips leading comments and extracts the raw parameter string from a JS function.
     * Returns the comma-separated parameter list (without surrounding parens), or null
     * if the input is not a recognizable function.
     *
     * <p>Shared by {@link #getParameterNames} and {@link #getDefaultParameterNames}
     * to avoid duplicating comment-stripping and parameter-extraction logic.</p>
     */
    static String extractParameterString(String input) {
        if (input == null || input.isBlank()) return null;
        // Strip leading comments
        int length;
        do {
            length = input.length();
            if (input.trim().startsWith("//")) {
                if (input.contains(System.lineSeparator())) {
                    input = input.substring(input.indexOf(System.lineSeparator()) + 1);
                }
            }
            if (input.trim().startsWith("/*")) {
                input = input.substring(input.indexOf("*/") + 2);
            }
        } while (input.length() < length);
        input = input.trim();
        // Extract parameter list based on function syntax
        String parameters = null;
        if (input.startsWith("function(")) {
            parameters = input.substring("function(".length(), input.indexOf(")")).trim();
        } else if (input.startsWith("function*")) {
            parameters = input.substring("function*".length()).trim();
            if (parameters.matches("(?s)^(?:[a-zA-Z_$][a-zA-Z0-9_$]*)?\\([^)]*\\)\\s*\\{.*")) {
                parameters = parameters.substring(parameters.indexOf("(") + 1, parameters.indexOf(")"));
            }
        } else if (input.contains("=>")) {
            parameters = input.substring(0, input.indexOf("=>")).trim();
            if (parameters.startsWith("(") && parameters.endsWith(")")) {
                parameters = parameters.substring(1, parameters.length() - 1);
            }
        }
        return parameters;
    }

    /**
     * Returns the set of parameter names that have default values in the function
     * signature. For example, {@code (arr, usePopulation = false)} returns
     * {@code {"usePopulation"}}. These parameters are JS-level config, not data
     * sources from the pipeline.
     */
    public static Set<String> getDefaultParameterNames(String input) {
        String parameters = extractParameterString(input);
        if (parameters == null) return Set.of();
        Set<String> defaults = new HashSet<>();
        for (String p : parameters.split(",")) {
            p = p.trim();
            if (p.contains("=")) {
                String name = p.substring(0, p.indexOf("=")).trim()
                        .replaceAll("\\.\\.\\.|\\{|}", "").trim();
                if (!name.isEmpty()) {
                    defaults.add(name);
                }
            }
        }
        return defaults;
    }

    /**
     * Returns the list of identified parameters
     * @param input
     * @return the list of parameters or null iff the input fails to parse
     */
    public static List<String> getParameterNames(String input){
        return getParameterNames(input,true);
    }
    public static List<String> getParameterNames(String input, boolean removeSpread){
        String parameters = extractParameterString(input);
        if (parameters == null) return null;
        String filter = removeSpread ? "\\.\\.\\.|\\{|}" : "\"\\\\.\\\\.\\\\.";
        return Stream.of(parameters.split(","))
                .map(s -> {
                            s = s.trim()
                            .replaceAll(filter, "")
                            .trim();
                            if(s.contains("=")){
                                s = s.substring(0,s.indexOf("=")).trim();
                            }
                            if(s.contains(":")){
                                s = s.substring(0,s.indexOf(":")).trim();
                            }
                            return s;
                        }
                )
                .filter(s -> !s.isBlank())
                .toList();
    }

    public JsNode(){
        super();
    }
    public JsNode(String name,String operation){
        super(name,operation);
    }
    public JsNode(String name,String operation,List<NodeEntity> sources){
        super(name,operation,sources);
    }

    @Override
    public NodeType type() {
        return NodeType.JS;
    }

    @Override
    protected NodeEntity shallowCopy() {
        return new JsNode(name,operation);
    }
}
