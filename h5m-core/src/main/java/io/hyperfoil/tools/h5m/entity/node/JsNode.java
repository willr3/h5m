package io.hyperfoil.tools.h5m.entity.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.Value;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

@Entity
@DiscriminatorValue("ecma")
public class JsNode extends Node {

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
    public static JsNode parse(String name, String input, Function<String,List<Node>> nodeFn){
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
        boolean ok = true;
        List<Node> sourceNodes = new ArrayList<>();
        for (String param : parameters) {
            List<Node> foundNodes = nodeFn.apply(param);
            if (foundNodes.isEmpty()) {
                System.err.println("Could not find node for " + param);
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
        if(ok) {
            rtrn = new JsNode(name, input, sourceNodes);
        }
        return rtrn;
    }

    public static List<JsonNode> createParameters(String function, Map<String,Value> sourceValues){
        List<String> params = JsNode.getParameterNames(function,false);
        List<JsonNode> rtrn = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode currentNode = null;
        for(String param : params){
            if(param.startsWith("{") && param.endsWith("}")){//sneaky but we don't care
                param = param.substring(1,param.length()-1).trim();
            }
            if(param.startsWith("{")){
                if(currentNode != null){
                    System.err.println("nesting destructured parameters is not supported\n"+function);
                }
                currentNode =  mapper.createObjectNode();
                rtrn.add(currentNode);
                param = param.substring(1).trim();
                if(sourceValues.containsKey(param)){
                    currentNode.set(param,sourceValues.get(param).data);
                }else{
                    System.err.println("unable to find parameter value for " + param);
                }
            }else if(param.endsWith("}")){
                if(currentNode == null){
                    System.err.println("closing a nested destructured parameters is not supported\n"+function);
                }else{
                    param = param.substring(0,param.length()-1).trim();
                    if(!sourceValues.containsKey(param)){
                        System.err.println("unable to find parameter value for " + param);
                    }else{
                        currentNode.set(param,sourceValues.get(param).data);
                    }
                    currentNode = null;
                }
            }else{
                if(!sourceValues.containsKey(param)){
                    if(params.size()==1 && sourceValues.size()==1){
                        rtrn.add(sourceValues.get(sourceValues.keySet().iterator().next()).data);
                    }else {
                        System.err.println("unable to find parameter value for " + param);
                    }
                }else{
                    if(currentNode != null){
                        currentNode.set(param,sourceValues.get(param).data);
                    }else{
                        rtrn.add(sourceValues.get(param).data);
                    }
                }
            }
        }
        //if no parameter names match but there are input values and params
        if(rtrn.isEmpty() && !sourceValues.isEmpty() && !params.isEmpty()){
            if(sourceValues.size()==1){
                rtrn.add(sourceValues.get(sourceValues.keySet().iterator().next()).data);
            }else{
                ObjectNode node = mapper.createObjectNode();
                sourceValues.forEach((k,v)->{node.set(k,v.data);});
                rtrn.add(node);
            }
        }
        return rtrn;
    }

    public static boolean isNullEmptyOrIdentityFunction(String input){
        return
            input==null ||
            input.isEmpty() ||
            input.matches("\\s*\\(?\\s*(?<arg>[a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\)?\\s*=>\\s*\\k<arg>\\s*") ||
            input.matches("\\s*\\(?\\s*(?<arg>[a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\)?\\s*=>.*?return\\s+\\k<arg>.*") ||
            input.matches("\\s*function\\*?\\s*\\w*\\s*\\(\\s*(?<arg>[a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\)\\s*\\{.*?return\\s+\\k<arg>.*")
            ;
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
        if(input==null || input.isBlank()){
            return null;
        }
        List<String> rtrn = null;
        String parameters = null;
        //remove leading comments
        int length = input.length();
        do {
            length = input.length();
            if(input.trim().startsWith("//")){
                input = input.substring(input.indexOf(System.lineSeparator()+1));
            }
            if(input.trim().startsWith("/*")){
                input = input.substring(input.indexOf("*/")+2);
            }
        }while(input.length() < length);
        if(input.startsWith("function(")) {
            parameters = input.substring("function(".length(), input.indexOf(")")).trim();
        }else if (input.startsWith("function*")) {
            parameters = input.substring("function*".length()).trim();
            if(parameters.matches("(?s)^[a-zA-Z_$][a-zA-Z0-9_$]*\\([^)]*\\)\\s*\\{.*")){
                parameters = parameters.substring(parameters.indexOf("(")+1, parameters.indexOf(")"));
            }
        }else if (input.contains("=>")) {
            parameters = input.substring(0, input.indexOf("=>")).trim();
            if (parameters.startsWith("(") && parameters.endsWith(")")) {
                parameters = parameters.substring(1, parameters.length() - 1);
            }
        }
        String filter = removeSpread ? "\\.\\.\\.|\\{|}" : "\"\\\\.\\\\.\\\\.";
        if(parameters != null){
            rtrn = Stream.of(parameters.split(","))
                .map(s -> {
                            s = s.trim()
                            .replaceAll(filter, "")
                            .trim();
                            if(s.contains("=")){
                                s = s.substring(0,s.indexOf("=")).trim();
                            }
                            return s;
                        }
                )
                .filter(s -> !s.isBlank())
                .toList();
        }
        return rtrn;
    }

    public JsNode(){
        super();
        this.type="ecma";
    }
    public JsNode(String name,String operation){
        super(name,operation);
        this.type="ecma";
    }
    public JsNode(String name,String operation,List<Node> sources){
        super(name,operation,sources);
        this.type="ecma";
    }

    @Override
    protected Node shallowCopy() {
        return new JsNode(name,operation);
    }
}
