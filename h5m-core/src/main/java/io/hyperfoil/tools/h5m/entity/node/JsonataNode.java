package io.hyperfoil.tools.h5m.entity.node;

import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import io.hyperfoil.tools.h5m.entity.Node;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Entity
@DiscriminatorValue("nata")
public class JsonataNode extends Node {


    public JsonataNode() {}
    public JsonataNode(String name, String operation, List<Node> sources) {
        super(name, operation, sources);
    }

    //this is a copy of exp.entity.node.JqNode.parse to have different error messages
    //TODO merge with exp.entity.node.JqNode.parse
    public static JsonataNode parse(String name, String input, Function<String,List<Node>> nodeFn){
        if(input == null || input.isEmpty()){
            System.err.println("missing jsonata input");
            return null;
        }
        String prefix = "";
        if(input.startsWith(JqNode.SOURCE_PREFIX)){
            if(!input.contains(JqNode.SOURCE_SUFFIX)){
                System.err.println("jsonata node starts with "+ JqNode.SOURCE_PREFIX+" but is missing "+ JqNode.SOURCE_SUFFIX);
                return null;//invalid source
            }
            prefix =  input.substring(JqNode.SOURCE_PREFIX.length(),input.indexOf(JqNode.SOURCE_SUFFIX));
            input = input.substring(input.indexOf(JqNode.SOURCE_SUFFIX)+ JqNode.SOURCE_SUFFIX.length());

        }
        List<Node> sources = new ArrayList<>();
        boolean ok = true;
        if(!prefix.isBlank()) {
            for (String s : prefix.split(JqNode.SOURCE_SEPARATOR)) {
                List<Node> found = nodeFn.apply(s);
                if (found.isEmpty()) {
                    System.err.println("failed to find source node " + s);
                    ok = false;
                } else if (found.size() > 1) {
                    System.err.println("found more than one source node " + s);
                    ok = false;
                } else {
                    sources.add(found.get(0));
                }
            }
        }
        try{
            Expressions expr = Expressions.parse(input);
        } catch (ParseException|IOException e) {
            System.err.println("cannot create jsonata from operation\n"+input+"\n"+e.getLocalizedMessage());
            ok = false;
        }
        if(!ok){
            return null;
        }
        JsonataNode rtrn = new JsonataNode(name,input,sources);
        return rtrn;
    }


    @Override
    protected Node shallowCopy() {
        return new JsonataNode(name, operation, sources);
    }
}
