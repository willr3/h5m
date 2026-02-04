package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.Node;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.hyperfoil.tools.h5m.entity.node.JqNode.*;

@Entity
@DiscriminatorValue("sqlall")
public class SqlJsonpathAllNode extends Node {

    //another copy of exp.entity.node.JqNode.parse with different error messages (and no splitting sources)
    //TODO create a more abstract exp.entity.node.JqNode.parse for different error messages and source limits
    public static SqlJsonpathAllNode parse(String name, String input, Function<String, List<Node>> nodeFn){
        if(input==null || input.isBlank()){
            System.err.println("missing sqlpathall node input");
            return null;
        }
        String prefix = "";
        if(input.startsWith(SOURCE_PREFIX)){
            if(!input.contains(SOURCE_SUFFIX)){
                System.err.println("jq node starts with "+SOURCE_PREFIX+" but is missing "+SOURCE_SUFFIX);
                return null;//invalid source
            }
            prefix =  input.substring(SOURCE_PREFIX.length(),input.indexOf(SOURCE_SUFFIX));
            input = input.substring(input.indexOf(SOURCE_SUFFIX)+SOURCE_SUFFIX.length());
        }
        List<Node> sources = new ArrayList<>();
        boolean ok = true;
        if(!prefix.isBlank()) {
            for (String s : prefix.split(SOURCE_SEPARATOR)) {
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
        if(!ok){
            return null;
        }
        if(sources.size() > 1){
            System.err.println("sqlpathall nodes can only have one source");
            return null;
        }
        return new SqlJsonpathAllNode(name,input,sources);

    }

    public SqlJsonpathAllNode(){
        super();
        this.type = "sqlall";
    }
    public SqlJsonpathAllNode(String name, String operation, List<Node> sources){
        super(name,operation,sources);
        this.type = "sqlall";
    }

    @Override
    protected Node shallowCopy() {
        return new SqlJsonpathAllNode(this.name, this.operation,this.sources);
    }

}
