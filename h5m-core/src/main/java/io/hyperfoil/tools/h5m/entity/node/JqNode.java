package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.Node;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Entity
@DiscriminatorValue("jq")
public class JqNode extends Node {
    public static String SOURCE_PREFIX="{";
    public static String SOURCE_SUFFIX="}:";
    public static String SOURCE_SEPARATOR=",";

    //{ sourceFqdn, ...}:...
    public static JqNode parse(String name, String input, Function<String,List<Node>> nodeFn){
        if(input==null || input.isBlank()){
            System.err.println("missing jq node input");
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
        JqNode rtrn = new JqNode(name,input,sources);
        return rtrn;
    }

    public JqNode(){
        super();
        this.type="jq";//setting type because detached entities might not have this field
    }
    public JqNode(String name){
        super(name);
        this.type = "jq";
    }
    public JqNode(String name,String operation){
        super(name,operation);
        this.type = "jq";
    }
    public JqNode(String name,String operation,List<Node> sources){
        super(name,operation,sources);
        this.type = "jq";
    }
    public JqNode(String name,String operation,Node...sources){
        super(name,operation,List.of(sources));
        this.type = "jq";
    }


    public static String getCwd(){
        Path currentRelativePath = Paths.get("");
        return currentRelativePath.toAbsolutePath().toString();
    }
    private static Path ensurePath(String path){
        Path basePath = Paths.get(getCwd(),path);
        if( !Files.exists(basePath) ){
            basePath.toFile().mkdirs();
        }
        return basePath;
    }
    //detect if the jq command is processing the inputs together
    public static boolean isNullInput(String command){
        return command.matches("(?<!\\.)[^.]*inputs.*");//^(?<!\.)inputs
    }

    @Override
    protected Node shallowCopy() {
        return new JqNode(name,operation);
    }

    @Override
    public String getOperationEncoding() {
        StringBuilder sb = new StringBuilder();
        if(hasNonRootSource()){
            sb.append(SOURCE_PREFIX);
            sb.append(sources.stream().map(Node::getFqdn).collect(Collectors.joining(SOURCE_SEPARATOR+" ")));
            sb.append(SOURCE_SUFFIX);
        }
        sb.append(operation);
        return sb.toString();
    }

}
