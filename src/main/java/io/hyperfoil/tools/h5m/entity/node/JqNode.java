package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import org.hibernate.annotations.SQLInsert;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Entity
@DiscriminatorValue("jq")
public class JqNode extends NodeEntity {
    public static String SOURCE_PREFIX="{";
    public static String SOURCE_SUFFIX="}:";
    public static String SOURCE_SEPARATOR=",";

    //{ sourceFqdn, ...}:...
    public static JqNode parse(String name, String input, Function<String,List<NodeEntity>> nodeFn){
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
        List<NodeEntity> sources = new ArrayList<>();
        boolean ok = true;
        if(!prefix.isBlank()) {
            for (String s : prefix.split(SOURCE_SEPARATOR)) {
                List<NodeEntity> found = nodeFn.apply(s);
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
    }
    public JqNode(String name){
        super(name);
    }
    public JqNode(String name,String operation){
        super(name,operation);
    }
    public JqNode(String name,String operation,List<NodeEntity> sources){
        super(name,operation,sources);
    }
    public JqNode(String name,String operation, NodeEntity...sources){
        super(name,operation,List.of(sources));
    }

    @Override
    public NodeType type() {
        return NodeType.JQ;
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
        // Match the jq builtin `inputs` as a standalone word, but not `.inputs` (field access)
        return command.matches(".*(?<!\\.)\\binputs\\b.*");
    }

    @Override
    protected NodeEntity shallowCopy() {
        return new JqNode(name,operation);
    }

    @Override
    public String getOperationEncoding() {
        StringBuilder sb = new StringBuilder();
        if(hasNonRootSource()){
            sb.append(SOURCE_PREFIX);
            sb.append(sources.stream().map(NodeEntity::getFqdn).collect(Collectors.joining(SOURCE_SEPARATOR + " ")));
            sb.append(SOURCE_SUFFIX);
        }
        sb.append(operation);
        return sb.toString();
    }

}
