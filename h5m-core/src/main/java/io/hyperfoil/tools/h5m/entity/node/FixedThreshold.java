package io.hyperfoil.tools.h5m.entity.node;

import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.yaup.json.Json;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.PostLoad;
import jakarta.persistence.Transient;

import java.util.ArrayList;
import java.util.List;

@Entity
@DiscriminatorValue("ft")
public class FixedThreshold extends Node {

    private static final String VALUE = "value";
    private static final int DEFAULT_VALUE = 0;
    private static final String ENABLED = "enabled";
    private static final Boolean DEFAULT_ENABLED = false;
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String INCLUSIVE = "inclusive";
    private static final Boolean DEFAULT_INCLUSIVE = true;
    private static final String EMPTY_OPERATION = """
            { "max": { "enabled":false, "inclusive":true, "value": 0},
              "min": { "enabled":false, "inclusive":true, "value": 0},
            }
            """;
    @Transient
    private Json config;
    public FixedThreshold(){
        config = Json.fromString(EMPTY_OPERATION);
    }
    public FixedThreshold(String name,String operation){
        super(name,operation);
        config = Json.fromString(operation);
    }
    @PostLoad
    public void loadConfig(){
        if(this.operation!=null && !this.operation.isBlank()){
            config = Json.fromString(this.operation);
        }else {
            config = new Json();
            //TODO load default values?
        }
    }
    public void setNodes(Node fingerprint,Node groupBy,Node range,Node domain){
        List<Node> sources = new ArrayList<>();
        sources.add(fingerprint);
        sources.add(groupBy);
        sources.add(range);
        if(domain!=null){
            sources.add(domain);
        }
        this.sources = sources;
    }
    @Transient
    public Node getRangeNode(){
        return sources.get(2);
    }
    //domain node can be null
    @Transient
    public Node getDomainNode(){
        return sources.size() > 3 ? sources.get(3) : null;
    }
    @Transient
    public Node getGroupByNode(){return sources.get(1);}
    @Transient
    public Node getFingerprintNode(){
        return sources.get(0);
    }
    @Transient
    public List<Node> getFingerprintNodes(){
        return sources.get(0).sources;
    }
    @Override
    protected Node shallowCopy() {
        return new FixedThreshold(name,operation);
    }
    public void setMaxEnabled(boolean enabled){
        Json.chainSet(config,MAX+"."+ENABLED,enabled);
        operation=config.toString();
    }
    @Transient
    public boolean isMaxEnabled(){
        return (Boolean)Json.find(config,MAX+"."+ENABLED,DEFAULT_ENABLED);
    }
    public void setMinEnabled(boolean enabled){
        Json.chainSet(config,MIN+"."+ENABLED,enabled);
        operation=config.toString();
    }
    @Transient
    public boolean isMinEnabled(){
        return (Boolean)Json.find(config,MIN+"."+ENABLED,DEFAULT_ENABLED);
    }

    public void setMaxInclusive(boolean inclusive){
        Json.chainSet(config,MAX+"."+INCLUSIVE,inclusive);
        operation=config.toString();
    }
    @Transient
    public boolean isMaxInclusive(){
        return (Boolean)Json.find(config,MAX+"."+INCLUSIVE,DEFAULT_INCLUSIVE);
    }

    public void setMinInclusive(boolean inclusive){
        Json.chainSet(config,MIN+"."+INCLUSIVE,inclusive);
        operation=config.toString();
    }
    @Transient
    public boolean isMinInclusive(){
        return (Boolean)Json.find(config,MIN+"."+INCLUSIVE,DEFAULT_INCLUSIVE);
    }

    public void setMaxValue(int value){
        Json.chainSet(config,MAX+"."+VALUE,value);
        operation=config.toString();
    }
    @Transient
    public int getMaxValue(){
        return (Integer)Json.find(config,MAX+"."+VALUE,DEFAULT_VALUE);
    }

    public void setMinValue(int value){
        Json.chainSet(config,MIN+"."+VALUE,value);
        operation=config.toString();
    }
    @Transient
    public int getMinValue(){
        return (Integer)Json.find(config,MIN+"."+VALUE,DEFAULT_VALUE);
    }

}
