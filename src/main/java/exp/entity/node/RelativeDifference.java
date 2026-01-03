package exp.entity.node;

import exp.entity.Node;
import exp.entity.Value;
import io.hyperfoil.tools.yaup.json.Json;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.PostLoad;
import jakarta.persistence.Transient;
import jakarta.validation.constraints.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Entity
@DiscriminatorValue("rd")
public class RelativeDifference extends Node {

    private static final String THRESHOLD = "threshold";
    public static final double DEFAULT_THRESHOLD = 0.2;
    private static final String WINDOW = "window";
    public static final int DEFAULT_WINDOW = 1;
    private static final String MIN_PREVIOUS = "minPrevious";
    public static final int DEFAULT_MIN_PREVIOUS = 5;
    private static final String FILTER =  "filter";
    public static final String DEFAULT_FILTER = "mean";//attribute value must be a constant

    @Transient
    private Json config;

    public RelativeDifference() {
        config = new Json();
    }

    public RelativeDifference(String name, String operation) {
        super(name,operation);
        config = new Json();
    }

    @PostLoad
    public void loadConfig(){
        if(this.config == null || this.config.isEmpty()){
            if(this.operation!=null && !this.operation.isBlank()){
                config = Json.fromString(this.operation);
            }else {
                config = new Json();
                //TODO load default values?
            }
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


    @Transient
    public double getThreshold(){
        return config.getDouble(THRESHOLD,DEFAULT_THRESHOLD);
    }
    public void setThreshold(double threshold){
        config.set(THRESHOLD,threshold);
        operation=config.toString();
    }
    @Transient
    public long getWindow(){
        return config.getLong(WINDOW,DEFAULT_WINDOW);
    }
    public void setWindow(long window){
        config.set(WINDOW,window);
        operation=config.toString();
    }
    @Transient
    public long getMinPrevious(){
        return config.getLong(MIN_PREVIOUS,DEFAULT_MIN_PREVIOUS);
    }
    public void setMinPrevious(long minPrevious){
        config.set(MIN_PREVIOUS,minPrevious);
        operation=config.toString();
    }
    @Transient
    public String getFilter(){
        return config.getString(FILTER);
    }
    public void setFilter(String filter){
        config.set(FILTER,filter);
        operation=config.toString();
    }

    @Override
    protected Node shallowCopy() {
        return new RelativeDifference(name,operation);
    }

}
