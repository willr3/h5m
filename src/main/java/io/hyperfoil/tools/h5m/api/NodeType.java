package io.hyperfoil.tools.h5m.api;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.Arrays;
import java.util.stream.Collectors;

@Schema(description = "The type of transformation a node performs")
public enum NodeType {
    EDIVISIVE("ed",true),
    FINGERPRINT("fp",true),
    FIXED_THRESHOLD("ft",true),
    JQ("jq",false),
    JS("js",false),
    JSONATA("nata",false),
    RELATIVE_DIFFERENCE("rd",true),
    ROOT("root",false),
    STDDEV_ANOMALY("sd",true),
    SPLIT("split",false),
    USER_INPUT("user",false);

    private final String display;
    private final boolean analysis;

    NodeType(String display,boolean analysis) {
        this.display = display;this.analysis = analysis;
    }

    public String display() {
        return display;
    }
    public boolean isAnalysis(){return analysis;}

    /**
     * Returns true if this node type is a change detection node.
     * Detection nodes (ft, rd, sd, ed) are never ephemeral-nullified
     * and their source nodes are protected from nullification.
     */
    public boolean isDetection() {
        return this == FIXED_THRESHOLD || this == RELATIVE_DIFFERENCE
            || this == STDDEV_ANOMALY || this == EDIVISIVE;
    }

}
