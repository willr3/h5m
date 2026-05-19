package io.hyperfoil.tools.h5m.entity.mapper;

import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.api.NodeGroup;
import io.hyperfoil.tools.h5m.api.Value;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import org.mapstruct.Context;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

@Mapper(componentModel = MappingConstants.ComponentModel.JAKARTA)
public interface ApiMapper {

    @Mapping(target = "groupId", source = "group.id")
    Folder toFolder(FolderEntity folder);

    @Mapping(target = "type", expression = "java(node.type())")
    @Mapping(target = "groupId", source = "group.id")
    Node toNode(NodeEntity node, @Context CycleAvoidingContext context);

    NodeGroup toNodeGroup(NodeGroupEntity nodeGroup, @Context CycleAvoidingContext context);

    Value toValue(ValueEntity value, @Context CycleAvoidingContext context);

}
