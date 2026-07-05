import type { Node, NodeGroup } from '@client/types.gen.ts';
import type { GraphLabel, NodeLabel } from '@dagrejs/dagre';
import type { Edge, EdgeProps, Node as FlowNode } from '@xyflow/react';

import { blue50, gray40, gray90, green50, red60, yellow30, teal50 } from '@carbon/colors';
import { Code, Compare, FingerprintRecognition, FlowData, ForkNode, Home, Login, Rule, Script } from '@carbon/icons-react';
import { Tooltip } from '@carbon/react';
import dagre from '@dagrejs/dagre';
import { Graph } from '@dagrejs/graphlib';
import { BaseEdge, Controls, getSmoothStepPath, Handle, MarkerType, Position, ReactFlow, useReactFlow } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { ComponentType, useCallback, useEffect, useMemo, useState } from 'react';

const TYPE_COLORS: Record<string, string> = {
  ROOT: yellow30,
  SPLIT: yellow30,
  FINGERPRINT: blue50,
  FIXED_THRESHOLD: red60,
  RELATIVE_DIFFERENCE: red60,
  JS: green50,
  USER_INPUT: teal50,
};

const TYPE_ICONS: Record<string, ComponentType<{ size?: number }>> = {
  ROOT: Home,
  SPLIT: ForkNode,
  FINGERPRINT: FingerprintRecognition,
  FIXED_THRESHOLD: Rule,
  RELATIVE_DIFFERENCE: Compare,
  JQ: Script,
  JSONATA: Script,
  JS: Code,

  USER_INPUT: Login,
};

export function nodeColor(type?: string): string {
  return TYPE_COLORS[type ?? ''] ?? gray40;
}

export function prettyOperation(op: string) {
  let text = op;
  try {
    text = JSON.stringify(JSON.parse(op), null, 2);
  } catch {
    /* not JSON, use raw */
  }
  return <pre style={{ textAlign: 'left', fontSize: 7, maxWidth: 3 * NODE_WIDTH, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{text}</pre>;
}

const NODE_WIDTH = 200;
const NODE_HEIGHT = 40;
const GRID_UNIT = 10;
const EDGE_OFFSET = 20;

const FixedOffsetEdge = ({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, markerStart, markerEnd, style }: EdgeProps) => {
  const [path] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    stepPosition: 0.1,
    offset: EDGE_OFFSET,
    borderRadius: EDGE_OFFSET,
  });
  return <BaseEdge path={path} markerStart={markerStart} markerEnd={markerEnd} style={style} />;
};

const PipelineNode = ({ data }: { data: { name: string; operation?: string; nodeType?: string; sourceIds?: string[]; dimmed?: boolean } }) => {
  const color = nodeColor(data.nodeType);
  const Icon = (data.nodeType ? TYPE_ICONS[data.nodeType] : undefined) ?? FlowData;
  return (
    <div
      style={{
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
        padding: '6px 12px',
        fontSize: 10,
        textAlign: 'center',
        border: `1px solid ${color}`,
        borderRadius: 10,
        background: gray90,
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        position: 'relative',
        opacity: data.dimmed ? 0.2 : 1,
        transition: 'opacity 1s',
      }}
    >
      {data.nodeType !== 'ROOT' && <Handle type="target" position={Position.Left} />}
      <div style={{ position: 'absolute', top: 5, right: 5, color }}>
        <Tooltip align="top-right" enterDelayMs={1000} label={prettyOperation(data.nodeType ?? '')}>
          <Icon size={15} />
        </Tooltip>
      </div>
      <div style={{ fontWeight: 'bolder' }}>{data.name}</div>
      {data.operation && (
        <Tooltip align="bottom" enterDelayMs={1000} label={prettyOperation(data.operation)}>
          <div style={{ opacity: 0.7, marginTop: 5, fontSize: 7, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{data.operation}</div>
        </Tooltip>
      )}
      <Handle type="source" position={Position.Right} />
    </div>
  );
};

const nodeTypes = { pipeline: PipelineNode };
const edgeTypes = { fixedStep: FixedOffsetEdge };

export interface GraphNode extends Omit<Node, 'id' | 'sources'> {
  id: string;
  sources?: { id: string }[];
}

export function collectNodes(node: Node, map: Map<string, GraphNode>) {
  if (node.id == null || map.has(String(node.id))) return;
  map.set(String(node.id), {
    ...node,
    id: String(node.id),
    sources: node.sources?.filter((s) => s.id != null).map((s) => ({ id: String(s.id) })),
  });
  node.sources?.forEach((s) => {
    collectNodes(s, map);
  });
}

export function buildGraph(nodeGroup: NodeGroup): { nodes: FlowNode[]; edges: Edge[] } {
  const nodeMap = new Map<string, GraphNode>();
  if (nodeGroup.root) collectNodes(nodeGroup.root, nodeMap);
  nodeGroup.sources?.forEach((s) => {
    collectNodes(s, nodeMap);
  });
  const allNodes = [...nodeMap.values()];

  const graph = new Graph<GraphLabel, NodeLabel, Record<string, never>>();
  graph.setGraph({ align: 'DL', rankdir: 'LR', rankalign: 'center', ranksep: NODE_WIDTH, nodesep: NODE_HEIGHT / 2, edgesep: NODE_HEIGHT / 4 });
  graph.setDefaultEdgeLabel(() => ({}));

  const edges: Edge[] = [];

  allNodes.forEach((node) => {
    graph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
    const edgeColor = nodeColor(node.type);
    node.sources?.forEach((src) => {
      if (!nodeMap.has(src.id)) return;
      const key = `${src.id}-${node.id}`;
      if (!graph.hasEdge(src.id, node.id)) {
        graph.setEdge(src.id, node.id);
        edges.push({
          id: key,
          source: src.id,
          target: node.id,
          animated: true,
          style: { stroke: edgeColor },
          markerEnd: { type: MarkerType.ArrowClosed, color: edgeColor },
          data: { color: edgeColor },
          type: 'fixedStep',
        });
      }
    });
  });

  dagre.layout(graph);

  const nodes: FlowNode[] = allNodes.map((node) => {
    const nodePosition = graph.node(node.id);
    return {
      id: node.id,
      position: {
        x: Math.round(((nodePosition.x ?? 0) - NODE_WIDTH / 2) / GRID_UNIT) * GRID_UNIT,
        y: Math.round(((nodePosition.y ?? 0) - NODE_HEIGHT / 2) / GRID_UNIT) * GRID_UNIT,
      },
      type: 'pipeline',
      data: {
        name: node.name ?? 'root',
        operation: node.operation,
        nodeType: node.type,
        sourceIds: node.sources?.map((s) => s.id),
      },
    };
  });

  return { nodes, edges };
}

const HighlightManager = ({ selectedId }: { selectedId?: string }) => {
  const { setNodes, setEdges, getNodes } = useReactFlow();

  useEffect(() => {
    const selected = getNodes().find((n) => n.id === selectedId);
    const sourceIds = new Set((selected?.data as { sourceIds?: string[] } | undefined)?.sourceIds ?? []);
    const relatedIds = new Set([...sourceIds, selectedId]);

    setNodes((nodes) =>
      nodes.map((n) => ({
        ...n,
        data: {
          ...n.data,
          dimmed: selected !== undefined && !relatedIds.has(n.id),
        },
      })),
    );

    setEdges((edges) =>
      edges.map((e) => {
        const isHighlighted = selected !== undefined && e.target === selectedId && sourceIds.has(e.source);
        return {
          ...e,
          style: { stroke: e.style?.stroke, strokeWidth: isHighlighted ? 3 : 1, opacity: selected === undefined || isHighlighted ? 1 : 0 },
        };
      }),
    );
  }, [selectedId, getNodes, setNodes, setEdges]);

  return null;
};

export const NodeGraphVisualizer = ({ nodeGroup }: { nodeGroup: NodeGroup }) => {
  const { nodes, edges } = useMemo(() => buildGraph(nodeGroup), [nodeGroup]);
  const [selectedId, setSelectedId] = useState<string | undefined>();

  const onNodeClick = useCallback((_: unknown, node: FlowNode) => {
    setSelectedId((prev) => (prev === node.id ? undefined : node.id));
  }, []);
  const onPaneClick = useCallback(() => {
    setSelectedId(undefined);
  }, []);

  if (nodes.length === 0) {
    return <p>No nodes defined</p>;
  }

  return (
    <div style={{ height: 'calc(100vh - 200px)' }}>
      <ReactFlow
        defaultNodes={nodes}
        defaultEdges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        fitView
        snapToGrid
        snapGrid={[GRID_UNIT, GRID_UNIT]}
        minZoom={0.1}
        maxZoom={2}
        colorMode="dark"
        nodesConnectable={false}
      >
        <HighlightManager selectedId={selectedId} />
        <Controls position="top-right" showInteractive={false} />
      </ReactFlow>
    </div>
  );
};
