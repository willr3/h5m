import type { Node, NodeGroup } from '@client/types.gen.ts';
import type { GraphLabel, NodeLabel } from '@dagrejs/dagre';
import type { Edge, EdgeProps, Node as FlowNode } from '@xyflow/react';

import { blue50, gray40, gray70, gray90, green50, red60, yellow30 } from '@carbon/colors';
import { Code, Compare, FingerprintRecognition, FlowData, ForkNode, Home, Login, Rule, Script } from '@carbon/icons-react';
import { Tooltip } from '@carbon/react';
import dagre from '@dagrejs/dagre';
import { Graph } from '@dagrejs/graphlib';
import { BaseEdge, Controls, getSmoothStepPath, Handle, MarkerType, Position, ReactFlow } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { ComponentType, useMemo } from 'react';

const TYPE_COLORS: Record<string, string> = {
  ROOT: yellow30,
  SPLIT: yellow30,
  FINGERPRINT: blue50,
  FIXED_THRESHOLD: red60,
  RELATIVE_DIFFERENCE: red60,
  USER_INPUT: green50,
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
  SQL_JSONPATH_NODE: Script,
  SQL_JSONPATH_ALL_NODE: Script,
  USER_INPUT: Login,
};

function prettyOperation(op: string): string {
  try {
    return JSON.stringify(JSON.parse(op), null, 2);
  } catch {
    return op;
  }
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
    stepPosition: 0,
    offset: EDGE_OFFSET,
    borderRadius: EDGE_OFFSET,
  });
  return <BaseEdge path={path} markerStart={markerStart} markerEnd={markerEnd} style={style} />;
};

const PipelineNode = ({ data }: { data: { name: string; operation?: string; nodeType?: string } }) => {
  const borderColor = TYPE_COLORS[data.nodeType ?? ''] ?? gray40;
  const Icon = (data.nodeType ? TYPE_ICONS[data.nodeType] : undefined) ?? FlowData;
  return (
    <div
      style={{
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
        padding: '6px 12px',
        fontSize: 10,
        textAlign: 'center',
        border: `1px solid ${borderColor}`,
        borderRadius: 10,
        background: gray90,
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        position: 'relative',
      }}
    >
      {data.nodeType !== 'ROOT' && <Handle type="target" position={Position.Top} />}
      <div style={{ position: 'absolute', top: 5, right: 5, color: borderColor }}>
        <Icon size={15} />
      </div>
      <div style={{ fontWeight: 'bolder' }}>{data.name}</div>
      {data.operation && (
        <Tooltip enterDelayMs={1000} label={<pre style={{ textAlign: 'left', fontSize: 7 }}>{prettyOperation(data.operation)}</pre>}>
          <div style={{ opacity: 0.7, marginTop: 5, fontSize: 8, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{data.operation}</div>
        </Tooltip>
      )}
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
};

const nodeTypes = { pipeline: PipelineNode };
const edgeTypes = { fixedStep: FixedOffsetEdge };

interface GraphNode extends Omit<Node, 'id' | 'sources'> {
  id: string;
  sources?: { id: string }[];
}

function collectNodes(node: Node, map: Map<string, GraphNode>) {
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

function buildGraph(nodeGroup: NodeGroup): { nodes: FlowNode[]; edges: Edge[] } {
  const nodeMap = new Map<string, GraphNode>();
  if (nodeGroup.root) collectNodes(nodeGroup.root, nodeMap);
  nodeGroup.sources?.forEach((s) => {
    collectNodes(s, nodeMap);
  });
  const allNodes = [...nodeMap.values()];

  const graph = new Graph<GraphLabel, NodeLabel, Record<string, never>>();
  graph.setGraph({ align: 'DL' });
  graph.setDefaultEdgeLabel(() => ({}));

  const edges: Edge[] = [];

  allNodes.forEach((node) => {
    graph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
    node.sources?.forEach((src) => {
      if (!nodeMap.has(src.id)) return;
      const key = `${src.id}-${node.id}`;
      if (!graph.hasEdge(src.id, node.id)) {
        graph.setEdge(src.id, node.id);
        edges.push({ id: key, source: src.id, target: node.id, animated: true, markerEnd: { type: MarkerType.ArrowClosed, color: gray70 }, type: 'fixedStep' });
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
      data: { name: node.name ?? 'root', operation: node.operation, nodeType: node.type },
    };
  });

  return { nodes, edges };
}

export const NodeGraphVisualizer = ({ nodeGroup }: { nodeGroup: NodeGroup }) => {
  const { nodes, edges } = useMemo(() => buildGraph(nodeGroup), [nodeGroup]);
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
        fitView
        snapToGrid
        snapGrid={[GRID_UNIT, GRID_UNIT]}
        colorMode="dark"
        nodesConnectable={false}
      >
        <Controls position="top-right" showInteractive={false} />
      </ReactFlow>
    </div>
  );
};
