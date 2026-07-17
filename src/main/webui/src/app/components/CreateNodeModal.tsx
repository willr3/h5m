import {
  Button,
  Checkbox,
  ComposedModal,
  FormGroup,
  ModalBody,
  ModalFooter,
  ModalHeader,
  MultiSelect,
  Select,
  SelectItem,
  TextArea,
  TextInput,
} from '@carbon/react';
import {
  byIdOptions,
  createConfiguredMutation,
  createNodeMutation,
} from '@client/@tanstack/react-query.gen.ts';
import type { Node as ApiNode, NodeType } from '@client/types.gen.ts';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { AxiosError } from 'axios';

interface CreateNodeModalProps {
  open: boolean;
  onClose: () => void;
  groupId: number;
}

type NodeCategory = 'extraction' | 'aggregation' | 'detection';

const CATEGORIES: { value: NodeCategory; label: string }[] = [
  { value: 'extraction', label: 'Extraction' },
  { value: 'aggregation', label: 'Aggregation' },
  { value: 'detection', label: 'Detection' },
];

const TYPES_BY_CATEGORY: Record<NodeCategory, { value: NodeType; label: string }[]> = {
  extraction: [
    { value: 'JQ', label: 'JQ' },
    { value: 'JS', label: 'JavaScript' },
    { value: 'JSONATA', label: 'JSONata' },
    { value: 'SPLIT', label: 'Split' },
  ],
  aggregation: [
    { value: 'FINGERPRINT', label: 'Fingerprint' },
  ],
  detection: [
    { value: 'FIXED_THRESHOLD', label: 'Fixed Threshold' },
    { value: 'RELATIVE_DIFFERENCE', label: 'Relative Difference' },
    { value: 'STDDEV_ANOMALY', label: 'StdDev Anomaly' },
    { value: 'EDIVISIVE', label: 'E-Divisive' },
  ],
};

const DETECTION_TYPES: NodeType[] = ['FIXED_THRESHOLD', 'RELATIVE_DIFFERENCE', 'STDDEV_ANOMALY', 'EDIVISIVE'];
const OPERATION_TYPES: NodeType[] = ['JQ', 'JS', 'JSONATA', 'SPLIT'];

interface FtConfig { min: string; max: string; minInclusive: boolean; maxInclusive: boolean; fingerprintFilter: string }
interface RdConfig { filter: string; threshold: string; window: string; minPrevious: string; fingerprintFilter: string }
interface SdConfig { windowSize: string; deviations: string; direction: string; minDataPoints: string; fingerprintFilter: string }
interface EdConfig { windowLen: string; maxPvalue: string; minMagnitude: string; maxSeriesLength: string; fingerprintFilter: string }

const INITIAL_FT: FtConfig = { min: '', max: '', minInclusive: true, maxInclusive: true, fingerprintFilter: '' };
const INITIAL_RD: RdConfig = { filter: 'mean', threshold: '0.2', window: '1', minPrevious: '5', fingerprintFilter: '' };
const INITIAL_SD: SdConfig = { windowSize: '10', deviations: '2.0', direction: 'BOTH', minDataPoints: '5', fingerprintFilter: '' };
const INITIAL_ED: EdConfig = { windowLen: '50', maxPvalue: '0.001', minMagnitude: '0.0', maxSeriesLength: '500', fingerprintFilter: '' };

const inputStyle = { marginTop: '1rem' };
const monoStyle: React.CSSProperties = { fontFamily: 'var(--cds-code-01-font-family, monospace)' };

export const CreateNodeModal = ({ open, onClose, groupId }: CreateNodeModalProps) => {
  const [nodeName, setNodeName] = useState('');
  const [nodeCategory, setNodeCategory] = useState<NodeCategory | ''>('');
  const [nodeType, setNodeType] = useState<NodeType | ''>('');
  const [operation, setOperation] = useState('');
  const [sourceNode, setSourceNode] = useState('');
  const [jsSelectKey, setJsSelectKey] = useState(0);
  const [jqSelectKey, setJqSelectKey] = useState(0);
  const [error, setError] = useState<string | null>(null);

  // Sources for FINGERPRINT (multi-select)
  const [fpSources, setFpSources] = useState<number[]>([]);
  // Sources for detection nodes (ordered slots)
  const [srcFingerprint, setSrcFingerprint] = useState('');
  const [srcGroupBy, setSrcGroupBy] = useState('');
  const [srcRange, setSrcRange] = useState('');
  const [srcDomain, setSrcDomain] = useState('');

  // Detection configs
  const [ftConfig, setFtConfig] = useState<FtConfig>(INITIAL_FT);
  const [rdConfig, setRdConfig] = useState<RdConfig>(INITIAL_RD);
  const [sdConfig, setSdConfig] = useState<SdConfig>(INITIAL_SD);
  const [edConfig, setEdConfig] = useState<EdConfig>(INITIAL_ED);

  const queryClient = useQueryClient();

  const { data: nodeGroup, isLoading: nodesLoading } = useQuery({
    ...byIdOptions({ path: { id: groupId } }),
    enabled: open,
  });

  const availableNodes: ApiNode[] = [
    ...(nodeGroup?.root ? [nodeGroup.root] : []),
    ...(nodeGroup?.sources ?? []),
  ];

  const createNode = useMutation({
    ...createNodeMutation(),
    onSuccess: handleSuccess,
    onError: handleError,
  });

  const createConfigured = useMutation({
    ...createConfiguredMutation(),
    onSuccess: handleSuccess,
    onError: handleError,
  });

  function handleSuccess() {
    void queryClient.invalidateQueries();
    resetState();
    onClose();
  }

  function handleError(e: AxiosError<Error>) {
    if (e.response?.status === 409) {
      setError('A node with the same name already exists');
    } else {
      setError(e.message ?? 'Failed to create node');
    }
  }

  function resetState() {
    setNodeName('');
    setNodeCategory('');
    setNodeType('');
    setOperation('');
    setSourceNode('');
    setJsSelectKey((k) => k + 1);
    setJqSelectKey((k) => k + 1);
    setError(null);
    setFpSources([]);
    setSrcFingerprint('');
    setSrcGroupBy('');
    setSrcRange('');
    setSrcDomain('');
    setFtConfig(INITIAL_FT);
    setRdConfig(INITIAL_RD);
    setSdConfig(INITIAL_SD);
    setEdConfig(INITIAL_ED);
  }

  function buildDetectionSources(): number[] {
    const srcs = [Number(srcFingerprint), Number(srcGroupBy), Number(srcRange)];
    if (nodeType !== 'FIXED_THRESHOLD' && srcDomain) srcs.push(Number(srcDomain));
    return srcs;
  }

  function buildConfig(): Record<string, unknown> | undefined {
    switch (nodeType) {
      case 'FIXED_THRESHOLD': {
        const cfg: Record<string, unknown> = {};
        if (ftConfig.min !== '') cfg.min = Number(ftConfig.min);
        if (ftConfig.max !== '') cfg.max = Number(ftConfig.max);
        cfg.minInclusive = ftConfig.minInclusive;
        cfg.maxInclusive = ftConfig.maxInclusive;
        if (ftConfig.fingerprintFilter) cfg.fingerprintFilter = ftConfig.fingerprintFilter;
        return cfg;
      }
      case 'RELATIVE_DIFFERENCE': {
        const cfg: Record<string, unknown> = {
          filter: rdConfig.filter,
          threshold: Number(rdConfig.threshold),
          window: Number(rdConfig.window),
          minPrevious: Number(rdConfig.minPrevious),
        };
        if (rdConfig.fingerprintFilter) cfg.fingerprintFilter = rdConfig.fingerprintFilter;
        return cfg;
      }
      case 'STDDEV_ANOMALY': {
        const cfg: Record<string, unknown> = {
          windowSize: Number(sdConfig.windowSize),
          deviations: Number(sdConfig.deviations),
          direction: sdConfig.direction,
          minDataPoints: Number(sdConfig.minDataPoints),
        };
        if (sdConfig.fingerprintFilter) cfg.fingerprintFilter = sdConfig.fingerprintFilter;
        return cfg;
      }
      case 'EDIVISIVE': {
        const cfg: Record<string, unknown> = {
          windowLen: Number(edConfig.windowLen),
          maxPvalue: Number(edConfig.maxPvalue),
          minMagnitude: Number(edConfig.minMagnitude),
          maxSeriesLength: Number(edConfig.maxSeriesLength),
        };
        if (edConfig.fingerprintFilter) cfg.fingerprintFilter = edConfig.fingerprintFilter;
        return cfg;
      }
      default:
        return undefined;
    }
  }

  const handleSave = () => {
    if (nodeName.trim() === '') { setError('Node name is required'); return; }
    if (nodeType === '') { setError('Node type is required'); return; }

    if (DETECTION_TYPES.includes(nodeType as NodeType)) {
      if (!srcFingerprint || !srcGroupBy || !srcRange) {
        setError('Fingerprint, GroupBy, and Range source nodes are required');
        return;
      }
      createConfigured.mutate({
        query: { name: nodeName.trim(), groupId, type: nodeType as NodeType, sources: buildDetectionSources() },
        body: buildConfig(),
      });
    } else if (nodeType === 'FINGERPRINT') {
      if (fpSources.length === 0) { setError('At least one source node is required for Fingerprint'); return; }
      createConfigured.mutate({
        query: { name: nodeName.trim(), groupId, type: 'FINGERPRINT', sources: fpSources },
        body: undefined,
      });
    } else {
      if (operation.trim() === '') { setError('Operation is required'); return; }
      createNode.mutate({
        query: { name: nodeName.trim(), groupId, type: nodeType as NodeType, operation: operation.trim() },
      });
    }
  };

  const jsNodeItems = availableNodes
    .filter((n) => n.type !== 'ROOT')
    .map((n) => ({ id: String(n.id), label: `${n.name ?? '?'} (${n.type ?? '?'})`, name: n.name ?? '' }));

  const jqNodeItems = availableNodes
    .filter((n) => n.type !== 'ROOT')
    .map((n) => ({ id: String(n.id), label: `${n.name ?? '?'} (${n.type ?? '?'})`, name: n.name ?? '' }));

  const handleJsSourceChange = ({ selectedItems }: { selectedItems: { id: string; label: string; name: string }[] | null }) => {
    const names = (selectedItems ?? []).map((i) => i.name);
    const params = names.join(', ');
    const bodyMatch = operation.match(/=>\s*([\s\S]*)$/);
    const body = bodyMatch ? (bodyMatch[1] ?? '').trim() : '';
    setOperation(names.length > 0 ? `(${params}) => ${body}` : body);
  };

  const handleJqSourceChange = ({ selectedItems }: { selectedItems: { id: string; label: string; name: string }[] | null }) => {
    const names = (selectedItems ?? []).map((i) => i.name);
    const stripped = operation.replace(/^\{[^}]+\}:/, '');
    setOperation(names.length > 0 ? `{${names.join(',')}}:${stripped}` : stripped);
  };

  const handleSourceSelect = (nodeId: string) => {
    setSourceNode(nodeId);
    if (!nodeId) {
      setOperation(operation.replace(/^\{[^}]+\}:/, ''));
      return;
    }
    const selected = availableNodes.find((n) => String(n.id) === nodeId);
    if (!selected?.name) return;
    const prefix = `{${selected.name}}:`;
    const stripped = operation.replace(/^\{[^}]+\}:/, '');
    setOperation(prefix + stripped);
  };

  const isPending = createNode.isPending || createConfigured.isPending;
  const isDetection = DETECTION_TYPES.includes(nodeType as NodeType);
  const isOperation = OPERATION_TYPES.includes(nodeType as NodeType);

  const nodeSelectItems = availableNodes.map((n) => ({
    id: String(n.id),
    label: `${n.name ?? '?'} (${n.type ?? '?'})`,
    nodeId: n.id!,
  }));

  return (
    <ComposedModal open={open} onClose={() => { resetState(); onClose(); }} size="lg">
      <ModalHeader title="Create Node" />
      <ModalBody>
        <TextInput
          id="node-name"
          labelText="Name"
          placeholder="e.g. cpu"
          value={nodeName}
          onChange={(e) => setNodeName(e.target.value)}
        />

        <Select
          id="node-category"
          labelText="Category"
          value={nodeCategory}
          onChange={(e) => {
            setNodeCategory(e.target.value as NodeCategory);
            setNodeType('');
            setOperation('');
            setSourceNode('');
            setJsSelectKey((k) => k + 1);
            setJqSelectKey((k) => k + 1);
            setError(null);
          }}
          style={inputStyle}
        >
          <SelectItem value="" text="Select category" />
          {CATEGORIES.map((c) => (
            <SelectItem key={c.value} value={c.value} text={c.label} />
          ))}
        </Select>

        <Select
          id="node-type"
          labelText="Type"
          value={nodeType}
          onChange={(e) => {
            setNodeType(e.target.value as NodeType);
            setOperation('');
            setSourceNode('');
            setJsSelectKey((k) => k + 1);
            setJqSelectKey((k) => k + 1);
            setError(null);
          }}
          disabled={nodeCategory === ''}
          style={inputStyle}
        >
          <SelectItem value="" text={nodeCategory ? 'Select type' : '— select a category first —'} />
          {(nodeCategory ? TYPES_BY_CATEGORY[nodeCategory] : []).map((t) => (
            <SelectItem key={t.value} value={t.value} text={t.label} />
          ))}
        </Select>

        {/* Source node selector — JQ multi-select */}
        {nodeType === 'JQ' && (
          <div style={inputStyle}>
            <MultiSelect
              key={jqSelectKey}
              id="jq-sources"
              titleText="Source nodes (optional)"
              label="Select source nodes"
              disabled={nodesLoading}
              items={jqNodeItems}
              itemToString={(item) => item?.label ?? ''}
              onChange={handleJqSourceChange}
            />
          </div>
        )}

        {/* Source node selector — JSONata single select */}
        {nodeType === 'JSONATA' && (
          <Select
            id="node-source"
            labelText="Source node (optional)"
            value={sourceNode}
            onChange={(e) => handleSourceSelect(e.target.value)}
            disabled={nodesLoading}
            style={inputStyle}
          >
            <SelectItem value="" text="Root (no parent source)" />
            {availableNodes
              .filter((n) => n.type !== 'ROOT')
              .map((n) => (
                <SelectItem key={n.id} value={String(n.id)} text={`${n.name ?? '?'} (${n.type ?? '?'})`} />
              ))}
          </Select>
        )}

        {/* Source node selector — JS multi-select */}
        {nodeType === 'JS' && (
          <div style={inputStyle}>
            <MultiSelect
              key={jsSelectKey}
              id="js-sources"
              titleText="Source nodes (optional)"
              label="Select source nodes"
              disabled={nodesLoading}
              items={jsNodeItems}
              itemToString={(item) => item?.label ?? ''}
              onChange={handleJsSourceChange}
            />
          </div>
        )}

        {/* Operation — JQ / JS / JSONata / Split */}
        {isOperation && (
          <div style={inputStyle}>
            <TextArea
              id="node-operation"
              labelText="Operation"
              placeholder={
                nodeType === 'JQ' ? '.cpu' :
                nodeType === 'JS' ? '(cpu, memory) => cpu / memory' :
                nodeType === 'JSONATA' ? 'payload.cpu' :
                'expression'
              }
              helperText={
                nodeType === 'JQ' || nodeType === 'JSONATA'
                  ? 'Expression applied to the selected source, e.g. .cpu'
                  : nodeType === 'JS'
                  ? 'Arrow function — parameter names match source node names, e.g. (cpu, memory) => cpu / memory'
                  : undefined
              }
              rows={4}
              value={operation}
              onChange={(e) => setOperation(e.target.value)}
              style={monoStyle}
            />
          </div>
        )}

        {/* Sources — FINGERPRINT (multi-select) */}
        {nodeType === 'FINGERPRINT' && (
          <div style={inputStyle}>
            <MultiSelect
              id="fp-sources"
              titleText="Source nodes"
              label={'Select source nodes'}
              disabled={nodesLoading}
              items={nodeSelectItems}
              itemToString={(item) => item?.label ?? ''}
              onChange={({ selectedItems }) => setFpSources((selectedItems ?? []).map((i) => i.nodeId))}
            />
          </div>
        )}

        {/* Sources — Detection nodes (ordered slots) */}
        {isDetection && (
          <FormGroup legendText="Source nodes (order matters)" style={inputStyle}>
            <Select
              id="src-fingerprint"
              labelText="Fingerprint node"
              value={srcFingerprint}
              onChange={(e) => setSrcFingerprint(e.target.value)}
              disabled={nodesLoading}
            >
              <SelectItem value="" text={'Select fingerprint node'} />
              {availableNodes.map((n) => (
                <SelectItem key={n.id} value={String(n.id)} text={`${n.name ?? '?'} (${n.type ?? '?'})`} />
              ))}
            </Select>

            <Select
              id="src-groupby"
              labelText="GroupBy node"
              value={srcGroupBy}
              onChange={(e) => setSrcGroupBy(e.target.value)}
              disabled={nodesLoading}
              style={{ marginTop: '0.75rem' }}
            >
              <SelectItem value="" text={'Select groupBy node'} />
              {availableNodes.map((n) => (
                <SelectItem key={n.id} value={String(n.id)} text={`${n.name ?? '?'} (${n.type ?? '?'})`} />
              ))}
            </Select>

            <Select
              id="src-range"
              labelText="Range node"
              value={srcRange}
              onChange={(e) => setSrcRange(e.target.value)}
              disabled={nodesLoading}
              style={{ marginTop: '0.75rem' }}
            >
              <SelectItem value="" text={'Select range node'} />
              {availableNodes.map((n) => (
                <SelectItem key={n.id} value={String(n.id)} text={`${n.name ?? '?'} (${n.type ?? '?'})`} />
              ))}
            </Select>

            {nodeType !== 'FIXED_THRESHOLD' && (
              <Select
                id="src-domain"
                labelText="Domain node (optional)"
                value={srcDomain}
                onChange={(e) => setSrcDomain(e.target.value)}
                disabled={nodesLoading}
                style={{ marginTop: '0.75rem' }}
              >
                <SelectItem value="" text="None" />
                {availableNodes.map((n) => (
                  <SelectItem key={n.id} value={String(n.id)} text={`${n.name ?? '?'} (${n.type ?? '?'})`} />
                ))}
              </Select>
            )}
          </FormGroup>
        )}

        {/* Config — Fixed Threshold */}
        {nodeType === 'FIXED_THRESHOLD' && (
          <FormGroup legendText="Threshold configuration" style={inputStyle}>
            <div style={{ display: 'flex', gap: '1rem' }}>
              <TextInput
                id="ft-min"
                labelText="Min"
                placeholder="e.g. 0"
                type="number"
                value={ftConfig.min}
                onChange={(e) => setFtConfig((p) => ({ ...p, min: e.target.value }))}
                style={{ flex: 1 }}
              />
              <TextInput
                id="ft-max"
                labelText="Max"
                placeholder="e.g. 100"
                type="number"
                value={ftConfig.max}
                onChange={(e) => setFtConfig((p) => ({ ...p, max: e.target.value }))}
                style={{ flex: 1 }}
              />
            </div>
            <div style={{ display: 'flex', gap: '2rem', marginTop: '0.75rem' }}>
              <Checkbox
                id="ft-min-inclusive"
                labelText="Min inclusive"
                checked={ftConfig.minInclusive}
                onChange={(_, { checked }) => setFtConfig((p) => ({ ...p, minInclusive: checked }))}
              />
              <Checkbox
                id="ft-max-inclusive"
                labelText="Max inclusive"
                checked={ftConfig.maxInclusive}
                onChange={(_, { checked }) => setFtConfig((p) => ({ ...p, maxInclusive: checked }))}
              />
            </div>
          </FormGroup>
        )}

        {/* Config — Relative Difference */}
        {nodeType === 'RELATIVE_DIFFERENCE' && (
          <FormGroup legendText="Relative difference configuration" style={inputStyle}>
            <Select
              id="rd-filter"
              labelText="Aggregation filter"
              value={rdConfig.filter}
              onChange={(e) => setRdConfig((p) => ({ ...p, filter: e.target.value }))}
            >
              {['mean', 'median', 'min', 'max'].map((f) => (
                <SelectItem key={f} value={f} text={f} />
              ))}
            </Select>
            <div style={{ display: 'flex', gap: '1rem', marginTop: '0.75rem' }}>
              <TextInput
                id="rd-threshold"
                labelText="Threshold (fraction)"
                placeholder="0.2"
                type="number"
                value={rdConfig.threshold}
                onChange={(e) => setRdConfig((p) => ({ ...p, threshold: e.target.value }))}
                helperText="e.g. 0.2 = 20%"
                style={{ flex: 1 }}
              />
              <TextInput
                id="rd-window"
                labelText="Window"
                placeholder="1"
                type="number"
                value={rdConfig.window}
                onChange={(e) => setRdConfig((p) => ({ ...p, window: e.target.value }))}
                helperText="Recent values to compare"
                style={{ flex: 1 }}
              />
              <TextInput
                id="rd-min-previous"
                labelText="Min previous"
                placeholder="5"
                type="number"
                value={rdConfig.minPrevious}
                onChange={(e) => setRdConfig((p) => ({ ...p, minPrevious: e.target.value }))}
                helperText="History required"
                style={{ flex: 1 }}
              />
            </div>
          </FormGroup>
        )}

        {/* Config — StdDev Anomaly */}
        {nodeType === 'STDDEV_ANOMALY' && (
          <FormGroup legendText="StdDev anomaly configuration" style={inputStyle}>
            <div style={{ display: 'flex', gap: '1rem' }}>
              <TextInput
                id="sd-window"
                labelText="Window size"
                placeholder="10"
                type="number"
                value={sdConfig.windowSize}
                onChange={(e) => setSdConfig((p) => ({ ...p, windowSize: e.target.value }))}
                style={{ flex: 1 }}
              />
              <TextInput
                id="sd-deviations"
                labelText="Deviations"
                placeholder="2.0"
                type="number"
                value={sdConfig.deviations}
                onChange={(e) => setSdConfig((p) => ({ ...p, deviations: e.target.value }))}
                style={{ flex: 1 }}
              />
              <TextInput
                id="sd-min-dp"
                labelText="Min data points"
                placeholder="5"
                type="number"
                value={sdConfig.minDataPoints}
                onChange={(e) => setSdConfig((p) => ({ ...p, minDataPoints: e.target.value }))}
                style={{ flex: 1 }}
              />
            </div>
            <Select
              id="sd-direction"
              labelText="Direction"
              value={sdConfig.direction}
              onChange={(e) => setSdConfig((p) => ({ ...p, direction: e.target.value }))}
              style={{ marginTop: '0.75rem' }}
            >
              <SelectItem value="BOTH" text="Both" />
              <SelectItem value="UPPER" text="Upper" />
              <SelectItem value="LOWER" text="Lower" />
            </Select>
          </FormGroup>
        )}

        {/* Config — E-Divisive */}
        {nodeType === 'EDIVISIVE' && (
          <FormGroup legendText="E-Divisive configuration" style={inputStyle}>
            <div style={{ display: 'flex', gap: '1rem' }}>
              <TextInput
                id="ed-window"
                labelText="Window length"
                placeholder="50"
                type="number"
                value={edConfig.windowLen}
                onChange={(e) => setEdConfig((p) => ({ ...p, windowLen: e.target.value }))}
                helperText="Min 3; ≥100 data points recommended"
                style={{ flex: 1 }}
              />
              <TextInput
                id="ed-pvalue"
                labelText="Max p-value"
                placeholder="0.001"
                type="number"
                value={edConfig.maxPvalue}
                onChange={(e) => setEdConfig((p) => ({ ...p, maxPvalue: e.target.value }))}
                helperText="Significance threshold"
                style={{ flex: 1 }}
              />
            </div>
            <div style={{ display: 'flex', gap: '1rem', marginTop: '0.75rem' }}>
              <TextInput
                id="ed-magnitude"
                labelText="Min magnitude"
                placeholder="0.0"
                type="number"
                value={edConfig.minMagnitude}
                onChange={(e) => setEdConfig((p) => ({ ...p, minMagnitude: e.target.value }))}
                helperText="e.g. 0.1 = 10% change"
                style={{ flex: 1 }}
              />
              <TextInput
                id="ed-max-series"
                labelText="Max series length"
                placeholder="500"
                type="number"
                value={edConfig.maxSeriesLength}
                onChange={(e) => setEdConfig((p) => ({ ...p, maxSeriesLength: e.target.value }))}
                helperText="Most recent N points to analyze"
                style={{ flex: 1 }}
              />
            </div>
          </FormGroup>
        )}

        {error && (
          <p style={{ color: 'var(--cds-support-error)', marginTop: '0.75rem' }}>{error}</p>
        )}
      </ModalBody>
      <ModalFooter>
        <Button kind="secondary" onClick={() => { resetState(); onClose(); }}>
          Cancel
        </Button>
        <Button kind="primary" onClick={handleSave} disabled={isPending}>
          {isPending ? 'Saving…' : 'Save'}
        </Button>
      </ModalFooter>
    </ComposedModal>
  );
};
