import {
  Button,
  ComposedModal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Select,
  SelectItem,
  TextArea,
  TextInput,
} from '@carbon/react';
import { setEphemeralMutation, updateMutation } from '@client/@tanstack/react-query.gen.ts';
import type { EphemeralMode, Node as ApiNode } from '@client/types.gen.ts';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { AxiosError } from 'axios';
import { useEffect, useState } from 'react';

interface EditNodeModalProps {
  node: ApiNode | null;
  onClose: () => void;
  onRecalculation?: (id: string) => void;
}

const DETECTION_TYPES = ['FIXED_THRESHOLD', 'RELATIVE_DIFFERENCE', 'STDDEV_ANOMALY', 'EDIVISIVE'];

export const EditNodeModal = ({ node, onClose, onRecalculation }: EditNodeModalProps) => {
  const [name, setName] = useState('');
  const [operation, setOperation] = useState('');
  const [ephemeral, setEphemeral] = useState<EphemeralMode>('KEEP');
  const [error, setError] = useState<string | null>(null);

  const queryClient = useQueryClient();

  useEffect(() => {
    if (node) {
      setName(node.name ?? '');
      setOperation(node.operation ?? '');
      setEphemeral(node.ephemeral ?? 'KEEP');
      setError(null);
    }
  }, [node]);

  const updateNode = useMutation({
    ...updateMutation(),
    onSuccess: (data) => {
      void queryClient.invalidateQueries();
      if (data.recalculation?.id) {
        onRecalculation?.(data.recalculation.id);
      }
      onClose();
    },
    onError: (e: AxiosError<Error>) => {
      setError(e.message ?? 'Failed to update node');
    },
  });

  const updateEphemeral = useMutation({
    ...setEphemeralMutation(),
    onSuccess: () => {
      void queryClient.invalidateQueries();
      onClose();
    },
    onError: (e: AxiosError<Error>) => {
      setError(e.message ?? 'Failed to update ephemeral mode');
    },
  });

  const isDetection = DETECTION_TYPES.includes(node?.type ?? '');
  const isPending = updateNode.isPending || updateEphemeral.isPending;

  const handleSave = () => {
    if (name.trim() === '') { setError('Node name is required'); return; }
    if (!node?.id) return;

    const nameOrOpChanged = name.trim() !== node.name || operation.trim() !== (node.operation ?? '');
    const ephemeralChanged = ephemeral !== node.ephemeral;

    if (!nameOrOpChanged && !ephemeralChanged) { onClose(); return; }

    if (nameOrOpChanged) {
      updateNode.mutate({
        path: { id: node.id },
        body: { name: name.trim(), operation: operation.trim() || undefined },
      });
    }

    if (ephemeralChanged) {
      updateEphemeral.mutate({
        path: { id: node.id },
        query: { mode: ephemeral },
      });
    }
  };

  const handleClose = () => { setError(null); onClose(); };

  return (
    <ComposedModal open={node !== null} onClose={handleClose}>
      <ModalHeader title="Edit Node" label={node?.name ?? ''} />
      <ModalBody>
        <TextInput
          id="edit-node-name"
          labelText="Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />

        {!isDetection && (
          <div style={{ marginTop: '1rem' }}>
            <TextArea
              id="edit-node-operation"
              labelText="Operation"
              value={operation}
              rows={4}
              onChange={(e) => setOperation(e.target.value)}
              helperText={
                node?.type === 'JQ' || node?.type === 'JSONATA'
                  ? 'Expression applied to the selected source, e.g. .cpu'
                  : node?.type === 'JS'
                  ? 'Arrow function — parameter names match source node names, e.g. (cpu, memory) => cpu / memory'
                  : undefined
              }
              style={{ fontFamily: 'var(--cds-code-01-font-family, monospace)' }}
            />
          </div>
        )}

        <Select
          id="edit-node-ephemeral"
          labelText="Ephemeral mode"
          value={ephemeral}
          onChange={(e) => setEphemeral(e.target.value as EphemeralMode)}
          disabled={isDetection}
          helperText={isDetection ? 'Detection nodes cannot be set to ephemeral' : undefined}
          style={{ marginTop: '1rem' }}
        >
          <SelectItem value="KEEP" text="Keep — always retain computed values" />
          <SelectItem value="AUTO" text="Auto — system decides based on downstream nodes" />
          <SelectItem value="DISCARD" text="Discard — delete computed values after use" />
        </Select>

        {error && (
          <p style={{ color: 'var(--cds-support-error)', marginTop: '0.75rem' }}>{error}</p>
        )}
      </ModalBody>
      <ModalFooter>
        <Button kind="secondary" onClick={handleClose}>
          Cancel
        </Button>
        <Button kind="primary" onClick={handleSave} disabled={isPending}>
          {isPending ? 'Saving…' : 'Save'}
        </Button>
      </ModalFooter>
    </ComposedModal>
  );
};
