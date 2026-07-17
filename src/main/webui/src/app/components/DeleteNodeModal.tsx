import { Modal } from '@carbon/react';
import { deleteNodeMutation } from '@client/@tanstack/react-query.gen.ts';
import type { Node as ApiNode } from '@client/types.gen.ts';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { AxiosError } from 'axios';

interface DeleteNodeModalProps {
  node: ApiNode | null;
  onClose: () => void;
}

export const DeleteNodeModal = ({ node, onClose }: DeleteNodeModalProps) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<string | null>(null);

  const deleteNode = useMutation({
    ...deleteNodeMutation(),
    onSuccess: () => {
      void queryClient.invalidateQueries();
      onClose();
    },
    onError: (e: AxiosError<Error>) => {
        if (e.response?.status === 500) {
          setError('Cannot delete node');
        } else {
          setError(e.message ?? 'Failed to Delete Node');
        }
      },
  });

  const handleSubmit = () => {
    if (node?.id == null) return;
    deleteNode.mutate({ path: { id: node.id } });
  };

  return (
    <Modal
      open={node !== null}
      danger
      modalHeading="Delete node"
      modalLabel={node?.name ?? ''}
      primaryButtonText={deleteNode.isPending ? 'Deleting…' : 'Delete'}
      secondaryButtonText="Cancel"
      primaryButtonDisabled={deleteNode.isPending}
      onRequestSubmit={handleSubmit}
      onRequestClose={() => { setError(null); onClose(); }}
    >
      <p>
        Are you sure you want to delete <strong>{node?.name}</strong>?
      </p>
      <p style={{ marginTop: '0.75rem', color: 'var(--cds-text-secondary)' }}>
        All computed values for this node and any downstream nodes that depend on
        it will also be removed. This action cannot be undone.
      </p>
      {error && (
        <p style={{ marginTop: '0.75rem', color: 'var(--cds-support-error)' }}>
          {error}
        </p>
      )}
    </Modal>
  );
};
