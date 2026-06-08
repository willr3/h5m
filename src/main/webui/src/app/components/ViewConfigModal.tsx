import type { Node as ApiNode, View, ViewComponent } from '@client/types.gen.ts';

import {
  Button,
  ComposedModal,
  FilterableMultiSelect,
  ModalBody,
  ModalFooter,
  ModalHeader,
  TextInput,
} from '@carbon/react';
import { ArrowUp, ArrowDown, Close } from '@carbon/icons-react';
import { byIdOptions } from '@client/@tanstack/react-query.gen.ts';
import { ViewService } from '@client/sdk.gen.ts';
import { useMutation, useQueryClient, useSuspenseQuery } from '@tanstack/react-query';
import { useCallback, useState } from 'react';

interface ViewConfigModalProps {
  open: boolean;
  onClose: () => void;
  folderName: string;
  groupId: number;
  view?: View | null;
}

interface NodeItem {
  id: string;
  text: string;
  nodeId: number;
}

export const ViewConfigModal = ({ open, onClose, folderName, groupId, view }: ViewConfigModalProps) => {
  const queryClient = useQueryClient();
  const { data: nodeGroup } = useSuspenseQuery(byIdOptions({ path: { id: groupId } }));
  const isEditing = view != null && view.id != null;
  const isDefault = view?.name === 'Default';

  // Available nodes for the multi-select (exclude detection nodes)
  // Built once and stable so FilterableMultiSelect can match by reference
  const availableNodes: NodeItem[] = (nodeGroup.sources ?? [])
    .filter((n: ApiNode) => !['FIXED_THRESHOLD', 'RELATIVE_DIFFERENCE', 'EDIVISIVE'].includes(n.type ?? ''))
    .map((n: ApiNode) => ({
      id: String(n.id),
      text: n.name ?? '',
      nodeId: n.id!,
    }));

  const [viewName, setViewName] = useState(view?.name ?? '');
  // Initialize selectedNodes from the view's components, preserving the
  // existing headerOrder. Find matching items in availableNodes (same
  // object references required by Carbon's FilterableMultiSelect).
  const [selectedNodes, setSelectedNodes] = useState<NodeItem[]>(() => {
    if (!view?.components || view.components.length === 0) return [];
    // Sort components by headerOrder to preserve column ordering
    const sorted = [...view.components].sort(
      (a: ViewComponent, b: ViewComponent) => (a.headerOrder ?? 0) - (b.headerOrder ?? 0)
    );
    const ordered: NodeItem[] = [];
    for (const c of sorted) {
      const match = availableNodes.find((n) => n.id === String(c.nodeId));
      if (match) ordered.push(match);
    }
    return ordered;
  });

  const moveUp = useCallback((index: number) => {
    if (index <= 0) return;
    setSelectedNodes((prev) => {
      const next = [...prev];
      const temp = next[index - 1]!;
      next[index - 1] = next[index]!;
      next[index] = temp;
      return next;
    });
  }, []);

  const moveDown = useCallback((index: number) => {
    setSelectedNodes((prev) => {
      if (index >= prev.length - 1) return prev;
      const next = [...prev];
      const temp = next[index]!;
      next[index] = next[index + 1]!;
      next[index + 1] = temp;
      return next;
    });
  }, []);

  const removeNode = useCallback((index: number) => {
    setSelectedNodes((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const [saveError, setSaveError] = useState<string | null>(null);

  const createMutation = useMutation({
    mutationFn: (data: View) =>
      ViewService.createView({
        path: { name: folderName },
        body: data,
      }),
    onSuccess: () => {
      setSaveError(null);
      void queryClient.refetchQueries({ predicate: (q) => {
        const key = q.queryKey[0];
        return typeof key === 'object' && key !== null && '_id' in key &&
          (key._id === 'getViews' || key._id === 'getViewData');
      }});
      onClose();
    },
    onError: (e: Error) => {
      setSaveError(e.message ?? 'Failed to create view');
    },
  });

  const updateMutation = useMutation({
    mutationFn: (data: View) =>
      ViewService.updateView({
        path: { name: folderName, viewId: view!.id! },
        body: data,
      }),
    onSuccess: () => {
      setSaveError(null);
      void queryClient.refetchQueries({ predicate: (q) => {
        const key = q.queryKey[0];
        return typeof key === 'object' && key !== null && '_id' in key &&
          (key._id === 'getViews' || key._id === 'getViewData');
      }});
      onClose();
    },
    onError: (e: Error) => {
      setSaveError(e.message ?? 'Failed to update view');
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () =>
      ViewService.deleteView({
        path: { name: folderName, viewId: view!.id! },
      }),
    onSuccess: () => {
      void queryClient.refetchQueries({ predicate: (q) => {
        const key = q.queryKey[0];
        return typeof key === 'object' && key !== null && '_id' in key &&
          (key._id === 'getViews' || key._id === 'getViewData');
      }});
      onClose();
    },
  });

  const handleSave = useCallback(() => {
    const components: ViewComponent[] = selectedNodes.map((node, idx) => ({
      nodeId: node.nodeId,
      headerName: node.text,
      headerOrder: idx,
    }));

    const viewData: View = {
      name: viewName,
      components,
    };

    if (isEditing) {
      updateMutation.mutate(viewData);
    } else {
      createMutation.mutate(viewData);
    }
  }, [viewName, selectedNodes, isEditing, createMutation, updateMutation]);

  const handleDelete = useCallback(() => {
    if (isEditing && !isDefault && window.confirm('Delete this view?')) {
      deleteMutation.mutate();
    }
  }, [isEditing, isDefault, deleteMutation]);

  const isSaving = createMutation.isPending || updateMutation.isPending;
  const canSave = viewName.trim().length > 0 && selectedNodes.length > 0;

  return (
    <ComposedModal open={open} onClose={onClose} size="lg">
      <ModalHeader title={isEditing ? `Edit View: ${view?.name}` : 'Create View'} />
      <ModalBody style={{ minHeight: '24rem', overflow: 'visible' }}>
        <TextInput
          id="view-name"
          labelText="View name"
          value={viewName}
          onChange={(e) => setViewName(e.target.value)}
          disabled={isDefault}
          style={{ marginBottom: 'var(--cds-spacing-05)' }}
        />
        <FilterableMultiSelect
          id="node-selector"
          titleText="Select nodes to display as columns"
          items={availableNodes}
          itemToString={(item: NodeItem) => item?.text ?? ''}
          initialSelectedItems={selectedNodes}
          onChange={({ selectedItems }: { selectedItems: NodeItem[] }) => {
            // Preserve existing order: keep items that are still selected
            // in their current position, append newly added items at the end
            const existingIds = new Set(selectedNodes.map((n) => n.id));
            const newIds = new Set(selectedItems.map((n) => n.id));
            const kept = selectedNodes.filter((n) => newIds.has(n.id));
            const added = selectedItems.filter((n) => !existingIds.has(n.id));
            setSelectedNodes([...kept, ...added]);
          }}
        />
        {selectedNodes.length > 0 && (
          <div style={{ marginTop: 'var(--cds-spacing-05)' }}>
            <div style={{ fontSize: '0.75rem', opacity: 0.7, marginBottom: 'var(--cds-spacing-02)' }}>
              Column order (drag or use arrows to reorder)
            </div>
            {selectedNodes.map((node, idx) => (
              <div
                key={node.id}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 'var(--cds-spacing-02)',
                  padding: '4px 8px',
                  marginBottom: '2px',
                  background: 'var(--cds-layer-02)',
                  borderRadius: '4px',
                  fontSize: '0.875rem',
                }}
              >
                <span style={{ opacity: 0.5, minWidth: '20px' }}>{String(idx + 1)}.</span>
                <span style={{ flex: 1 }}>{node.text}</span>
                <Button
                  kind="ghost"
                  size="sm"
                  hasIconOnly
                  renderIcon={ArrowUp}
                  iconDescription="Move up"
                  onClick={() => moveUp(idx)}
                  disabled={idx === 0}
                />
                <Button
                  kind="ghost"
                  size="sm"
                  hasIconOnly
                  renderIcon={ArrowDown}
                  iconDescription="Move down"
                  onClick={() => moveDown(idx)}
                  disabled={idx === selectedNodes.length - 1}
                />
                <Button
                  kind="ghost"
                  size="sm"
                  hasIconOnly
                  renderIcon={Close}
                  iconDescription="Remove"
                  onClick={() => removeNode(idx)}
                />
              </div>
            ))}
          </div>
        )}
        {saveError && (
          <div style={{ color: 'var(--cds-support-error)', marginTop: 'var(--cds-spacing-03)' }}>
            {saveError}
          </div>
        )}
      </ModalBody>
      <ModalFooter>
        {isEditing && !isDefault && (
          <Button kind="danger" onClick={handleDelete} disabled={deleteMutation.isPending}>
            Delete
          </Button>
        )}
        <Button kind="secondary" onClick={onClose}>
          Cancel
        </Button>
        <Button kind="primary" onClick={handleSave} disabled={!canSave || isSaving}>
          {isSaving ? 'Saving...' : 'Save'}
        </Button>
      </ModalFooter>
    </ComposedModal>
  );
};
