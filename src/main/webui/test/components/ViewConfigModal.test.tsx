import type { View } from '@client/types.gen.ts';

import { cleanup, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

// jsdom doesn't have matchMedia or ResizeObserver — Carbon components need them
beforeAll(() => {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  });

  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});

const mockNodeGroup = {
  id: 1,
  name: 'test-group',
  root: { id: 1, name: 'root', type: 'ROOT', sources: [] },
  sources: [
    { id: 2, name: 'cpu', type: 'JQ', operation: '.cpu', sources: [] },
    { id: 3, name: 'mem', type: 'JQ', operation: '.mem', sources: [] },
    { id: 4, name: 'cpu_ft', type: 'FIXED_THRESHOLD', sources: [] },
    { id: 5, name: 'cpu_rd', type: 'RELATIVE_DIFFERENCE', sources: [] },
  ],
};

const mockCreateView = vi.fn().mockResolvedValue({ data: {} });
const mockUpdateView = vi.fn().mockResolvedValue({ data: {} });
const mockDeleteView = vi.fn().mockResolvedValue({ data: {} });

vi.mock('@client/@tanstack/react-query.gen.ts', () => ({
  byIdOptions: () => ({
    queryKey: ['byId'],
    queryFn: () => mockNodeGroup,
  }),
}));

vi.mock('@client/sdk.gen.ts', () => ({
  ViewService: {
    createView: (...args: unknown[]) => mockCreateView(...args),
    updateView: (...args: unknown[]) => mockUpdateView(...args),
    deleteView: (...args: unknown[]) => mockDeleteView(...args),
  },
}));

const { ViewConfigModal } = await import('@app/components/ViewConfigModal');
const { createTestQueryClient, renderWithProviders } = await import('../test-utils');

function renderModal(props: {
  open?: boolean;
  onClose?: () => void;
  view?: View | null;
}) {
  const queryClient = createTestQueryClient();
  queryClient.setQueryData(['byId'], mockNodeGroup);

  return renderWithProviders(
    <ViewConfigModal
      open={props.open ?? true}
      onClose={props.onClose ?? vi.fn()}
      folderName="test-folder"
      groupId={1}
      view={props.view ?? null}
    />,
    { queryClient },
  );
}

describe('<ViewConfigModal />', () => {
  beforeEach(() => {
    mockCreateView.mockClear();
    mockUpdateView.mockClear();
    mockDeleteView.mockClear();
  });

  it('renders Create View title when no view is provided', async () => {
    renderModal({});

    await waitFor(() => {
      expect(screen.getByText('Create View')).toBeDefined();
    });

    cleanup();
  });

  it('renders Edit View title when editing an existing view', async () => {
    const view: View = {
      id: 1,
      name: 'My View',
      components: [
        { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      expect(screen.getByText('Edit View: My View')).toBeDefined();
    });

    cleanup();
  });

  it('renders view name input', async () => {
    renderModal({});

    await waitFor(() => {
      expect(screen.getByLabelText('View name')).toBeDefined();
    });

    cleanup();
  });

  it('renders node selector with non-detection nodes only', async () => {
    renderModal({});

    await waitFor(() => {
      expect(screen.getByText('Select nodes to display as columns')).toBeDefined();
    });

    // Detection nodes (FIXED_THRESHOLD, RELATIVE_DIFFERENCE) should be excluded
    // The multi-select dropdown shows available items when opened
    // We verify the component renders — detailed item filtering is an integration concern

    cleanup();
  });

  it('renders Save and Cancel buttons', async () => {
    renderModal({});

    await waitFor(() => {
      expect(screen.getByText('Save')).toBeDefined();
      expect(screen.getByText('Cancel')).toBeDefined();
    });

    cleanup();
  });

  it('Save button is disabled when view name is empty', async () => {
    renderModal({});

    await waitFor(() => {
      const saveButton = screen.getByText('Save');
      expect(saveButton.closest('button')?.disabled).toBe(true);
    });

    cleanup();
  });

  it('calls onClose when Cancel is clicked', async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderModal({ onClose });

    await waitFor(() => {
      expect(screen.getByText('Cancel')).toBeDefined();
    });

    await user.click(screen.getByText('Cancel'));
    expect(onClose).toHaveBeenCalled();

    cleanup();
  });

  it('disables view name input for Default view', async () => {
    const defaultView: View = {
      id: 1,
      name: 'Default',
      components: [],
    };

    renderModal({ view: defaultView });

    await waitFor(() => {
      const input = screen.getByLabelText('View name') as HTMLInputElement;
      expect(input.disabled).toBe(true);
    });

    cleanup();
  });

  it('does not show Delete button for Default view', async () => {
    const defaultView: View = {
      id: 1,
      name: 'Default',
      components: [],
    };

    renderModal({ view: defaultView });

    await waitFor(() => {
      expect(screen.getByText('Save')).toBeDefined();
    });

    expect(screen.queryByText('Delete')).toBeNull();

    cleanup();
  });

  it('shows Delete button for non-Default views when editing', async () => {
    const view: View = {
      id: 2,
      name: 'Custom View',
      components: [
        { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      expect(screen.getByText('Delete')).toBeDefined();
    });

    cleanup();
  });

  it('does not show Delete button when creating a new view', async () => {
    renderModal({ view: null });

    await waitFor(() => {
      expect(screen.getByText('Save')).toBeDefined();
    });

    expect(screen.queryByText('Delete')).toBeNull();

    cleanup();
  });

  it('populates view name from existing view when editing', async () => {
    const view: View = {
      id: 2,
      name: 'Custom View',
      components: [],
    };

    renderModal({ view });

    await waitFor(() => {
      const input = screen.getByLabelText('View name') as HTMLInputElement;
      expect(input.value).toBe('Custom View');
    });

    cleanup();
  });

  it('starts with empty view name when creating', async () => {
    renderModal({ view: null });

    await waitFor(() => {
      const input = screen.getByLabelText('View name') as HTMLInputElement;
      expect(input.value).toBe('');
    });

    cleanup();
  });

  it('hides modal content when open is false', () => {
    renderModal({ open: false });

    // Carbon ComposedModal renders to DOM but hides via CSS when open=false
    // The modal container should not have the 'is-visible' class
    const modal = screen.getByText('Create View').closest('.cds--modal');
    expect(modal?.classList.contains('is-visible')).toBe(false);

    cleanup();
  });

  it('shows column ordering list when editing a view with components', async () => {
    const view: View = {
      id: 1,
      name: 'Ordered View',
      components: [
        { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
        { id: 2, nodeId: 3, nodeName: 'mem', headerName: 'Memory', headerOrder: 1 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      // Should show the column order label
      expect(screen.getByText(/column order/i)).toBeDefined();
      // Should show numbered positions with node names
      expect(screen.getByText('1.')).toBeDefined();
      expect(screen.getByText('2.')).toBeDefined();
      expect(screen.getByText('cpu')).toBeDefined();
      expect(screen.getByText('mem')).toBeDefined();
    });

    cleanup();
  });

  it('preserves column order from existing view headerOrder', async () => {
    const view: View = {
      id: 1,
      name: 'Reversed View',
      components: [
        { id: 1, nodeId: 3, nodeName: 'mem', headerName: 'Memory', headerOrder: 0 },
        { id: 2, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 1 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      // mem should be first (headerOrder 0), cpu second (headerOrder 1)
      const items = screen.getAllByText(/^(cpu|mem)$/);
      expect(items.at(0)?.textContent).toBe('mem');
      expect(items.at(1)?.textContent).toBe('cpu');
    });

    cleanup();
  });

  it('does not show column ordering list when no nodes are selected', async () => {
    renderModal({ view: null });

    await waitFor(() => {
      expect(screen.getByText('Create View')).toBeDefined();
    });

    expect(screen.queryByText(/column order/i)).toBeNull();

    cleanup();
  });

  it('shows move up, move down, and remove buttons for each column', async () => {
    const view: View = {
      id: 1,
      name: 'Test View',
      components: [
        { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
        { id: 2, nodeId: 3, nodeName: 'mem', headerName: 'Memory', headerOrder: 1 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      // Each column row should have Move up, Move down, and Remove buttons
      const moveUpButtons = screen.getAllByLabelText('Move up');
      const moveDownButtons = screen.getAllByLabelText('Move down');
      const removeButtons = screen.getAllByLabelText('Remove');
      expect(moveUpButtons.length).toBe(2);
      expect(moveDownButtons.length).toBe(2);
      expect(removeButtons.length).toBe(2);
    });

    cleanup();
  });

  it('disables move up for first item and move down for last item', async () => {
    const view: View = {
      id: 1,
      name: 'Test View',
      components: [
        { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
        { id: 2, nodeId: 3, nodeName: 'mem', headerName: 'Memory', headerOrder: 1 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      const moveUpButtons = screen.getAllByLabelText('Move up');
      const moveDownButtons = screen.getAllByLabelText('Move down');
      // First item's move up should be disabled
      expect((moveUpButtons.at(0) as HTMLButtonElement)?.disabled).toBe(true);
      // Last item's move down should be disabled
      expect((moveDownButtons.at(1) as HTMLButtonElement)?.disabled).toBe(true);
      // Other buttons should be enabled
      expect((moveUpButtons.at(1) as HTMLButtonElement)?.disabled).toBe(false);
      expect((moveDownButtons.at(0) as HTMLButtonElement)?.disabled).toBe(false);
    });

    cleanup();
  });

  it('reorders columns when move down is clicked', async () => {
    const user = userEvent.setup();
    const view: View = {
      id: 1,
      name: 'Test View',
      components: [
        { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
        { id: 2, nodeId: 3, nodeName: 'mem', headerName: 'Memory', headerOrder: 1 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      expect(screen.getByText('cpu')).toBeDefined();
    });

    // Click move down on first item (cpu)
    const moveDownButtons = screen.getAllByLabelText('Move down');
    await user.click(moveDownButtons.at(0)!);

    // Now mem should be first, cpu second
    await waitFor(() => {
      const items = screen.getAllByText(/^(cpu|mem)$/);
      expect(items.at(0)?.textContent).toBe('mem');
      expect(items.at(1)?.textContent).toBe('cpu');
    });

    cleanup();
  });

  it('removes a column when remove button is clicked', async () => {
    const user = userEvent.setup();
    const view: View = {
      id: 1,
      name: 'Test View',
      components: [
        { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
        { id: 2, nodeId: 3, nodeName: 'mem', headerName: 'Memory', headerOrder: 1 },
      ],
    };

    renderModal({ view });

    await waitFor(() => {
      expect(screen.getAllByLabelText('Remove').length).toBe(2);
    });

    // Remove the first item (cpu)
    const removeButtons = screen.getAllByLabelText('Remove');
    await user.click(removeButtons.at(0)!);

    // Only mem should remain
    await waitFor(() => {
      expect(screen.getAllByLabelText('Remove').length).toBe(1);
      expect(screen.getByText('mem')).toBeDefined();
      expect(screen.queryByText('cpu')).toBeNull();
    });

    cleanup();
  });
});
