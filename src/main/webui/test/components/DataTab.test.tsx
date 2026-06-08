import type { View } from '@client/types.gen.ts';

import { cleanup, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeAll, describe, expect, it, vi } from 'vitest';

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

const mockViews: View[] = [
  {
    id: 1,
    name: 'Default',
    folderId: 1,
    components: [
      { id: 1, nodeId: 2, nodeName: 'cpu', headerName: 'CPU', headerOrder: 0 },
      { id: 2, nodeId: 3, nodeName: 'mem', headerName: 'Memory', headerOrder: 1 },
    ],
  },
  {
    id: 2,
    name: 'Summary',
    folderId: 1,
    components: [
      { id: 3, nodeId: 2, nodeName: 'cpu', headerName: 'CPU Usage', headerOrder: 0 },
    ],
  },
];

const emptyDefaultView: View = {
  id: 1,
  name: 'Default',
  folderId: 1,
  components: [],
};

const mockViewData = [
  { cpu: 42, mem: 1024 },
  { cpu: 55, mem: 2048 },
];

const mockNodeGroup = {
  id: 1,
  name: 'test-group',
  root: { id: 1, name: 'root', type: 'ROOT', sources: [] },
  sources: [
    { id: 2, name: 'cpu', type: 'JQ', operation: '.cpu', sources: [] },
    { id: 3, name: 'mem', type: 'JQ', operation: '.mem', sources: [] },
  ],
};

vi.mock('@client/@tanstack/react-query.gen.ts', () => ({
  getViewsOptions: () => ({
    queryKey: ['getViews'],
    queryFn: () => mockViews,
  }),
  getViewDataOptions: () => ({
    queryKey: ['getViewData'],
    queryFn: () => mockViewData,
  }),
  byIdOptions: () => ({
    queryKey: ['byId'],
    queryFn: () => mockNodeGroup,
  }),
}));

vi.mock('@client/sdk.gen.ts', () => ({
  ViewService: {
    createView: vi.fn().mockResolvedValue({ data: {} }),
    updateView: vi.fn().mockResolvedValue({ data: {} }),
    deleteView: vi.fn().mockResolvedValue({ data: {} }),
  },
}));

const { DataTab } = await import('@app/components/DataTab');
const { createTestQueryClient, renderWithProviders } = await import('../test-utils');

function renderDataTab(views: View[] = mockViews, viewData: unknown[] = mockViewData) {
  const queryClient = createTestQueryClient();
  queryClient.setQueryData(['getViews'], views);
  queryClient.setQueryData(['getViewData'], viewData);
  queryClient.setQueryData(['byId'], mockNodeGroup);

  return renderWithProviders(
    <DataTab folderName="test-folder" groupId={1} />,
    { queryClient },
  );
}

describe('<DataTab />', () => {
  it('renders view selector dropdown with view names', async () => {
    renderDataTab();

    await waitFor(() => {
      expect(screen.getByText('Default')).toBeDefined();
    });

    cleanup();
  });

  it('renders Configure and New View buttons', async () => {
    renderDataTab();

    await waitFor(() => {
      expect(screen.getByText('Configure')).toBeDefined();
      expect(screen.getByText('New View')).toBeDefined();
    });

    cleanup();
  });

  it('renders data table with column headers from view components', async () => {
    renderDataTab();

    await waitFor(() => {
      expect(screen.getByText('CPU')).toBeDefined();
      expect(screen.getByText('Memory')).toBeDefined();
    });

    cleanup();
  });

  it('renders data table with cell values', async () => {
    renderDataTab();

    await waitFor(() => {
      expect(screen.getByText('42')).toBeDefined();
      expect(screen.getByText('1024')).toBeDefined();
      expect(screen.getByText('55')).toBeDefined();
      expect(screen.getByText('2048')).toBeDefined();
    });

    cleanup();
  });

  it('shows "No views configured" when views list is empty', async () => {
    renderDataTab([]);

    await waitFor(() => {
      expect(screen.getByText('No views configured')).toBeDefined();
    });

    cleanup();
  });

  it('shows "No data available" when view data is empty', async () => {
    renderDataTab(mockViews, []);

    await waitFor(() => {
      expect(screen.getByText('No data available')).toBeDefined();
    });

    cleanup();
  });

  it('prefers Default view with components on initial render', async () => {
    renderDataTab();

    // Default view should be selected, showing its columns
    await waitFor(() => {
      expect(screen.getByText('CPU')).toBeDefined();
      expect(screen.getByText('Memory')).toBeDefined();
    });

    cleanup();
  });

  it('falls back to first view with components when Default has none', async () => {
    const views: View[] = [
      emptyDefaultView,
      {
        id: 2,
        name: 'Summary',
        folderId: 1,
        components: [
          { id: 3, nodeId: 2, nodeName: 'cpu', headerName: 'CPU Usage', headerOrder: 0 },
        ],
      },
    ];

    renderDataTab(views);

    await waitFor(() => {
      expect(screen.getByText('CPU Usage')).toBeDefined();
    });

    cleanup();
  });

  it('opens config modal when Configure button is clicked', async () => {
    const user = userEvent.setup();
    renderDataTab();

    await waitFor(() => {
      expect(screen.getByText('Configure')).toBeDefined();
    });

    await user.click(screen.getByText('Configure'));

    await waitFor(() => {
      expect(screen.getByText('Edit View: Default')).toBeDefined();
    });

    cleanup();
  });

  it('shows configure prompt when view has no columns', async () => {
    const views: View[] = [emptyDefaultView];

    renderDataTab(views);

    await waitFor(() => {
      expect(screen.getByText(/no columns configured/i)).toBeDefined();
    });

    cleanup();
  });

  it('opens config modal in create mode when New View is clicked', async () => {
    const user = userEvent.setup();
    renderDataTab();

    await waitFor(() => {
      expect(screen.getByText('New View')).toBeDefined();
    });

    await user.click(screen.getByText('New View'));

    await waitFor(() => {
      expect(screen.getByText('Create View')).toBeDefined();
    });

    cleanup();
  });
});
