import type { Node as ApiNode } from '@client/types.gen.ts';

import { DataTab } from '@app/components/DataTab';
import { NodeGraphVisualizer } from '@app/components/NodeGraphVisualizer';
import { useState } from 'react';
import {
  Button,
  ErrorBoundary,
  InlineLoading,
  InlineNotification,
  MenuButton,
  MenuItem,
  SkeletonText,
  StructuredListBody,
  StructuredListCell,
  StructuredListHead,
  StructuredListRow,
  StructuredListWrapper,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Tag,
} from '@carbon/react';
import { byIdOptions, getRecalculationStatusOptions, listFoldersOptions } from '@client/@tanstack/react-query.gen.ts';
import { useQuery, useSuspenseQuery } from '@tanstack/react-query';
import { Suspense, useCallback, useEffect } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import '@app/pages/DashboardPage.css';
import { CreateNodeModal } from '@app/components/CreateNodeModal';
import { DeleteNodeModal } from '@app/components/DeleteNodeModal';
import { EditNodeModal } from '@app/components/EditNodeModal';

const NodesTab = ({ groupId }: { groupId: number }) => {
  const { data: nodeGroup } = useSuspenseQuery(byIdOptions({ path: { id: groupId } }));
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [nodeToDelete, setNodeToDelete] = useState<ApiNode | null>(null);
  const [nodeToEdit, setNodeToEdit] = useState<ApiNode | null>(null);
  const [recalculationId, setRecalculationId] = useState<string | null>(null);

  const { data: recalcStatus } = useQuery({
    ...getRecalculationStatusOptions({ path: { id: recalculationId ?? '' } }),
    enabled: recalculationId !== null,
    refetchInterval: (query) => query.state.data?.state === 'RUNNING' ? 2000 : false,
  });

  useEffect(() => {
    if (recalcStatus?.state === 'COMPLETED') {
      const timer = setTimeout(() => setRecalculationId(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [recalcStatus?.state]);

  return (
    <>
      <Button
          kind="primary"
          size="md"
          onClick={() => setIsCreateOpen(true)}
          className="create-folder-btn"
          style={{ margin: 'var(--cds-spacing-05)' }}>
          Create Node
      </Button>
      {recalcStatus?.state === 'RUNNING' && (
        <div style={{ margin: '0 var(--cds-spacing-05)' }}>
          <InlineLoading
            description={`Recalculating… ${recalcStatus.completedRoots ?? 0}/${recalcStatus.totalRoots ?? 0} values`}
            status="active"
          />
        </div>
      )}
      {recalcStatus?.state === 'COMPLETED' && (
        <InlineNotification
          kind="success"
          title="Recalculation complete"
          subtitle={`Finished in ${recalcStatus.durationMs ?? 0}ms`}
          onClose={() => setRecalculationId(null)}
          style={{ margin: '0 var(--cds-spacing-05)', maxWidth: 'none' }}
        />
      )}
      {recalcStatus?.state === 'FAILED' && (
        <InlineNotification
          kind="error"
          title="Recalculation failed"
          subtitle={recalcStatus.error ?? 'Unknown error'}
          onClose={() => setRecalculationId(null)}
          style={{ margin: '0 var(--cds-spacing-05)', maxWidth: 'none' }}
        />
      )}
      <CreateNodeModal open={isCreateOpen} onClose={() => setIsCreateOpen(false)} groupId={groupId} />
      <DeleteNodeModal node={nodeToDelete} onClose={() => setNodeToDelete(null)} />
      <EditNodeModal node={nodeToEdit} onClose={() => setNodeToEdit(null)} onRecalculation={setRecalculationId} />
      {nodeGroup.sources?.length === 0 ? (
        <p style={{ margin: 'var(--cds-spacing-05)' }}>No nodes defined yet. Use the Create Node button above to get started.</p>
      ) : (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>Name</StructuredListCell>
              <StructuredListCell head>Type</StructuredListCell>
              <StructuredListCell head>FQDN</StructuredListCell>
              <StructuredListCell head>Operation</StructuredListCell>
              <StructuredListCell head />
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {nodeGroup.sources?.map((node: ApiNode) => (
              <StructuredListRow key={node.id}>
                <StructuredListCell>{node.name}</StructuredListCell>
                <StructuredListCell>
                  <Tag size="sm">{node.type}</Tag>
                </StructuredListCell>
                <StructuredListCell>{node.fqdn}</StructuredListCell>
                <StructuredListCell>{node.operation}</StructuredListCell>
                <StructuredListCell>
                  <MenuButton label="Action" kind="ghost" size="sm" menuAlignment="bottom-end">
                    <MenuItem label="Delete" kind="danger" onClick={() => setNodeToDelete(node)} />
                    <MenuItem label="Edit" onClick={() => setNodeToEdit(node)} />
                  </MenuButton>
                </StructuredListCell>
              </StructuredListRow>
            ))}
          </StructuredListBody>
        </StructuredListWrapper>
      )}
    </>
  );
};

const GraphVisualizer = ({ groupId }: { groupId: number }) => {
  const { data: nodeGroup } = useSuspenseQuery(byIdOptions({ path: { id: groupId } }));
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  return (
    <>
      <Button
        kind="primary"
        size="md"
        onClick={() => setIsCreateOpen(true)}
        className="create-folder-btn"
        style={{ margin: 'var(--cds-spacing-05)' }}
      >
        Create Node
      </Button>
      <CreateNodeModal open={isCreateOpen} onClose={() => setIsCreateOpen(false)} groupId={groupId} />
      <NodeGraphVisualizer nodeGroup={nodeGroup} />
    </>
  );
};

const TAB_ANCHORS = ['data', 'nodes', 'graph'];

const FolderContent = ({ folderId }: { folderId: number }) => {
  const { data: folders } = useSuspenseQuery(listFoldersOptions());
  const folder = folders.find((f) => f.id === folderId);
  const navigate = useNavigate();
  const location = useLocation();
  const selectedIndex = Math.max(0, TAB_ANCHORS.indexOf(location.hash.slice(1)));
  const onTabChange = useCallback(({ selectedIndex: i }: { selectedIndex: number }) => {
    void navigate({ hash: TAB_ANCHORS[i] }, { replace: true });
  }, [navigate]);
  if (!folder) {
    return <InlineLoading status="error" description="Folder not found" />;
  }
  return (
    <Tabs selectedIndex={selectedIndex} onChange={onTabChange}>
      <TabList aria-label="Folder tabs">
        <Tab>Data</Tab>
        <Tab>Nodes</Tab>
        <Tab>Graph</Tab>
      </TabList>
      <TabPanels>
        <TabPanel>
          {folder.name && folder.groupId != null ? (
            <DataTab folderName={folder.name} groupId={folder.groupId} />
          ) : (
            <p>Folder name not available</p>
          )}
        </TabPanel>
        <TabPanel>
          {folder.groupId != null ? (
            <ErrorBoundary fallback={<InlineLoading status="error" description="Failed to load nodes" />}>
              <Suspense fallback={<SkeletonText paragraph={true} lineCount={5} />}>
                <NodesTab groupId={folder.groupId} />
              </Suspense>
            </ErrorBoundary>
          ) : (
            <p>No node group associated with this folder</p>
          )}
        </TabPanel>
        <TabPanel>
          {folder.groupId != null ? (
            <ErrorBoundary fallback={<InlineLoading status="error" description="Failed to load nodes" />}>
              <Suspense fallback={<SkeletonText paragraph={true} lineCount={5} />}>
                <GraphVisualizer groupId={folder.groupId} />
              </Suspense>
            </ErrorBoundary>
          ) : (
            <p>No node group associated with this folder</p>
          )}
        </TabPanel>
      </TabPanels>
    </Tabs>
  );
};

export const FolderPage = () => {
  const { folderId } = useParams<{ folderId: string }>();
  const id = Number(folderId);
  if (!folderId || isNaN(id)) {
    return null;
  }
  return (
    <div style={{ padding: 'var(--cds-spacing-05)', marginTop: 'var(--cds-spacing-09)' }}>
      <ErrorBoundary fallback={<InlineLoading status="error" description="Failed to load folder" />}>
        <Suspense fallback={<SkeletonText paragraph={true} lineCount={5} />}>
          <FolderContent folderId={id} />
        </Suspense>
      </ErrorBoundary>
    </div>
  );
};
