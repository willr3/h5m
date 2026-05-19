import type { Node as ApiNode } from '@client/types.gen.ts';

import { NodeGraphVisualizer } from '@app/components/NodeGraphVisualizer';
import {
  ErrorBoundary,
  InlineLoading,
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
import { byIdOptions, listFoldersOptions } from '@client/@tanstack/react-query.gen.ts';
import { useSuspenseQuery } from '@tanstack/react-query';
import { Suspense, useCallback } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';

const NodesTab = ({ groupId }: { groupId: number }) => {
  const { data: nodeGroup } = useSuspenseQuery(byIdOptions({ path: { id: groupId } }));
  if (nodeGroup.sources?.length === 0) {
    return <p>No nodes defined</p>;
  }
  return (
    <StructuredListWrapper>
      <StructuredListHead>
        <StructuredListRow head>
          <StructuredListCell head>Name</StructuredListCell>
          <StructuredListCell head>Type</StructuredListCell>
          <StructuredListCell head>FQDN</StructuredListCell>
          <StructuredListCell head>Operation</StructuredListCell>
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
          </StructuredListRow>
        ))}
      </StructuredListBody>
    </StructuredListWrapper>
  );
};

const GraphVisualizer = ({ groupId }: { groupId: number }) => {
  const { data: nodeGroup } = useSuspenseQuery(byIdOptions({ path: { id: groupId } }));
  return <NodeGraphVisualizer nodeGroup={nodeGroup} />;
};

const TAB_ANCHORS = ['nodes', 'graph'];

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
        <Tab>Nodes</Tab>
        <Tab>Graph</Tab>
      </TabList>
      <TabPanels>
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
