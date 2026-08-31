import { AuthActions } from '@app/layout/AuthActions.tsx';
import { DOCS_IFRAME_STYLE, docsIframeSrc } from '@app/pages/SitePage';
import { Help } from '@carbon/icons-react';
import {
  Content,
  ErrorBoundary,
  Header,
  HeaderGlobalAction,
  HeaderGlobalBar,
  HeaderMenuButton,
  HeaderName,
  InlineLoading,
  SideNav,
  SideNavDivider,
  SideNavItems,
  SideNavMenu,
  SideNavMenuItem,
  SideNavLink,
  SkeletonText,
  SkipToContent,
  Theme,
} from '@carbon/react';
import { listFoldersOptions, listTeamsOptions } from '@client/@tanstack/react-query.gen.ts';
import { useQuery, useSuspenseQuery } from '@tanstack/react-query';
import { ReactNode, Suspense, useCallback, useState } from 'react';
import { Link, Outlet, useParams } from 'react-router-dom';

const NavFolders = () => {
  const { data: folders } = useSuspenseQuery(listFoldersOptions());
  const { data: teams = [] } = useQuery(listTeamsOptions());
  const { folderId } = useParams<{ folderId: string }>();
  const activeId = Number(folderId);

  const teamsWithFolders = teams
      .map((team) => ({
        ...team,
        folders: folders.filter((folder) => folder.teamId === team.id),
      }))
      .filter((team) => team.folders.length > 0);

    const ungroupedFolders = folders.filter((folder) => !folder.teamId);

  return (
      <SideNavItems>
        {teamsWithFolders.map((team) => (
          <SideNavMenu key={team.id} title={team.name ?? '?'}>
            {team.folders.map((folder) => (
              <SideNavMenuItem key={folder.id} as={Link} to={`/folder/${String(folder.id)}`} isActive={folder.id === activeId}>
                {folder.name}
              </SideNavMenuItem>
            ))}
          </SideNavMenu>
        ))}
        {ungroupedFolders.length > 0 && <SideNavDivider />}
        {ungroupedFolders.map((folder) => (
          <SideNavLink key={folder.id} as={Link} to={`/folder/${String(folder.id)}`} isActive={folder.id === activeId}>
            {folder.name}
          </SideNavLink>
        ))}
      </SideNavItems>
    );
};

export const AppHeader = ({ children }: { children?: ReactNode }) => {
  const [sideNavOpen, setSideNavOpen] = useState(false);
  const [docsOpen, setDocsOpen] = useState(false);
  const toggleSideNav = useCallback(() => {
    setSideNavOpen((prev) => !prev);
  }, []);
  const toggleDocs = useCallback(() => {
    setDocsOpen((prev) => !prev);
  }, []);
  return (
    <>
      <Theme theme="g100">
        <Header aria-label="Carbon App">
          <SkipToContent />
          <HeaderMenuButton aria-label="Hamburger menu" onClick={toggleSideNav} isActive={sideNavOpen} isCollapsible={true} />
          <HeaderName as={Link} to="/" prefix="h5m">
            Horreum
          </HeaderName>
          <HeaderGlobalBar>
            <HeaderGlobalAction aria-label="Documentation" onClick={toggleDocs} isActive={docsOpen} tooltipAlignment="end">
              <Help size={24} />
            </HeaderGlobalAction>
            <AuthActions />
          </HeaderGlobalBar>
        </Header>
        <SideNav aria-label="Side navigation" expanded={sideNavOpen} isPersistent={false} isFixedNav={false}>
          <ErrorBoundary
            fallback={
              <div style={{ padding: 'var(--cds-spacing-05)' }}>
                <InlineLoading status="error" description="Folder load failed" />
              </div>
            }
          >
            <Suspense
              fallback={
                <div style={{ padding: 'var(--cds-spacing-05)' }}>
                  <SkeletonText paragraph={true} lineCount={50} />
                </div>
              }
            >
              <NavFolders />
            </Suspense>
          </ErrorBoundary>
        </SideNav>
      </Theme>
      {children ?? (
        <>
          <Content>
            <Outlet />
          </Content>
          <iframe
            src={docsIframeSrc()}
            title="Documentation"
            style={{
              ...DOCS_IFRAME_STYLE,
              display: docsOpen ? 'block' : 'none',
              position: 'fixed',
              top: '3rem',
              zIndex: 8000,
            }}
          />
        </>
      )}
    </>
  );
};
