import {
  ErrorBoundary,
  Header,
  HeaderGlobalBar,
  HeaderMenuButton,
  HeaderName,
  InlineLoading,
  SideNav,
  SideNavItems,
  SideNavLink,
  SkeletonText,
  SkipToContent,
  Theme,
} from '@carbon/react';
import { listFoldersOptions } from '@client/@tanstack/react-query.gen.ts';
import { useSuspenseQuery } from '@tanstack/react-query';
import { Suspense, useCallback, useState } from 'react';
import { Outlet, useParams } from 'react-router-dom';

const NavFolders = () => {
  const { data: folders } = useSuspenseQuery(listFoldersOptions());
  const { folderId } = useParams<{ folderId: string }>();
  const activeId = Number(folderId);
  return (
    <SideNavItems>
      {folders.map((folder) => (
        <SideNavLink key={folder.id} href={`/folder/${String(folder.id)}`} isActive={folder.id === activeId}>
          {folder.name}
        </SideNavLink>
      ))}
    </SideNavItems>
  );
};

export const AppHeader = () => {
  const { folderId } = useParams<{ folderId: string }>();
  const [sideNavOpen, setSideNavOpen] = useState(!folderId);
  const toggleSideNav = useCallback(() => {
    setSideNavOpen((prev) => !prev);
  }, []);
  return (
    <>
      <Theme theme="g100">
        <Header aria-label="Carbon App">
          <SkipToContent />
          <HeaderMenuButton aria-label="Hamburger menu" onClick={toggleSideNav} isActive={sideNavOpen} isCollapsible={true} />
          <HeaderName prefix="h5m">Horreum</HeaderName>
          <HeaderGlobalBar />
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
      <Outlet />
    </>
  );
};
