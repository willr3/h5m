import type { View, ViewComponent } from '@client/types.gen.ts';

import {
  DataTable,
  Dropdown,
  InlineLoading,
  SkeletonText,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@carbon/react';
import { getViewDataOptions, getViewsOptions } from '@client/@tanstack/react-query.gen.ts';
import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';

function formatCellValue(value: unknown): string {
  if (value == null) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

const ViewDataTable = ({
  folderName,
  view,
}: {
  folderName: string;
  view: View;
}) => {
  const viewId = view.id;
  const { data: rows, isLoading, isError } = useQuery(
    getViewDataOptions({
      path: { name: folderName, viewId: viewId! },
    }),
  );

  if (isLoading) return <SkeletonText paragraph={true} lineCount={5} />;
  if (isError) return <InlineLoading status="error" description="Failed to load view data" />;
  if (!rows || rows.length === 0) return <p>No data available</p>;

  const columns = (view.components ?? [])
    .sort((a: ViewComponent, b: ViewComponent) => (a.headerOrder ?? 0) - (b.headerOrder ?? 0))
    .map((c: ViewComponent) => ({
      key: c.nodeName ?? c.headerName ?? '',
      header: c.headerName ?? c.nodeName ?? '',
    }));

  const tableRows = rows.map((row: Record<string, unknown>, idx: number) => ({
    id: String(idx),
    ...Object.fromEntries(columns.map((col) => [col.key, formatCellValue(row[col.key])])),
  }));

  return (
    <DataTable rows={tableRows} headers={columns}>
      {({ rows: dataRows, headers, getTableProps, getHeaderProps, getRowProps }) => (
        <Table {...getTableProps()}>
          <TableHead>
            <TableRow>
              {headers.map((header) => (
                <TableHeader {...getHeaderProps({ header })}>
                  {header.header}
                </TableHeader>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {dataRows.map((row) => (
              <TableRow {...getRowProps({ row })}>
                {row.cells.map((cell) => (
                  <TableCell key={cell.id}>{cell.value}</TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </DataTable>
  );
};

export const DataTab = ({ folderName }: { folderName: string }) => {
  const { data: views, isLoading: viewsLoading } = useQuery(
    getViewsOptions({ path: { name: folderName } }),
  );
  const [selectedViewId, setSelectedViewId] = useState<number | null>(null);

  const selectedView = useMemo(() => {
    if (!views || views.length === 0) return null;
    if (selectedViewId != null) {
      return views.find((v: View) => v.id === selectedViewId) ?? views[0];
    }
    // Prefer the "Default" view if it has components, otherwise pick the first view with components
    const defaultView = views.find((v: View) => v.name === 'Default');
    if (defaultView && defaultView.components && defaultView.components.length > 0) {
      return defaultView;
    }
    const viewWithComponents = views.find((v: View) => v.components && v.components.length > 0);
    return viewWithComponents ?? defaultView ?? views[0];
  }, [views, selectedViewId]);

  if (viewsLoading) return <SkeletonText paragraph={true} lineCount={3} />;
  if (!views || views.length === 0) return <p>No views configured</p>;

  const dropdownItems = views.map((v: View) => ({
    id: String(v.id),
    text: v.name,
  }));

  return (
    <div>
      <div style={{ marginBottom: 'var(--cds-spacing-05)', maxWidth: '300px' }}>
        <Dropdown
          id="view-selector"
          titleText="View"
          label="Select a view"
          items={dropdownItems}
          selectedItem={selectedView ? { id: String(selectedView.id), text: selectedView.name } : undefined}
          itemToString={(item: { id: string; text: string }) => item?.text ?? ''}
          onChange={({ selectedItem }: { selectedItem: { id: string; text: string } }) => {
            setSelectedViewId(Number(selectedItem.id));
          }}
        />
      </div>
      {selectedView && (
        <ViewDataTable folderName={folderName} view={selectedView} />
      )}
    </div>
  );
};
