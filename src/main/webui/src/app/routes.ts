import { AppHeader } from '@app/layout/AppHeader';
import { FolderPage } from '@app/pages/FolderPage';
import { createBrowserRouter } from 'react-router-dom';

const router = createBrowserRouter([
  {
    Component: AppHeader,
    path: '/',
    children: [
      {
        Component: FolderPage,
        path: 'folder/:folderId',
      },
    ],
  },
]);

export default router;
