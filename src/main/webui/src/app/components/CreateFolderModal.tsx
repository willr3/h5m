import { extractErrorMessage } from '@app/context/NotificationProvider.tsx';
import { fieldError } from '@app/validation.ts';
import { Button, ComposedModal, Form, InlineNotification, ModalBody, ModalFooter, ModalHeader, Select, SelectItem, Stack, TextInput } from '@carbon/react';
import { createFolderMutation } from '@client/@tanstack/react-query.gen.ts';
import { zCreateFolderBody } from '@client/zod.gen.ts';
import { useForm } from '@tanstack/react-form';
 import { useTeams } from '@app/context/useTeams.tsx';
import {  useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';

interface CreateFolderModalProps {
  open: boolean;
  onClose: () => void;
}

export const CreateFolderModal = ({ open, onClose }: CreateFolderModalProps) => {
  const [submitError, setSubmitError] = useState<string | null>(null);
  const queryClient = useQueryClient();
  const teams = useTeams();

  const createFolder = useMutation({
    ...createFolderMutation(),
    onSuccess: () => {
      void queryClient.invalidateQueries();
      handleClose();
    },
    onError: (e) => {
      setSubmitError(extractErrorMessage(e) ?? 'Failed to create folder');
    },
  });

  const form = useForm({
    defaultValues: { name: '', teams: '' },
    onSubmit: ({ value }) => {
      setSubmitError(null);
      const teamId = value.teams ? Number(value.teams) : undefined;
      createFolder.mutate({ body: { name: value.name.trim(), teamId } });
    },
  });

  const handleClose = () => {
    form.reset();
    setSubmitError(null);
    onClose();
  };

  return (
    <ComposedModal open={open} onClose={handleClose}>
      <ModalHeader title="Create Folder" />
      <ModalBody>
        <Form
          onSubmit={(e) => {
            e.preventDefault();
          }}
        >
          <Stack gap={6}>
            <form.Field
              name="name"
              validators={{
                onBlur: zCreateFolderBody.shape.name,
                onSubmit: zCreateFolderBody.shape.name,
              }}
            >
              {(field) => (
                <TextInput
                  id="folder-name"
                  labelText="Folder name"
                  placeholder="e.g. benchmarks"
                  value={field.state.value}
                  onChange={(e) => {
                    field.handleChange(e.target.value);
                  }}
                  onBlur={field.handleBlur}
                  invalid={field.state.meta.errors.length > 0}
                  invalidText={fieldError(field.state.meta.errors)}
                />
              )}
            </form.Field>
            <form.Field name= "teams" >
            {(field)=>(
              <Select
                  id="folder-team"
                  labelText = "Team"
                  value ={field.state.value}
                  onChange={(e)=>field.handleChange(e.target.value)}
              >
                <SelectItem value="" text="Select a team" />
                 {teams.map((t) => (
                   <SelectItem key={t.id} value={String(t.id)} text={t.name ?? '?'} />
                 ))}
              </Select>
              )}
            </form.Field>

            {submitError && (
              <InlineNotification
                kind="error"
                lowContrast
                title="Failed to create folder"
                subtitle={submitError}
                onCloseButtonClick={() => {
                  setSubmitError(null);
                }}
              />
            )}
          </Stack>
        </Form>
      </ModalBody>
      <ModalFooter>
        <Button kind="secondary" onClick={handleClose}>
          Cancel
        </Button>
        <Button
          kind="primary"
          disabled={createFolder.isPending}
          onClick={() => {
            void form.handleSubmit();
          }}
        >
          {createFolder.isPending ? 'Saving...' : 'Save'}
        </Button>
      </ModalFooter>
    </ComposedModal>
  );
};
