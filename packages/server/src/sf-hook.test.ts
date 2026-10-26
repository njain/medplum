// SPDX-License-Identifier: Apache-2.0
// StrideForward registration hook: the host additions modules rely on (first-module brief §6; plan item 1.9).
import { isNotFound, OperationOutcomeError, Operator } from '@medplum/core';
import type { WithId } from '@medplum/core';
import type { Patient, Project } from '@medplum/fhirtypes';
import type * as BullMQ from 'bullmq';
import { Worker } from 'bullmq';
import { Router } from 'express';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Mock } from 'vitest';
import { initAppServices, shutdownApp } from './app';
import { loadTestConfig } from './config/loader';
import type { Repository } from './fhir/repo';
import { repoAccess } from './fhir/repository/access-tracker';
import type { StrideForwardHost } from './sf-hook';
import { installScheduler, registerStrideForwardModules } from './sf-hook';
import { createTestProject, withTestContext } from './test.setup';
import { queueRegistry } from './workers/utils';

describe('StrideForward hook', () => {
  let host: StrideForwardHost;
  let projectA: WithId<Project>;
  let projectB: WithId<Project>;
  let repoA: Repository;

  beforeAll(async () => {
    const config = await loadTestConfig();
    await initAppServices(config);
    // A module file that hands its host to the test.
    const dir = mkdtempSync(join(tmpdir(), 'sf-hook-'));
    const entry = join(dir, 'capture.mjs');
    writeFileSync(entry, "export const name = 'capture';\nexport function register(host) { globalThis.__sfHost = host; }\n");
    process.env.SF_MODULES = entry;
    await registerStrideForwardModules(Router());
    host = (globalThis as unknown as { __sfHost: StrideForwardHost }).__sfHost;
    const a = await createTestProject({ withRepo: true });
    projectA = a.project;
    repoA = a.repo;
    projectB = (await createTestProject()).project;
  });

  afterAll(async () => {
    delete process.env.SF_MODULES;
    await shutdownApp();
  });

  describe('withTransaction', () => {
    test('a row-security refusal (42501) is a 403, without the database message', async () => {
      const err = await withTestContext(() =>
        host
          .withTransaction(
            repoA,
            (tx) =>
              tx
                .getDatabaseClient(repoAccess.sqlWrite(['Patient'], { source: 'sf-hook.test' }))
                .query("DO $$ BEGIN RAISE EXCEPTION 'row secret detail' USING ERRCODE = '42501'; END $$"),
            { resourceTypes: ['Patient'], source: 'sf-hook.test' }
          )
          .catch((e: unknown) => e)
      );
      expect(err).toBeInstanceOf(OperationOutcomeError);
      const outcome = (err as OperationOutcomeError).outcome;
      expect(outcome.issue?.[0]?.code).toBe('forbidden');
      expect(JSON.stringify(outcome)).not.toContain('row secret detail');
    });
  });

  describe('systemRepo(projectId)', () => {
    test('writes land in that project only', async () => {
      const repo = await host.systemRepo(projectA.id);
      const patient = await withTestContext(() => repo.createResource<Patient>({ resourceType: 'Patient', name: [{ family: 'Synthetic' }] }));
      expect(patient.meta?.project).toBe(projectA.id);
    });

    test('cannot read another project`s resources', async () => {
      const inB = await (await host.systemRepo(projectB.id)).createResource<Patient>({ resourceType: 'Patient', name: [{ family: 'Other' }] });
      const repo = await host.systemRepo(projectA.id);
      const err = await repo.readResource<Patient>('Patient', inB.id).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(OperationOutcomeError);
      expect(isNotFound((err as OperationOutcomeError).outcome)).toBe(true);
      const found = await repo.searchResources<Patient>({ resourceType: 'Patient', filters: [{ code: '_id', operator: Operator.EQUALS, value: inB.id }] });
      expect(found).toHaveLength(0);
    });

    test('refuses a project that does not exist', async () => {
      await expect(host.systemRepo('00000000-0000-0000-0000-000000000000')).rejects.toBeInstanceOf(OperationOutcomeError);
    });
  });

  describe('jobs', () => {
    const workerFor = (name: string): unknown[] | undefined => (Worker as unknown as Mock).mock.calls.find((c) => c[0] === name);
    const processorFor = (name: string): ((job: { data: unknown }) => Promise<void>) | undefined =>
      workerFor(name)?.[1] as ((job: { data: unknown }) => Promise<void>) | undefined;

    test('a tenant queue requires an enterprise on enqueue and hands it to the handler with the data', async () => {
      const seen: unknown[] = [];
      const queue = await host.jobs.register('sf-test-dispatch', { scope: 'tenant' }, async (job) => {
        seen.push(job);
      });
      expect(queueRegistry.get('sf-test-dispatch')).toBeDefined();
      await expect(queue.add({ operationId: 'op-1' })).rejects.toThrow(/enterpriseId/);
      await queue.add({ operationId: 'op-1' }, { enterpriseId: 'proj-a' });
      const added = (queueRegistry.get('sf-test-dispatch') as unknown as { add: Mock }).add.mock.calls.at(-1);
      expect(added?.[1]).toEqual({ enterpriseId: 'proj-a', data: { operationId: 'op-1' } });
      await processorFor('sf-test-dispatch')?.({ data: added?.[1] });
      expect(seen).toEqual([{ enterpriseId: 'proj-a', data: { operationId: 'op-1' } }]);
    });

    test('job ids are namespaced by enterprise, never collide, and pass BullMQ`s own id validation', async () => {
      const queue = await host.jobs.register('sf-test-ids', { scope: 'tenant' }, async () => undefined);
      await queue.add({}, { enterpriseId: 'proj-a', jobId: 'same' });
      await queue.add({}, { enterpriseId: 'proj-b', jobId: 'same' });
      await queue.add({}, { enterpriseId: '6f1c1f2e-9a4b-4f6a', jobId: '12345' });
      const ids = (queueRegistry.get('sf-test-ids') as unknown as { add: Mock }).add.mock.calls.map((c) => (c[2] as { jobId?: string }).jobId);
      expect(ids).toEqual(['proj-a__same', 'proj-b__same', '6f1c1f2e-9a4b-4f6a__12345']);
      // The real BullMQ (not the test mock) validates each generated id.
      const real = await vi.importActual<typeof BullMQ>('bullmq');
      const validate = (real.Job.prototype as unknown as { validateOptions: (this: unknown, d: unknown) => void }).validateOptions;
      for (const jobId of ids) {
        expect(() => validate.call({ opts: { jobId } }, { data: '{}' })).not.toThrow();
      }
      expect(() => validate.call({ opts: { jobId: 'proj-a:same' } }, { data: '{}' })).toThrow(/cannot contain/);
      await expect(queue.add({}, { enterpriseId: 'proj-a', jobId: 'has:colon' })).rejects.toThrow(/jobId/);
    });

    test('a tenant job without a valid enterprise is refused at dequeue; consecutive jobs each get their own enterprise', async () => {
      const seen: (string | undefined)[] = [];
      await host.jobs.register('sf-test-dequeue', { scope: 'tenant' }, async (job) => {
        seen.push(job.enterpriseId);
      });
      const run = processorFor('sf-test-dequeue');
      await expect(run?.({ data: { data: {} } })).rejects.toThrow(/enterpriseId/);
      await expect(run?.({ data: { enterpriseId: 'bad id!', data: {} } })).rejects.toThrow(/enterpriseId/);
      await run?.({ data: { enterpriseId: 'proj-a', data: {} } });
      await run?.({ data: { enterpriseId: 'proj-b', data: {} } });
      expect(seen).toEqual(['proj-a', 'proj-b']);
    });

    test('a platform queue carries no enterprise and may repeat; a tenant queue may not repeat', async () => {
      await host.jobs.register('sf-test-sweep', { scope: 'platform', everyMs: 5000 }, async () => undefined);
      const q = queueRegistry.get('sf-test-sweep') as unknown as { upsertJobScheduler: Mock };
      expect(q.upsertJobScheduler).toHaveBeenCalledWith('sf-test-sweep:every', { every: 5000 }, expect.objectContaining({ name: 'sf-test-sweep' }));
      await expect(host.jobs.register('sf-test-bad-repeat', { scope: 'tenant', everyMs: 5000 } as never, async () => undefined)).rejects.toThrow(/platform/);
    });

    test('workers get Medplum`s BullMQ settings (concurrency, job retention) and its global concurrency', async () => {
      await host.jobs.register('sf-test-config', { scope: 'platform' }, async () => undefined);
      const options = workerFor('sf-test-config')?.[2] as Record<string, unknown>;
      expect(options).toMatchObject({ concurrency: 20, removeOnComplete: { count: 1 }, removeOnFail: { count: 1 } });
      const q = queueRegistry.get('sf-test-config') as unknown as { removeGlobalConcurrency: Mock; setGlobalConcurrency: Mock };
      expect(q.removeGlobalConcurrency.mock.calls.length + q.setGlobalConcurrency.mock.calls.length).toBeGreaterThan(0);
    });

    test('installing the repeating job propagates a failure (registration awaits it, so startup fails)', async () => {
      const failing = { upsertJobScheduler: vi.fn().mockRejectedValue(new Error('redis down')) };
      await expect(installScheduler(failing as never, 'sf-test-x', 5000)).rejects.toThrow('redis down');
      const ok = { upsertJobScheduler: vi.fn().mockResolvedValue(undefined) };
      await installScheduler(ok as never, 'sf-test-x', 5000);
      expect(ok.upsertJobScheduler).toHaveBeenCalledWith('sf-test-x:every', { every: 5000 }, { name: 'sf-test-x', data: {} });
    });

    test('queue names are namespaced and registered once', async () => {
      await expect(host.jobs.register('not-namespaced', { scope: 'platform' }, async () => undefined)).rejects.toThrow(/sf-/);
      await expect(host.jobs.register('sf-test-dispatch', { scope: 'tenant' }, async () => undefined)).rejects.toThrow(/already/);
    });
  });
});
