// SPDX-License-Identifier: Apache-2.0
// StrideForward registration hook: the host additions modules rely on (first-module brief §6; plan item 1.9).
import { isNotFound, OperationOutcomeError, Operator } from '@medplum/core';
import type { WithId } from '@medplum/core';
import type { Patient, Project } from '@medplum/fhirtypes';
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
import { registerStrideForwardModules } from './sf-hook';
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
    test('register creates a queue and a worker whose processor hands the job data to the handler', async () => {
      const seen: unknown[] = [];
      const queue = host.jobs.register('sf-test-dispatch', async (data) => {
        seen.push(data);
      });
      expect(queueRegistry.get('sf-test-dispatch')).toBeDefined();
      await queue.add({ operationId: 'op-1' });
      const created = (Worker as unknown as Mock).mock.calls.find((c) => c[0] === 'sf-test-dispatch');
      const processor = created?.[1] as ((job: { data: unknown }) => Promise<void>) | undefined;
      expect(processor).toBeDefined();
      await processor?.({ data: { operationId: 'op-1' } });
      expect(seen).toEqual([{ operationId: 'op-1' }]);
    });

    test('a repeating job is scheduled once under a stable id', async () => {
      host.jobs.register('sf-test-sweep', async () => undefined, { everyMs: 5000 });
      const q = queueRegistry.get('sf-test-sweep') as unknown as { upsertJobScheduler: Mock };
      expect(q.upsertJobScheduler).toHaveBeenCalledWith('sf-test-sweep:every', { every: 5000 }, expect.objectContaining({ name: 'sf-test-sweep' }));
    });

    test('queue names are namespaced and registered once', () => {
      expect(() => host.jobs.register('not-namespaced', async () => undefined)).toThrow(/sf-/);
      expect(() => host.jobs.register('sf-test-dispatch', async () => undefined)).toThrow();
    });
  });
});
