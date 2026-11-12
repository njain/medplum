// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0
// sf-has (named patch, StrideForward fork): chained-search links (`_has` and forward chains) only match resources the
// caller may search. Synthetic data only.
import type { WithId } from '@medplum/core';
import { parseSearchRequest } from '@medplum/core';
import type { AccessPolicy, Encounter, Observation, Patient, Project } from '@medplum/fhirtypes';
import { randomUUID } from 'crypto';
import { initAppServices, shutdownApp } from '../app';
import { loadTestConfig } from '../config/loader';
import { createTestProject, withTestContext } from '../test.setup';
import type { SystemRepository } from './repo';
import { getProjectSystemRepo, Repository } from './repo';
import { PLACEHOLDER_SHARD_ID } from './sharding';

const ESTRADIOL = 'http://loinc.org|2243-4';
// Every resource type the tests use, unrestricted (the control: only the project limits what matches).
const OPEN: AccessPolicy['resource'] = [
  { resourceType: 'Patient' },
  { resourceType: 'Observation' },
  { resourceType: 'Encounter' },
];

describe('sf-has: chained search honours the access policy', () => {
  let project: WithId<Project>;
  let systemRepo: SystemRepository;
  let teamA: string;
  let teamB: string;
  let patientA: WithId<Patient>;
  let patientB: WithId<Patient>;
  let encounterA: WithId<Encounter>;
  let encounterB: WithId<Encounter>;

  const repoWith = (resource: AccessPolicy['resource']): Repository =>
    new Repository({
      projects: [project],
      author: { reference: 'Practitioner/' + randomUUID() },
      accessPolicy: { resourceType: 'AccessPolicy', resource },
      routing: { kind: 'project-shard', shardId: PLACEHOLDER_SHARD_ID },
    });
  const ids = async (repo: Repository, query: string): Promise<string[]> =>
    (await repo.searchResources(parseSearchRequest(query))).map((r) => r.id).sort();

  beforeAll(async () => {
    await initAppServices(await loadTestConfig());
  });

  afterAll(async () => {
    await shutdownApp();
  });

  beforeEach(() =>
    withTestContext(async () => {
      project = (await createTestProject()).project;
      systemRepo = await getProjectSystemRepo(project);
      teamA = 'Organization/' + randomUUID();
      teamB = 'Organization/' + randomUUID();
      const chart = async (team: string, given: string): Promise<[WithId<Patient>, WithId<Encounter>]> => {
        const meta = { project: project.id, accounts: [{ reference: team }] };
        const patient = await systemRepo.createResource<Patient>({
          resourceType: 'Patient',
          meta,
          name: [{ given: [given] }],
        });
        const obs = await systemRepo.createResource<Observation>({
          resourceType: 'Observation',
          meta,
          status: 'final',
          code: { coding: [{ system: 'http://loinc.org', code: '2243-4' }] },
          subject: { reference: 'Patient/' + patient.id },
        });
        const encounter = await systemRepo.createResource<Encounter>({
          resourceType: 'Encounter',
          meta,
          status: 'finished',
          class: { code: 'AMB' },
          subject: { reference: 'Patient/' + patient.id },
          reasonReference: [{ reference: 'Observation/' + obs.id }],
        });
        return [patient, encounter];
      };
      [patientA, encounterA] = await chart(teamA, 'Alice');
      [patientB, encounterB] = await chart(teamB, 'Bea');
    })
  );

  // Demographics and encounters for everyone; lab results only in team A (the care-team policy shape).
  const teamPolicy = (): AccessPolicy['resource'] => [
    { resourceType: 'Patient' },
    { resourceType: 'Encounter' },
    { resourceType: 'Observation', criteria: `Observation?_compartment=${teamA}` },
  ];

  test('_has matches only resources the caller may search', () =>
    withTestContext(async () => {
      const repo = repoWith(teamPolicy());
      expect(await ids(repo, `Patient?_has:Observation:subject:code=${ESTRADIOL}`)).toStrictEqual([patientA.id]);
      // Unrestricted within the project, both match (the control).
      expect(await ids(repoWith(OPEN), `Patient?_has:Observation:subject:code=${ESTRADIOL}`)).toStrictEqual(
        [patientA.id, patientB.id].sort()
      );
    }));

  test('_has inside _filter matches only resources the caller may search', () =>
    withTestContext(async () => {
      const repo = repoWith(teamPolicy());
      expect(
        await ids(repo, `Patient?_filter=${encodeURIComponent('_has:Observation:subject:code eq 2243-4')}`)
      ).toStrictEqual([patientA.id]);
    }));

  test('a forward chain does not look through a hidden resource', () =>
    withTestContext(async () => {
      const repo = repoWith(teamPolicy());
      expect(await ids(repo, `Encounter?reason-reference:Observation.code=${ESTRADIOL}`)).toStrictEqual([
        encounterA.id,
      ]);
      expect(await ids(repoWith(OPEN), `Encounter?reason-reference:Observation.code=${ESTRADIOL}`)).toStrictEqual(
        [encounterA.id, encounterB.id].sort()
      );
    }));

  test('a chain through a type the caller cannot search at all is refused', () =>
    withTestContext(async () => {
      const repo = repoWith([{ resourceType: 'Patient' }, { resourceType: 'Encounter' }]);
      await expect(ids(repo, `Patient?_has:Observation:subject:code=${ESTRADIOL}`)).rejects.toThrow('Forbidden');
    }));

  test('an unrestricted entry for the linked type keeps every match', () =>
    withTestContext(async () => {
      const repo = repoWith([{ resourceType: 'Patient' }, { resourceType: 'Observation' }]);
      expect(await ids(repo, `Patient?_has:Observation:subject:code=${ESTRADIOL}`)).toStrictEqual(
        [patientA.id, patientB.id].sort()
      );
    }));

  test('policy criteria that are themselves chained still apply, without recursion', () =>
    withTestContext(async () => {
      // Observation visible only when its subject is named Alice: a chain inside the policy, not filtered again.
      const repo = repoWith([
        { resourceType: 'Patient', criteria: `Patient?_compartment=${teamA}` },
        { resourceType: 'Observation', criteria: 'Observation?subject:Patient.name=Alice' },
      ]);
      const visible = await repo.searchResources(parseSearchRequest<Observation>('Observation'));
      expect(visible.map((o) => o.subject?.reference)).toStrictEqual(['Patient/' + patientA.id]);
      expect(await ids(repo, `Patient?_has:Observation:subject:code=${ESTRADIOL}`)).toStrictEqual([patientA.id]);
    }));

  test('chained policy criteria cannot be satisfied from another project', () =>
    withTestContext(async () => {
      // Another project's Observation references this project's patient B; it must not satisfy B's chained policy.
      const other = (await createTestProject()).project;
      const otherRepo = await getProjectSystemRepo(other);
      await otherRepo.createResource<Observation>({
        resourceType: 'Observation',
        meta: { project: other.id },
        status: 'final',
        code: { coding: [{ system: 'http://example.com/sf-has', code: 'foreign' }] },
        subject: { reference: 'Patient/' + patientB.id },
      });
      const repo = repoWith([
        {
          resourceType: 'Patient',
          criteria: 'Patient?_has:Observation:subject:code=http://example.com/sf-has|foreign',
        },
        { resourceType: 'Observation' },
      ]);
      expect(await ids(repo, 'Patient')).toStrictEqual([]);
    }));
});
