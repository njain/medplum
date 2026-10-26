// SPDX-License-Identifier: Apache-2.0
// StrideForward registration hook: the only StrideForward code in this fork.
//
// StrideForward's server modules live in their own repository and are added to the image at build time. They
// never import Medplum internals directly: this hook hands them a small host interface (an authenticated
// router, the request context, transaction-safe SQL access, the database pool and the logger lookup). Upgrading
// Medplum therefore conflicts, at most, here.
//
// SF_MODULES is a comma-separated list of module entry files (absolute paths). Each exports
// `register(host: StrideForwardHost): Promise<void> | void`. With SF_MODULES unset this hook does nothing.
import * as medplumCore from '@medplum/core';
import type { Project } from '@medplum/fhirtypes';
import { Queue, UnrecoverableError, Worker } from 'bullmq';
import type { NextFunction, Request, Response, Router } from 'express';
import { Router as createRouter } from 'express';
import { getConfig } from './config/loader';
import type { MedplumBullmqConfig } from './config/types';
import { getAuthenticatedContext } from './context';
import { DatabaseMode, getDatabasePool } from './database';
import { sendOutcome } from './fhir/outcomes';
import type { TransactionSqlOptions } from './fhir/repository/access-tracker';
import { repoAccess } from './fhir/repository/access-tracker';
import { getGlobalSystemRepo, Repository } from './fhir/repo';
import { PLACEHOLDER_SHARD_ID } from './fhir/sharding';
import { getLogger } from './logger';
import { authenticateRequest } from './oauth/middleware';
import { applyGlobalConcurrency, defaultQueueOptions, queueRegistry } from './workers/utils';

/** What a handler receives: tenant jobs carry the enterprise they were enqueued for; platform jobs carry none. */
export interface StrideForwardJob {
  readonly enterpriseId?: string;
  readonly data: Record<string, unknown>;
}

/** A module's background job queue: `add` from any node; the handler runs on nodes whose workers are enabled. */
export interface StrideForwardQueue {
  /** Tenant queues require `enterpriseId`; job ids are namespaced by it, so enterprises never collide. */
  add(data: Record<string, unknown>, options?: { enterpriseId?: string; jobId?: string; delayMs?: number }): Promise<void>;
}

/**
 * `tenant`: every job belongs to one enterprise, checked on enqueue and again on dequeue (architecture standards §5;
 * AGENTS.md rule 9); the handler re-establishes its tenant context from `job.enterpriseId` for each job. `platform`:
 * work across enterprises by design (the outbox sweeper), the only kind that may repeat.
 */
export type StrideForwardJobScope = { readonly scope: 'tenant' } | { readonly scope: 'platform'; readonly everyMs?: number };

export interface StrideForwardJobs {
  /**
   * Registers a queue named `sf-<module>-<job>` on Medplum's BullMQ connection and in its queue registry (closed on
   * shutdown), with Medplum's BullMQ settings (concurrency, job retention, global concurrency; per-queue overrides in
   * `workers.bullmq.<name>`). Its worker runs where MEDPLUM_WORKERS_ENABLED includes the queue name or '*' (sf-jobs),
   * never on request nodes configured with an empty list. Resolves once the queue and any repeating job are installed,
   * so a failure fails the module's registration and the server's startup.
   */
  register(name: string, scope: StrideForwardJobScope, handler: (job: StrideForwardJob) => Promise<void>): Promise<StrideForwardQueue>;
}

export interface StrideForwardHost {
  /** Mounted at /sf/<name>/; every request is already authenticated by Medplum's own middleware. */
  readonly router: Router;
  readonly getAuthenticatedContext: typeof getAuthenticatedContext;
  readonly repoAccess: typeof repoAccess;
  readonly DatabaseMode: typeof DatabaseMode;
  readonly getDatabasePool: typeof getDatabasePool;
  readonly sendOutcome: typeof sendOutcome;
  /**
   * Runs `callback` in one transaction on `repo` (FHIR writes through the transaction-scoped repository, module SQL
   * through its `getDatabaseClient`). Deferred constraints are checked before the callback returns, and a raw Postgres
   * error from module SQL is replaced by a generic OperationOutcomeError before Medplum wraps it, so database messages
   * never reach the client. Modules must not register pre-commit callbacks on `tx`.
   */
  readonly withTransaction: <T>(repo: Repository, callback: (tx: Repository) => Promise<T>, options: TransactionSqlOptions) => Promise<T>;
  /** Call per use: inside a request it returns the logger carrying that request's project, request and trace ids. */
  readonly getLogger: typeof getLogger;
  readonly core: typeof medplumCore;
  /** Background jobs on Medplum's worker framework. */
  readonly jobs: StrideForwardJobs;
  /**
   * A repository with full access inside one project only (project admin, no access policy), for jobs that must write
   * FHIR (Tasks for rejected charges or unpriced visits). Medplum's own project system repository is not limited to the
   * project; this one reads and writes nothing outside it. Fails for a project that does not exist.
   */
  readonly systemRepo: (projectId: string) => Promise<Repository>;
}

const QUEUE_NAME = /^sf-[a-z0-9]+(-[a-z0-9]+)*$/;
const ENTERPRISE_ID = /^[A-Za-z0-9-]{1,64}$/;
/** A caller's job id: no ':' (BullMQ refuses it) and no '_' run that could be mistaken for the separator. */
const JOB_ID = /^[A-Za-z0-9.-]{1,128}$/;
/** Separates enterprise and job id; enterprise ids cannot contain '_', so the first '__' always splits them. */
const JOB_ID_SEPARATOR = '__';

/**
 * Installs a queue's repeating job under a stable scheduler id (re-registration replaces it). Exported for tests.
 * @param queue - The queue.
 * @param name - The queue name; the job name and the scheduler id derive from it.
 * @param everyMs - The interval.
 */
export async function installScheduler(queue: Pick<Queue, 'upsertJobScheduler'>, name: string, everyMs: number): Promise<void> {
  await queue.upsertJobScheduler(`${name}:every`, { every: everyMs }, { name, data: {} });
}

const jobs: StrideForwardJobs = {
  async register(name, scope, handler) {
    if (!QUEUE_NAME.test(name)) {
      throw new Error(`StrideForward queue names must look like sf-<module>-<job>: ${name}`);
    }
    if (queueRegistry.get(name)) {
      throw new Error(`StrideForward queue ${name} is already registered`);
    }
    const everyMs = scope.scope === 'platform' ? scope.everyMs : (scope as { everyMs?: number }).everyMs;
    if (everyMs !== undefined && scope.scope !== 'platform') {
      throw new Error('only platform queues may repeat');
    }
    if (everyMs !== undefined && (!Number.isSafeInteger(everyMs) || everyMs < 1000)) {
      throw new Error('everyMs must be a whole number of milliseconds, at least 1000');
    }
    const config = getConfig();
    const queueOptions = defaultQueueOptions(config);
    const perQueue = (config.workers?.bullmq as Record<string, Partial<MedplumBullmqConfig>> | undefined)?.[name];
    const bullmq: Partial<MedplumBullmqConfig> = { ...config.bullmq, ...perQueue };
    const queue = new Queue(name, queueOptions);
    const enabled = config.workers?.enabled as string[] | undefined;
    const workerEnabled = enabled === undefined || enabled.includes('*') || enabled.includes(name);
    const worker = workerEnabled
      ? new Worker(
          name,
          async (job: { data: unknown }) => {
            const envelope = (job.data ?? {}) as { enterpriseId?: unknown; data?: unknown };
            const data = (envelope.data ?? {}) as Record<string, unknown>;
            if (scope.scope === 'tenant') {
              if (typeof envelope.enterpriseId !== 'string' || !ENTERPRISE_ID.test(envelope.enterpriseId)) {
                // Never retried: a job without its enterprise cannot be run in any tenant's context.
                throw new UnrecoverableError('tenant job has no valid enterpriseId');
              }
              await handler({ enterpriseId: envelope.enterpriseId, data });
            } else {
              await handler({ data });
            }
          },
          { ...queueOptions, ...bullmq }
        )
      : undefined;
    queueRegistry.add(name, queue, worker);
    await applyGlobalConcurrency(queue, bullmq);
    if (everyMs !== undefined) {
      await installScheduler(queue, name, everyMs);
    }
    return {
      async add(data, options) {
        let jobId = options?.jobId;
        const envelope: Record<string, unknown> = { data };
        if (scope.scope === 'tenant') {
          const enterpriseId = options?.enterpriseId;
          if (typeof enterpriseId !== 'string' || !ENTERPRISE_ID.test(enterpriseId)) {
            throw new Error('tenant jobs need a valid enterpriseId');
          }
          envelope.enterpriseId = enterpriseId;
          if (jobId !== undefined) {
            if (!JOB_ID.test(jobId)) {
              throw new Error('jobId must be 1-128 letters, digits, dots or hyphens');
            }
            jobId = `${enterpriseId}${JOB_ID_SEPARATOR}${jobId}`;
          }
        } else if (jobId !== undefined && !JOB_ID.test(jobId)) {
          throw new Error('jobId must be 1-128 letters, digits, dots or hyphens');
        }
        await queue.add(name, envelope, { jobId, delay: options?.delayMs });
      },
    };
  },
};

async function projectSystemRepo(projectId: string): Promise<Repository> {
  const project = await getGlobalSystemRepo().readResource<Project>('Project', projectId);
  return new Repository({
    routing: { kind: 'project-shard', shardId: PLACEHOLDER_SHARD_ID },
    projects: [project],
    author: { reference: 'system' },
    superAdmin: false,
    projectAdmin: true,
    strictMode: true,
    extendedMode: true,
    checkReferencesOnWrite: project.checkReferencesOnWrite,
  });
}

export async function registerStrideForwardModules(apiRouter: Router): Promise<void> {
  const entries = (process.env.SF_MODULES ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  for (const entry of entries) {
    const mod = (await import(entry)) as { name?: string; register: (host: StrideForwardHost) => Promise<void> | void };
    const name = mod.name ?? entry.replace(/^.*\//, '').replace(/\.[^.]+$/, '');
    const router = createRouter();
    router.use(authenticateRequest);
    await mod.register({
      router,
      getAuthenticatedContext,
      repoAccess,
      DatabaseMode,
      getDatabasePool,
      sendOutcome,
      withTransaction: moduleTransaction,
      getLogger,
      core: medplumCore,
      jobs,
      systemRepo: projectSystemRepo,
    });
    router.use(moduleErrorBoundary(name));
    apiRouter.use(`/sf/${name}/`, router);
    getLogger().info('StrideForward module registered', { name, entry });
  }
}

/**
 * Every error a module route throws or rejects with leaves as an OperationOutcome (architecture standards §2.5).
 * Intentional outcomes (OperationOutcomeError) pass through unchanged; anything else is logged (type and request
 * context only, never the message) and returned as a generic server error, so internal messages never reach the
 * client or the logs.
 * @param name - The module name, for the log entry.
 * @returns Express error middleware for the module's router.
 */
function moduleErrorBoundary(name: string) {
  return (err: unknown, _req: Request, res: Response, next: NextFunction): void => {
    if (res.headersSent) {
      next(err);
      return;
    }
    if (err instanceof medplumCore.OperationOutcomeError) {
      sendOutcome(res, err.outcome);
      return;
    }
    // Log the error's type only: messages can carry PHI (security §9). The request ids come from the request logger.
    getLogger().error('StrideForward module error', { module: name, errorType: err instanceof Error ? err.name : typeof err });
    sendOutcome(res, medplumCore.serverError(new Error('Internal server error')));
  };
}

/**
 * Repository.withTransaction for modules. Medplum's normalizeDatabaseError copies a Postgres error's message into
 * the OperationOutcome it returns and does not keep the original error, so raw database errors from module SQL are
 * replaced here, inside the transaction, before Medplum sees them. A serialization failure becomes a conflict that keeps
 * its 40001 coding, so Medplum retries the transaction; a duplicate key becomes a 409; a row-security or privilege
 * refusal (42501) a 403; anything else a 500. The
 * transaction still rolls back because the callback still throws. Only the SQLSTATE is logged.
 * @param repo - The caller's repository (ctx.repo).
 * @param callback - The transactional work.
 * @param options - Medplum transaction options (resource types, source label).
 * @returns The callback's result.
 */
async function moduleTransaction<T>(
  repo: Repository,
  callback: (tx: Repository) => Promise<T>,
  options: TransactionSqlOptions
): Promise<T> {
  return repo.withTransaction(async (tx) => {
    try {
      const result = await callback(tx);
      // Check deferred constraints now, inside this catch, instead of at COMMIT (where Medplum would wrap the error
      // with its database message). Afterwards only Medplum's own commit work remains.
      // Uses the transaction's own resource types: Medplum routes every SQL call to a shard by them.
      await tx
        .getDatabaseClient(repoAccess.sqlWrite(options.resourceTypes, { source: 'sf-hook.checkDeferred' }))
        .query('SET CONSTRAINTS ALL IMMEDIATE');
      return result;
    } catch (err) {
      const sqlState = postgresErrorCode(err);
      if (!sqlState) {
        throw err;
      }
      // Log the SQLSTATE and the call site only: Postgres messages can repeat input values (PHI).
      getLogger().error('StrideForward module database error', { source: options.source, sqlState });
      if (sqlState === '40001') {
        // Keep the SQLSTATE coding so Medplum's isRetryableTransactionError retries the whole transaction.
        throw new medplumCore.OperationOutcomeError(medplumCore.conflict('Concurrent update; retrying', sqlState));
      }
      if (sqlState === '42501') {
        // A row-security or privilege refusal: the caller may not touch that row (multi-tenancy doc 12 §3).
        throw new medplumCore.OperationOutcomeError(medplumCore.forbidden);
      }
      throw new medplumCore.OperationOutcomeError(
        sqlState === '23505'
          ? medplumCore.conflict('The record already exists')
          : medplumCore.serverError(new Error('Internal server error'))
      );
    }
  }, options);
}

/**
 * @param err - Any thrown value.
 * @returns The Postgres SQLSTATE when the error came from node-postgres (a five-character `code`), else undefined.
 */
function postgresErrorCode(err: unknown): string | undefined {
  const code = (err as { code?: unknown } | undefined)?.code;
  return typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code) ? code : undefined;
}
