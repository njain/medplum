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
import type { NextFunction, Request, Response, Router } from 'express';
import { Router as createRouter } from 'express';
import { getAuthenticatedContext } from './context';
import { DatabaseMode, getDatabasePool } from './database';
import { sendOutcome } from './fhir/outcomes';
import type { TransactionSqlOptions } from './fhir/repository/access-tracker';
import { repoAccess } from './fhir/repository/access-tracker';
import type { Repository } from './fhir/repo';
import { getLogger } from './logger';
import { authenticateRequest } from './oauth/middleware';

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
 * its 40001 coding, so Medplum retries the transaction; a duplicate key becomes a 409; anything else a 500. The
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
