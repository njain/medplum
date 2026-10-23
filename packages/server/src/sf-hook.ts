// SPDX-License-Identifier: Apache-2.0
// StrideForward registration hook: the only StrideForward code in this fork.
//
// StrideForward's server modules live in their own repository and are added to the image at build time. They
// never import Medplum internals directly: this hook hands them a small host interface (an authenticated
// router, the request context, transaction-safe SQL access, the database pool and the logger). Upgrading
// Medplum therefore conflicts, at most, here.
//
// SF_MODULES is a comma-separated list of module entry files (absolute paths). Each exports
// `register(host: StrideForwardHost): Promise<void> | void`. With SF_MODULES unset this hook does nothing.
import * as medplumCore from '@medplum/core';
import type { Router } from 'express';
import { Router as createRouter } from 'express';
import { getAuthenticatedContext } from './context';
import { DatabaseMode, getDatabasePool } from './database';
import { sendOutcome } from './fhir/outcomes';
import { repoAccess } from './fhir/repository/access-tracker';
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
  readonly logger: ReturnType<typeof getLogger>;
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
      logger: getLogger(),
      core: medplumCore,
    });
    apiRouter.use(`/sf/${name}/`, router);
    getLogger().info('StrideForward module registered', { name, entry });
  }
}
