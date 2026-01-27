import * as lark from "@larksuiteoapi/node-sdk";

import type { ResolvedLarkAccount } from "./accounts.js";

type CachedClient = {
  key: string;
  client: lark.Client;
};

const clientCache = new Map<string, CachedClient>();

function resolveDomain(account: ResolvedLarkAccount): lark.Domain {
  return account.region === "feishu" ? lark.Domain.Feishu : lark.Domain.Lark;
}

function buildCacheKey(account: ResolvedLarkAccount): string {
  return [account.appId ?? "", account.appSecret ?? "", account.region].join("|");
}

export function getLarkClient(account: ResolvedLarkAccount): lark.Client {
  if (!account.appId || !account.appSecret) {
    throw new Error("Lark appId/appSecret not configured");
  }
  const key = buildCacheKey(account);
  const cached = clientCache.get(account.accountId);
  if (cached && cached.key === key) return cached.client;

  const client = new lark.Client({
    appId: account.appId,
    appSecret: account.appSecret,
    appType: lark.AppType.SelfBuild,
    domain: resolveDomain(account),
  });
  clientCache.set(account.accountId, { key, client });
  return client;
}

export function resolveTenantOptions(
  tenantKey?: string,
): ReturnType<typeof lark.withTenantKey> | undefined {
  const trimmed = tenantKey?.trim();
  if (!trimmed) return undefined;
  return lark.withTenantKey(trimmed);
}
