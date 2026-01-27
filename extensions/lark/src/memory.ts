import type { ClawdbotPluginService } from "clawdbot/plugin-sdk";

import type { ResolvedLarkAccount } from "./accounts.js";
import { listLarkAccountIds, resolveLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey } from "./ids.js";
import { upsertLarkKnowledgeDoc } from "./knowledge.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import type { LarkRuntimeEnv } from "./types.js";

type MemoryScope = {
  scopeType: "dm_user" | "group" | "group_user" | "tenant";
  scopeId: string;
};

const DEFAULT_SUMMARY_INTERVAL_MINUTES = 360;
const DEFAULT_SUMMARY_MAX_ITEMS = 20;
const DEFAULT_RETRIEVAL_LIMIT = 4;

function resolveMemoryEnabled(account: ResolvedLarkAccount): boolean {
  return account.config.memory?.enabled !== false;
}

function buildMemoryScopes(params: {
  chatType: "direct" | "group";
  chatId: string;
  senderId: string;
  tenantKey: string;
}): MemoryScope[] {
  const scopes: MemoryScope[] = [];
  if (params.chatType === "direct") {
    scopes.push({ scopeType: "dm_user", scopeId: params.senderId });
  } else {
    scopes.push({ scopeType: "group", scopeId: params.chatId });
    scopes.push({ scopeType: "group_user", scopeId: `${params.chatId}:${params.senderId}` });
  }
  scopes.push({ scopeType: "tenant", scopeId: params.tenantKey });
  return scopes;
}

async function insertMemoryItems(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  messageId?: string | null;
  chatId?: string | null;
  userId?: string | null;
  content: string;
  scopes: MemoryScope[];
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const cols = [
    "tenant_key",
    "scope_type",
    "scope_id",
    "message_id",
    "chat_id",
    "user_id",
    "content",
  ];
  const rows = params.scopes.map((scope) => [
    params.tenantKey,
    scope.scopeType,
    scope.scopeId,
    params.messageId ?? null,
    params.chatId ?? null,
    params.userId ?? null,
    params.content,
  ]);
  const placeholders = rows.map(() => `(${cols.map(() => "?").join(",")})`).join(",");
  await context.pool.execute(
    `INSERT INTO ${context.tables.memoryItem} (${cols.join(",")}) VALUES ${placeholders}`,
    rows.flat(),
  );
}

export async function recordLarkMemoryFromMessage(params: {
  account: ResolvedLarkAccount;
  tenantKey?: string;
  chatId: string;
  chatType: "direct" | "group";
  senderId: string;
  messageId: string;
  content: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  if (!resolveMemoryEnabled(params.account)) return;
  const content = params.content.trim();
  if (!content) return;
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const scopes = buildMemoryScopes({
    chatType: params.chatType,
    chatId: params.chatId,
    senderId: params.senderId,
    tenantKey,
  });
  await insertMemoryItems({
    account: params.account,
    tenantKey,
    messageId: params.messageId,
    chatId: params.chatId,
    userId: params.senderId,
    content,
    scopes,
    env: params.env,
  });
}

export async function recordLarkMemoryFromExtraction(params: {
  account: ResolvedLarkAccount;
  tenantKey?: string;
  messageId: string;
  content: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  if (!resolveMemoryEnabled(params.account)) return;
  const text = params.content.trim();
  if (!text) return;
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const [rows] = await context.pool.execute(
    `SELECT chat_id, chat_type, sender_user_id, sender_open_id, sender_union_id
     FROM ${context.tables.message}
     WHERE message_id = ?
     LIMIT 1`,
    [params.messageId],
  );
  const row = (rows as Array<Record<string, unknown>>)[0];
  if (!row) return;
  const chatId = typeof row.chat_id === "string" ? row.chat_id : "";
  const chatTypeRaw = typeof row.chat_type === "string" ? row.chat_type : "group";
  const chatType = chatTypeRaw === "p2p" ? "direct" : "group";
  const senderId =
    (typeof row.sender_user_id === "string" && row.sender_user_id) ||
    (typeof row.sender_open_id === "string" && row.sender_open_id) ||
    (typeof row.sender_union_id === "string" && row.sender_union_id) ||
    chatId;
  const scopes = buildMemoryScopes({
    chatType,
    chatId,
    senderId,
    tenantKey,
  });
  await insertMemoryItems({
    account: params.account,
    tenantKey,
    messageId: params.messageId,
    chatId,
    userId: senderId,
    content: text,
    scopes,
    env: params.env,
  });
}

async function fetchMemoryItems(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  scope: MemoryScope;
  query?: string | null;
  limit: number;
  env?: LarkRuntimeEnv;
}): Promise<string[]> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return [];
  await ensureLarkSchema(context, params.env);
  const q = params.query?.trim();
  if (q) {
    const like = `%${q}%`;
    const [rows] = await context.pool.execute(
      `SELECT content
       FROM ${context.tables.memoryItem}
       WHERE tenant_key = ?
         AND scope_type = ?
         AND scope_id = ?
         AND content LIKE ?
       ORDER BY created_at DESC
       LIMIT ?`,
      [params.tenantKey, params.scope.scopeType, params.scope.scopeId, like, params.limit],
    );
    return (rows as Array<Record<string, unknown>>)
      .map((row) => (typeof row.content === "string" ? row.content : ""))
      .filter(Boolean);
  }
  const [rows] = await context.pool.execute(
    `SELECT content
     FROM ${context.tables.memoryItem}
     WHERE tenant_key = ?
       AND scope_type = ?
       AND scope_id = ?
     ORDER BY created_at DESC
     LIMIT ?`,
    [params.tenantKey, params.scope.scopeType, params.scope.scopeId, params.limit],
  );
  return (rows as Array<Record<string, unknown>>)
    .map((row) => (typeof row.content === "string" ? row.content : ""))
    .filter(Boolean);
}

export async function buildLarkMemoryContext(params: {
  account: ResolvedLarkAccount;
  tenantKey?: string;
  chatId: string;
  chatType: "direct" | "group";
  senderId: string;
  query?: string | null;
  env?: LarkRuntimeEnv;
}): Promise<string | null> {
  if (!resolveMemoryEnabled(params.account)) return null;
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const limit = params.account.config.memory?.retrievalLimit ?? DEFAULT_RETRIEVAL_LIMIT;
  const scopes = buildMemoryScopes({
    chatType: params.chatType,
    chatId: params.chatId,
    senderId: params.senderId,
    tenantKey,
  });
  const sections: string[] = [];
  for (const scope of scopes) {
    const items = await fetchMemoryItems({
      account: params.account,
      tenantKey,
      scope,
      query: params.query,
      limit,
      env: params.env,
    });
    if (items.length === 0) continue;
    const label =
      scope.scopeType === "dm_user"
        ? "DM memory"
        : scope.scopeType === "group"
          ? "Group memory"
          : scope.scopeType === "group_user"
            ? "Group user memory"
            : "Company memory";
    sections.push(`${label}:\n- ${items.join("\n- ")}`);
  }
  if (sections.length === 0) return null;
  return `Relevant memory:\n${sections.join("\n\n")}`;
}

function summarizeLines(lines: string[], maxChars: number): string {
  const summary: string[] = [];
  const seen = new Set<string>();
  for (const line of lines) {
    const cleaned = line.replace(/\s+/g, " ").trim();
    if (!cleaned || seen.has(cleaned)) continue;
    summary.push(cleaned);
    seen.add(cleaned);
    if (summary.join(" ").length > maxChars) break;
  }
  return summary.join(" ");
}

async function summarizeScope(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  scope: MemoryScope;
  maxItems: number;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const [rows] = await context.pool.execute(
    `SELECT content
     FROM ${context.tables.memoryItem}
     WHERE tenant_key = ?
       AND scope_type = ?
       AND scope_id = ?
     ORDER BY created_at DESC
     LIMIT ?`,
    [params.tenantKey, params.scope.scopeType, params.scope.scopeId, params.maxItems],
  );
  const items = (rows as Array<Record<string, unknown>>)
    .map((row) => (typeof row.content === "string" ? row.content : ""))
    .filter(Boolean);
  if (items.length === 0) return;
  const summary = summarizeLines(items, 800);
  if (!summary) return;
  await insertMemoryItems({
    account: params.account,
    tenantKey: params.tenantKey,
    content: summary,
    scopes: [params.scope],
    env: params.env,
  });
  await upsertLarkKnowledgeDoc({
    account: params.account,
    tenantKey: params.tenantKey,
    scopeType: params.scope.scopeType,
    scopeId: params.scope.scopeId,
    title: `Summary ${params.scope.scopeType} ${new Date().toISOString().slice(0, 10)}`,
    content: summary,
    tags: ["summary"],
    env: params.env,
  });
}

async function summarizeAllScopes(params: {
  account: ResolvedLarkAccount;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  if (!resolveMemoryEnabled(params.account)) return;
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account);
  const [rows] = await context.pool.execute(
    `SELECT DISTINCT scope_type, scope_id
     FROM ${context.tables.memoryItem}
     WHERE tenant_key = ?`,
    [tenantKey],
  );
  const scopes = (rows as Array<Record<string, unknown>>)
    .map((row) => ({
      scopeType: row.scope_type as MemoryScope["scopeType"],
      scopeId: typeof row.scope_id === "string" ? row.scope_id : "",
    }))
    .filter((row) => row.scopeType && row.scopeId);
  const maxItems = params.account.config.memory?.summaryMaxItems ?? DEFAULT_SUMMARY_MAX_ITEMS;
  for (const scope of scopes) {
    await summarizeScope({
      account: params.account,
      tenantKey,
      scope,
      maxItems,
      env: params.env,
    });
  }
}

export function createLarkMemoryService(): ClawdbotPluginService {
  const timers = new Map<string, NodeJS.Timeout>();
  return {
    id: "lark-memory",
    start: (ctx) => {
      const cfg = ctx.config as any;
      const ids = listLarkAccountIds(cfg ?? {});
      for (const accountId of ids) {
        const account = resolveLarkAccount({ cfg, accountId });
        if (!account) continue;
        const intervalMinutes =
          account.config.memory?.summaryIntervalMinutes ?? DEFAULT_SUMMARY_INTERVAL_MINUTES;
        const intervalMs = Math.max(10, intervalMinutes) * 60 * 1000;
        const env: LarkRuntimeEnv = {
          log: ctx.logger.info,
          error: ctx.logger.error,
        };
        const run = () => {
          void summarizeAllScopes({ account, env }).catch((err) => {
            env.error?.(`[lark] memory summary failed: ${String(err)}`);
          });
        };
        run();
        const timer = setInterval(run, intervalMs);
        timers.set(account.accountId, timer);
      }
    },
    stop: () => {
      for (const timer of timers.values()) clearInterval(timer);
      timers.clear();
    },
  };
}
