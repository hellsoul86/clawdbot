import type { ResolvedLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey } from "./ids.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import type { LarkRuntimeEnv } from "./types.js";

type KnowledgeScope = {
  scopeType: "dm_user" | "group" | "group_user" | "tenant";
  scopeId: string;
};

function buildKnowledgeScopes(params: {
  chatType: "direct" | "group";
  chatId: string;
  senderId: string;
  tenantKey: string;
}): KnowledgeScope[] {
  const scopes: KnowledgeScope[] = [];
  if (params.chatType === "direct") {
    scopes.push({ scopeType: "dm_user", scopeId: params.senderId });
  } else {
    scopes.push({ scopeType: "group", scopeId: params.chatId });
    scopes.push({ scopeType: "group_user", scopeId: `${params.chatId}:${params.senderId}` });
  }
  scopes.push({ scopeType: "tenant", scopeId: params.tenantKey });
  return scopes;
}

function mergeContent(existing: string, next: string): string {
  if (!existing.trim()) return next;
  if (existing.includes(next.trim())) return existing;
  return `${existing.trim()}\n\n---\n\n${next.trim()}`;
}

export async function upsertLarkKnowledgeDoc(params: {
  account: ResolvedLarkAccount;
  tenantKey?: string;
  scopeType: KnowledgeScope["scopeType"];
  scopeId: string;
  title: string;
  content: string;
  tags?: string[];
  sourceIds?: string[];
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const title = params.title.trim();
  const content = params.content.trim();
  if (!title || !content) return;
  const tags = params.tags?.join(",") ?? null;
  const sources = params.sourceIds && params.sourceIds.length > 0 ? JSON.stringify(params.sourceIds) : null;
  const [rows] = await context.pool.execute(
    `SELECT id, content, version
     FROM ${context.tables.kbDoc}
     WHERE tenant_key = ? AND scope_type = ? AND scope_id = ? AND title = ?
     ORDER BY version DESC
     LIMIT 1`,
    [tenantKey, params.scopeType, params.scopeId, title],
  );
  const existing = (rows as Array<Record<string, unknown>>)[0];
  if (!existing) {
    await context.pool.execute(
      `INSERT INTO ${context.tables.kbDoc} (
        tenant_key,
        scope_type,
        scope_id,
        title,
        tags,
        source_ids,
        content,
        version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [tenantKey, params.scopeType, params.scopeId, title, tags, sources, content, 1],
    );
    return;
  }
  const merged = mergeContent(String(existing.content ?? ""), content);
  const version = Number(existing.version ?? 1) + 1;
  await context.pool.execute(
    `UPDATE ${context.tables.kbDoc}
     SET content = ?, tags = ?, source_ids = ?, version = ?
     WHERE id = ?`,
    [merged, tags, sources, version, existing.id],
  );
}

async function searchKnowledgeDocs(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  scopes: KnowledgeScope[];
  query?: string | null;
  limit: number;
  env?: LarkRuntimeEnv;
}): Promise<Array<{ title: string; content: string }>> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return [];
  await ensureLarkSchema(context, params.env);
  const q = params.query?.trim();
  const scopeFilters = params.scopes
    .map(() => `(scope_type = ? AND scope_id = ?)`)
    .join(" OR ");
  const scopeArgs = params.scopes.flatMap((scope) => [scope.scopeType, scope.scopeId]);
  const baseArgs = [params.tenantKey, ...scopeArgs];
  if (q) {
    const like = `%${q}%`;
    const [rows] = await context.pool.execute(
      `SELECT title, content
       FROM ${context.tables.kbDoc}
       WHERE tenant_key = ?
         AND (${scopeFilters})
         AND (title LIKE ? OR content LIKE ?)
       ORDER BY updated_at DESC
       LIMIT ?`,
      [...baseArgs, like, like, params.limit],
    );
    return (rows as Array<Record<string, unknown>>)
      .map((row) => ({
        title: String(row.title ?? ""),
        content: String(row.content ?? ""),
      }))
      .filter((row) => row.title && row.content);
  }
  const [rows] = await context.pool.execute(
    `SELECT title, content
     FROM ${context.tables.kbDoc}
     WHERE tenant_key = ? AND (${scopeFilters})
     ORDER BY updated_at DESC
     LIMIT ?`,
    [...baseArgs, params.limit],
  );
  return (rows as Array<Record<string, unknown>>)
    .map((row) => ({
      title: String(row.title ?? ""),
      content: String(row.content ?? ""),
    }))
    .filter((row) => row.title && row.content);
}

export async function buildLarkKnowledgeContext(params: {
  account: ResolvedLarkAccount;
  tenantKey?: string;
  chatType: "direct" | "group";
  chatId: string;
  senderId: string;
  query?: string | null;
  limit?: number;
  env?: LarkRuntimeEnv;
}): Promise<string | null> {
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const scopes = buildKnowledgeScopes({
    chatType: params.chatType,
    chatId: params.chatId,
    senderId: params.senderId,
    tenantKey,
  });
  const docs = await searchKnowledgeDocs({
    account: params.account,
    tenantKey,
    scopes,
    query: params.query,
    limit: params.limit ?? 3,
    env: params.env,
  });
  if (docs.length === 0) return null;
  const lines = docs.map((doc) => `- ${doc.title}: ${doc.content}`).join("\n");
  return `Knowledge base:\n${lines}`;
}
