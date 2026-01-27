import type { ChannelDirectoryEntry, ClawdbotConfig } from "clawdbot/plugin-sdk";

import type { ResolvedLarkAccount } from "./accounts.js";
import { resolveLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey, resolveLarkUserKey, type LarkUserIdBundle } from "./ids.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import type { LarkRuntimeEnv } from "./types.js";

type OrgDepartment = {
  departmentId: string;
  name: string;
  parentDepartmentId?: string;
};

type OrgUserRow = {
  userKey: string;
  openId?: string;
  userId?: string;
  unionId?: string;
  name?: string;
};

type OrgRelationRow = {
  userKey: string;
  departmentId: string;
  isPrimary: boolean;
};

type OrgUserProfile = {
  id: string;
  name?: string;
  departmentChain?: string;
};

type OrgGroupProfile = {
  id: string;
  name?: string;
};

type ChatProfile = {
  name?: string;
  memberCount?: number;
};

function normalizeString(value?: string | null): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

function buildDepartmentChain(
  departmentId: string | undefined,
  departments: Map<string, OrgDepartment>,
): string | undefined {
  if (!departmentId) return undefined;
  const parts: string[] = [];
  let current = departmentId;
  const seen = new Set<string>();
  while (current && !seen.has(current)) {
    seen.add(current);
    const dept = departments.get(current);
    if (!dept) break;
    parts.unshift(dept.name);
    if (!dept.parentDepartmentId || dept.parentDepartmentId === current) break;
    current = dept.parentDepartmentId;
  }
  return parts.length > 0 ? parts.join(" > ") : undefined;
}

async function loadDepartments(params: {
  context: NonNullable<ReturnType<typeof resolveLarkMySqlContext>>;
  tenantKey: string;
}): Promise<Map<string, OrgDepartment>> {
  const [rows] = await params.context.pool.execute(
    `SELECT department_id, name, parent_department_id
     FROM ${params.context.tables.orgDepartment}
     WHERE tenant_key = ?`,
    [params.tenantKey],
  );
  const map = new Map<string, OrgDepartment>();
  for (const row of rows as Array<Record<string, unknown>>) {
    const departmentId = typeof row.department_id === "string" ? row.department_id : "";
    const name = typeof row.name === "string" ? row.name : "";
    if (!departmentId || !name) continue;
    map.set(departmentId, {
      departmentId,
      name,
      parentDepartmentId:
        typeof row.parent_department_id === "string" ? row.parent_department_id : undefined,
    });
  }
  return map;
}

async function loadRelations(params: {
  context: NonNullable<ReturnType<typeof resolveLarkMySqlContext>>;
  tenantKey: string;
  userKeys: string[];
}): Promise<OrgRelationRow[]> {
  if (params.userKeys.length === 0) return [];
  const placeholders = params.userKeys.map(() => "?").join(",");
  const [rows] = await params.context.pool.execute(
    `SELECT user_key, department_id, is_primary
     FROM ${params.context.tables.orgUserDepartmentRel}
     WHERE tenant_key = ? AND user_key IN (${placeholders})`,
    [params.tenantKey, ...params.userKeys],
  );
  return (rows as Array<Record<string, unknown>>)
    .map((row) => ({
      userKey: typeof row.user_key === "string" ? row.user_key : "",
      departmentId: typeof row.department_id === "string" ? row.department_id : "",
      isPrimary: Boolean(row.is_primary),
    }))
    .filter((row) => row.userKey && row.departmentId);
}

function formatOrgUserDisplay(params: {
  user: OrgUserRow;
  relations: OrgRelationRow[];
  departments: Map<string, OrgDepartment>;
}): OrgUserProfile {
  const name = normalizeString(params.user.name);
  const chain = params.relations.find((rel) => rel.isPrimary) ?? params.relations[0];
  const departmentChain = buildDepartmentChain(chain?.departmentId, params.departments);
  const displayName = departmentChain && name ? `${name} (${departmentChain})` : name;
  const id = params.user.userId ?? params.user.openId ?? params.user.unionId ?? params.user.userKey;
  return { id, name: displayName, departmentChain };
}

async function listOrgUsers(params: {
  account: ResolvedLarkAccount;
  query?: string | null;
  limit?: number | null;
  env?: LarkRuntimeEnv;
}): Promise<OrgUserProfile[]> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return [];
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account);
  const limit = Math.max(1, params.limit ?? 50);
  const q = params.query?.trim();

  const rows: Array<Record<string, unknown>> = [];
  if (q) {
    const like = `%${q}%`;
    const [result] = await context.pool.execute(
      `SELECT user_key, open_id, user_id, union_id, name
       FROM ${context.tables.orgUser}
       WHERE tenant_key = ?
         AND (name LIKE ? OR email LIKE ? OR user_id LIKE ? OR open_id LIKE ? OR union_id LIKE ?)
       ORDER BY name ASC
       LIMIT ?`,
      [tenantKey, like, like, like, like, like, limit],
    );
    rows.push(...(result as Array<Record<string, unknown>>));
  } else {
    const [result] = await context.pool.execute(
      `SELECT user_key, open_id, user_id, union_id, name
       FROM ${context.tables.orgUser}
       WHERE tenant_key = ?
       ORDER BY name ASC
       LIMIT ?`,
      [tenantKey, limit],
    );
    rows.push(...(result as Array<Record<string, unknown>>));
  }

  const users: OrgUserRow[] = rows.map((row) => ({
    userKey: typeof row.user_key === "string" ? row.user_key : "",
    openId: typeof row.open_id === "string" ? row.open_id : undefined,
    userId: typeof row.user_id === "string" ? row.user_id : undefined,
    unionId: typeof row.union_id === "string" ? row.union_id : undefined,
    name: typeof row.name === "string" ? row.name : undefined,
  }));

  const userKeys = users.map((user) => user.userKey).filter(Boolean);
  const [relations, departments] = await Promise.all([
    loadRelations({ context, tenantKey, userKeys }),
    loadDepartments({ context, tenantKey }),
  ]);
  const relationsByUser = new Map<string, OrgRelationRow[]>();
  for (const relation of relations) {
    const list = relationsByUser.get(relation.userKey) ?? [];
    list.push(relation);
    relationsByUser.set(relation.userKey, list);
  }

  return users
    .filter((user) => Boolean(user.userKey))
    .map((user) =>
      formatOrgUserDisplay({
        user,
        relations: relationsByUser.get(user.userKey) ?? [],
        departments,
      }),
    );
}

async function listOrgGroups(params: {
  account: ResolvedLarkAccount;
  query?: string | null;
  limit?: number | null;
  env?: LarkRuntimeEnv;
}): Promise<OrgGroupProfile[]> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return [];
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account);
  const limit = Math.max(1, params.limit ?? 50);
  const q = params.query?.trim();
  const rows: Array<Record<string, unknown>> = [];
  if (q) {
    const like = `%${q}%`;
    const [result] = await context.pool.execute(
      `SELECT chat_id, name
       FROM ${context.tables.imChat}
       WHERE tenant_key = ?
         AND (name LIKE ? OR chat_id LIKE ?)
       ORDER BY name ASC
       LIMIT ?`,
      [tenantKey, like, like, limit],
    );
    rows.push(...(result as Array<Record<string, unknown>>));
  } else {
    const [result] = await context.pool.execute(
      `SELECT chat_id, name
       FROM ${context.tables.imChat}
       WHERE tenant_key = ?
       ORDER BY name ASC
       LIMIT ?`,
      [tenantKey, limit],
    );
    rows.push(...(result as Array<Record<string, unknown>>));
  }
  return rows
    .map((row) => ({
      id: typeof row.chat_id === "string" ? row.chat_id : "",
      name: typeof row.name === "string" ? row.name : undefined,
    }))
    .filter((entry) => entry.id);
}

async function listOrgGroupMembers(params: {
  account: ResolvedLarkAccount;
  groupId: string;
  limit?: number | null;
  env?: LarkRuntimeEnv;
}): Promise<OrgUserProfile[]> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return [];
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account);
  const limit = Math.max(1, params.limit ?? 50);
  const [result] = await context.pool.execute(
    `SELECT m.user_key, m.open_id, m.user_id, m.union_id,
            COALESCE(m.name, u.name) AS name
     FROM ${context.tables.imChatMember} m
     LEFT JOIN ${context.tables.orgUser} u
       ON u.tenant_key = m.tenant_key AND u.user_key = m.user_key
     WHERE m.tenant_key = ? AND m.chat_id = ?
     ORDER BY name ASC
     LIMIT ?`,
    [tenantKey, params.groupId, limit],
  );
  const rows = result as Array<Record<string, unknown>>;
  const users: OrgUserRow[] = rows.map((row) => ({
    userKey: typeof row.user_key === "string" ? row.user_key : "",
    openId: typeof row.open_id === "string" ? row.open_id : undefined,
    userId: typeof row.user_id === "string" ? row.user_id : undefined,
    unionId: typeof row.union_id === "string" ? row.union_id : undefined,
    name: typeof row.name === "string" ? row.name : undefined,
  }));
  const userKeys = users.map((user) => user.userKey).filter(Boolean);
  const [relations, departments] = await Promise.all([
    loadRelations({ context, tenantKey, userKeys }),
    loadDepartments({ context, tenantKey }),
  ]);
  const relationsByUser = new Map<string, OrgRelationRow[]>();
  for (const relation of relations) {
    const list = relationsByUser.get(relation.userKey) ?? [];
    list.push(relation);
    relationsByUser.set(relation.userKey, list);
  }
  return users
    .filter((user) => Boolean(user.userKey))
    .map((user) =>
      formatOrgUserDisplay({
        user,
        relations: relationsByUser.get(user.userKey) ?? [],
        departments,
      }),
    );
}

export async function resolveLarkDirectorySelf(params: {
  cfg: ClawdbotConfig;
  accountId?: string | null;
  env?: LarkRuntimeEnv;
}): Promise<ChannelDirectoryEntry | null> {
  const account = resolveLarkAccount({ cfg: params.cfg, accountId: params.accountId });
  const botId = account.botUserId?.trim();
  if (!botId) return null;
  const profile = await resolveLarkSenderProfile({
    account,
    senderIds: { user_id: botId, open_id: botId, union_id: botId },
    env: params.env,
  });
  if (!profile) return { kind: "user", id: botId };
  return { kind: "user", id: profile.id, name: profile.name };
}

export async function listLarkDirectoryPeers(params: {
  cfg: ClawdbotConfig;
  accountId?: string | null;
  query?: string | null;
  limit?: number | null;
  env?: LarkRuntimeEnv;
}): Promise<ChannelDirectoryEntry[]> {
  const account = resolveLarkAccount({ cfg: params.cfg, accountId: params.accountId });
  const profiles = await listOrgUsers({
    account,
    query: params.query,
    limit: params.limit,
    env: params.env,
  });
  return profiles.map((profile) => ({
    kind: "user",
    id: profile.id,
    name: profile.name,
  }));
}

export async function listLarkDirectoryGroups(params: {
  cfg: ClawdbotConfig;
  accountId?: string | null;
  query?: string | null;
  limit?: number | null;
  env?: LarkRuntimeEnv;
}): Promise<ChannelDirectoryEntry[]> {
  const account = resolveLarkAccount({ cfg: params.cfg, accountId: params.accountId });
  const groups = await listOrgGroups({
    account,
    query: params.query,
    limit: params.limit,
    env: params.env,
  });
  return groups.map((group) => ({
    kind: "group",
    id: group.id,
    name: group.name,
  }));
}

export async function listLarkDirectoryGroupMembers(params: {
  cfg: ClawdbotConfig;
  accountId?: string | null;
  groupId: string;
  limit?: number | null;
  env?: LarkRuntimeEnv;
}): Promise<ChannelDirectoryEntry[]> {
  const account = resolveLarkAccount({ cfg: params.cfg, accountId: params.accountId });
  const members = await listOrgGroupMembers({
    account,
    groupId: params.groupId,
    limit: params.limit,
    env: params.env,
  });
  return members.map((member) => ({
    kind: "user",
    id: member.id,
    name: member.name,
  }));
}

export async function resolveLarkSenderProfile(params: {
  account: ResolvedLarkAccount;
  senderIds?: LarkUserIdBundle | null;
  env?: LarkRuntimeEnv;
}): Promise<OrgUserProfile | null> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return null;
  const userKey = resolveLarkUserKey(params.senderIds ?? null);
  if (!userKey) return null;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account);
  const [result] = await context.pool.execute(
    `SELECT user_key, open_id, user_id, union_id, name
     FROM ${context.tables.orgUser}
     WHERE tenant_key = ?
       AND (user_key = ? OR user_id = ? OR open_id = ? OR union_id = ?)
     LIMIT 1`,
    [tenantKey, userKey, userKey, userKey, userKey],
  );
  const rows = result as Array<Record<string, unknown>>;
  const row = rows[0];
  if (!row) return null;
  const user: OrgUserRow = {
    userKey: typeof row.user_key === "string" ? row.user_key : userKey,
    openId: typeof row.open_id === "string" ? row.open_id : undefined,
    userId: typeof row.user_id === "string" ? row.user_id : undefined,
    unionId: typeof row.union_id === "string" ? row.union_id : undefined,
    name: typeof row.name === "string" ? row.name : undefined,
  };
  const [relations, departments] = await Promise.all([
    loadRelations({ context, tenantKey, userKeys: [user.userKey] }),
    loadDepartments({ context, tenantKey }),
  ]);
  return formatOrgUserDisplay({
    user,
    relations,
    departments,
  });
}

export async function resolveLarkChatProfile(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  env?: LarkRuntimeEnv;
}): Promise<ChatProfile | null> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return null;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account);
  const [result] = await context.pool.execute(
    `SELECT name, member_count
     FROM ${context.tables.imChat}
     WHERE tenant_key = ? AND chat_id = ?
     LIMIT 1`,
    [tenantKey, params.chatId],
  );
  const rows = result as Array<Record<string, unknown>>;
  const row = rows[0];
  if (!row) return null;
  return {
    name: typeof row.name === "string" ? row.name : undefined,
    memberCount:
      typeof row.member_count === "number" && Number.isFinite(row.member_count)
        ? row.member_count
        : undefined,
  };
}
