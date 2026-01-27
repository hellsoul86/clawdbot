import type { ClawdbotPluginService } from "clawdbot/plugin-sdk";

import type { ResolvedLarkAccount } from "./accounts.js";
import { listLarkAccountIds, resolveLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey, resolveLarkUserKey, type LarkUserIdBundle } from "./ids.js";
import { coerceLarkPage, collectAllPages, requestLarkApi, type LarkPage } from "./lark-api.js";
import { scheduleLarkExtraction } from "./extraction.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import { setLarkStateDir } from "./storage.js";
import type { LarkRuntimeEnv } from "./types.js";

type LarkDepartment = {
  department_id: string;
  name?: string;
  parent_department_id?: string;
  leader_user_id?: string;
  status?: string;
  member_count?: number;
};

type LarkUser = {
  user_id?: string;
  open_id?: string;
  union_id?: string;
  name?: string;
  email?: string;
  mobile?: string;
  job_title?: string;
  status?: string;
  department_ids?: string[];
};

type DirectorySyncOptions = {
  rootDepartmentId: string;
  userIdType: "open_id" | "user_id" | "union_id";
  departmentIdType: "department_id" | "open_department_id";
};

const DEFAULT_ROOT_DEPARTMENT_ID = "0";
const DEFAULT_SYNC_INTERVAL_MINUTES = 360;
const MAX_PAGE_SIZE = 50;
const WRITE_BATCH_SIZE = 200;

function resolveDirectorySyncOptions(account: ResolvedLarkAccount): DirectorySyncOptions {
  const sync = account.config.directorySync;
  const rootDepartmentId = sync?.rootDepartmentId?.trim() || DEFAULT_ROOT_DEPARTMENT_ID;
  const userIdType = sync?.userIdType === "user_id" || sync?.userIdType === "union_id"
    ? sync.userIdType
    : "open_id";
  const departmentIdType =
    sync?.departmentIdType === "open_department_id" ? "open_department_id" : "department_id";
  return { rootDepartmentId, userIdType, departmentIdType };
}

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
    .filter(Boolean);
}

function normalizeLarkDepartment(raw: Record<string, unknown>): LarkDepartment | null {
  const departmentId =
    typeof raw.department_id === "string"
      ? raw.department_id
      : typeof raw.id === "string"
        ? raw.id
        : "";
  if (!departmentId) return null;
  return {
    department_id: departmentId,
    name: typeof raw.name === "string" ? raw.name : undefined,
    parent_department_id:
      typeof raw.parent_department_id === "string" ? raw.parent_department_id : undefined,
    leader_user_id:
      typeof raw.leader_user_id === "string" ? raw.leader_user_id : undefined,
    status: typeof raw.status === "string" ? raw.status : undefined,
    member_count:
      typeof raw.member_count === "number" && Number.isFinite(raw.member_count)
        ? raw.member_count
        : undefined,
  };
}

function normalizeLarkUser(raw: Record<string, unknown>): LarkUser {
  const departmentIds = normalizeStringArray(raw.department_ids);
  if (departmentIds.length === 0 && typeof raw.department_id === "string") {
    departmentIds.push(raw.department_id);
  }
  return {
    user_id: typeof raw.user_id === "string" ? raw.user_id : undefined,
    open_id: typeof raw.open_id === "string" ? raw.open_id : undefined,
    union_id: typeof raw.union_id === "string" ? raw.union_id : undefined,
    name:
      typeof raw.name === "string"
        ? raw.name
        : typeof raw.en_name === "string"
          ? raw.en_name
          : undefined,
    email:
      typeof raw.email === "string"
        ? raw.email
        : typeof raw.enterprise_email === "string"
          ? raw.enterprise_email
          : undefined,
    mobile: typeof raw.mobile === "string" ? raw.mobile : undefined,
    job_title: typeof raw.job_title === "string" ? raw.job_title : undefined,
    status: typeof raw.status === "string" ? raw.status : undefined,
    department_ids: departmentIds,
  };
}

async function listDepartmentChildren(params: {
  account: ResolvedLarkAccount;
  parentId: string;
  options: DirectorySyncOptions;
  pageToken?: string;
}): Promise<LarkPage<LarkDepartment>> {
  const data = await requestLarkApi<unknown>(params.account, {
    url: `/open-apis/contact/v3/departments/${encodeURIComponent(params.parentId)}/children`,
    method: "GET",
    params: {
      page_size: MAX_PAGE_SIZE,
      page_token: params.pageToken,
      department_id_type: params.options.departmentIdType,
      user_id_type: params.options.userIdType,
    },
  });
  const page = coerceLarkPage<Record<string, unknown>>(data);
  return {
    items: page.items.map((item) => normalizeLarkDepartment(item)).filter(Boolean) as LarkDepartment[],
    hasMore: page.hasMore,
    nextPageToken: page.nextPageToken,
  };
}

async function getDepartment(params: {
  account: ResolvedLarkAccount;
  departmentId: string;
  options: DirectorySyncOptions;
}): Promise<LarkDepartment | null> {
  const data = await requestLarkApi<Record<string, unknown>>(params.account, {
    url: `/open-apis/contact/v3/departments/${encodeURIComponent(params.departmentId)}`,
    method: "GET",
    params: {
      department_id_type: params.options.departmentIdType,
      user_id_type: params.options.userIdType,
    },
  });
  const raw =
    data && typeof data === "object"
      ? (data as Record<string, unknown>).department ?? (data as Record<string, unknown>).data ?? data
      : data;
  if (!raw || typeof raw !== "object") return null;
  return normalizeLarkDepartment(raw as Record<string, unknown>);
}

async function listDepartmentUsers(params: {
  account: ResolvedLarkAccount;
  departmentId: string;
  options: DirectorySyncOptions;
  pageToken?: string;
}): Promise<LarkPage<LarkUser>> {
  const data = await requestLarkApi<unknown>(params.account, {
    url: "/open-apis/contact/v3/users",
    method: "GET",
    params: {
      department_id: params.departmentId,
      page_size: MAX_PAGE_SIZE,
      page_token: params.pageToken,
      user_id_type: params.options.userIdType,
      department_id_type: params.options.departmentIdType,
      fetch_child: false,
    },
  });
  const page = coerceLarkPage<Record<string, unknown>>(data);
  return {
    items: page.items.map((item) => normalizeLarkUser(item)),
    hasMore: page.hasMore,
    nextPageToken: page.nextPageToken,
  };
}

async function fetchAllDepartments(
  account: ResolvedLarkAccount,
  options: DirectorySyncOptions,
  env?: LarkRuntimeEnv,
): Promise<LarkDepartment[]> {
  const queue = [options.rootDepartmentId];
  const visited = new Set<string>();
  const results: LarkDepartment[] = [];

  while (queue.length > 0) {
    const parentId = queue.shift();
    if (!parentId || visited.has(parentId)) continue;
    visited.add(parentId);
    const children = await collectAllPages((pageToken) =>
      listDepartmentChildren({ account, parentId, options, pageToken }),
    );
    for (const dept of children) {
      if (!dept.department_id) continue;
      results.push(dept);
      if (!visited.has(dept.department_id)) {
        queue.push(dept.department_id);
      }
    }
  }

  if (!results.some((dept) => dept.department_id === options.rootDepartmentId)) {
    try {
      const root = await getDepartment({ account, departmentId: options.rootDepartmentId, options });
      if (root) results.unshift(root);
    } catch (err) {
      env?.error?.(`[lark] failed fetching root department: ${String(err)}`);
      results.unshift({
        department_id: options.rootDepartmentId,
        name: "Root Department",
      });
    }
  }

  return results;
}

async function fetchUsersForDepartments(params: {
  account: ResolvedLarkAccount;
  departments: LarkDepartment[];
  options: DirectorySyncOptions;
  env?: LarkRuntimeEnv;
}): Promise<{
  users: Map<string, LarkUser>;
  relations: Array<{ userKey: string; departmentId: string; isPrimary: boolean }>;
}> {
  const users = new Map<string, LarkUser>();
  const relations: Array<{ userKey: string; departmentId: string; isPrimary: boolean }> = [];

  for (const dept of params.departments) {
    const departmentId = dept.department_id;
    if (!departmentId) continue;
    try {
      const members = await collectAllPages((pageToken) =>
        listDepartmentUsers({
          account: params.account,
          departmentId,
          options: params.options,
          pageToken,
        }),
      );
      for (const member of members) {
        const userKey = resolveLarkUserKey(member as LarkUserIdBundle);
        if (!userKey) continue;
        if (!users.has(userKey)) users.set(userKey, member);
        const primary = member.department_ids?.[0];
        relations.push({
          userKey,
          departmentId,
          isPrimary: primary ? primary === departmentId : false,
        });
      }
    } catch (err) {
      params.env?.error?.(`[lark] failed fetching users for department ${departmentId}: ${String(err)}`);
    }
  }

  return { users, relations };
}

function chunkRows<T>(rows: T[], size: number): T[][] {
  if (rows.length <= size) return [rows];
  const chunks: T[][] = [];
  for (let i = 0; i < rows.length; i += size) {
    chunks.push(rows.slice(i, i + size));
  }
  return chunks;
}

export async function runLarkDirectorySync(params: {
  account: ResolvedLarkAccount;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  if (params.account.config.directorySync?.enabled === false) return;

  await ensureLarkSchema(context, params.env);
  const options = resolveDirectorySyncOptions(params.account);
  const tenantKey = resolveLarkTenantKey(params.account);

  params.env?.log?.(`[lark] directory sync started (${params.account.accountId})`);

  const departments = await fetchAllDepartments(params.account, options, params.env);
  const { users, relations } = await fetchUsersForDepartments({
    account: params.account,
    departments,
    options,
    env: params.env,
  });

  const conn = await context.pool.getConnection();
  try {
    await conn.beginTransaction();

    const now = new Date();
    if (departments.length > 0) {
      const rows = departments
        .map((dept) => [
          tenantKey,
          dept.department_id,
          dept.name?.trim() || dept.department_id,
          dept.parent_department_id ?? null,
          dept.leader_user_id ?? null,
          dept.status ?? null,
          dept.member_count ?? null,
          now,
        ])
        .filter((row) => row[1]);
      const cols = [
        "tenant_key",
        "department_id",
        "name",
        "parent_department_id",
        "leader_user_id",
        "status",
        "member_count",
        "synced_at",
      ];
      for (const batch of chunkRows(rows, WRITE_BATCH_SIZE)) {
        const placeholders = batch.map(() => `(${cols.map(() => "?").join(",")})`).join(",");
        const sql = `INSERT INTO ${context.tables.orgDepartment} (${cols.join(",")}) VALUES ${placeholders}
          ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            parent_department_id = VALUES(parent_department_id),
            leader_user_id = VALUES(leader_user_id),
            status = VALUES(status),
            member_count = VALUES(member_count),
            synced_at = VALUES(synced_at)`;
        await conn.execute(sql, batch.flat());
      }
    }

    if (users.size > 0) {
      const rows = Array.from(users.entries()).map(([userKey, user]) => [
        tenantKey,
        userKey,
        user.open_id ?? null,
        user.user_id ?? null,
        user.union_id ?? null,
        user.name ?? null,
        user.email ?? null,
        user.mobile ?? null,
        user.status ?? null,
        user.job_title ?? null,
        now,
      ]);
      const cols = [
        "tenant_key",
        "user_key",
        "open_id",
        "user_id",
        "union_id",
        "name",
        "email",
        "mobile",
        "status",
        "job_title",
        "synced_at",
      ];
      for (const batch of chunkRows(rows, WRITE_BATCH_SIZE)) {
        const placeholders = batch.map(() => `(${cols.map(() => "?").join(",")})`).join(",");
        const sql = `INSERT INTO ${context.tables.orgUser} (${cols.join(",")}) VALUES ${placeholders}
          ON DUPLICATE KEY UPDATE
            open_id = VALUES(open_id),
            user_id = VALUES(user_id),
            union_id = VALUES(union_id),
            name = VALUES(name),
            email = VALUES(email),
            mobile = VALUES(mobile),
            status = VALUES(status),
            job_title = VALUES(job_title),
            synced_at = VALUES(synced_at)`;
        await conn.execute(sql, batch.flat());
      }
    }

    await conn.execute(
      `DELETE FROM ${context.tables.orgUserDepartmentRel} WHERE tenant_key = ?`,
      [tenantKey],
    );
    if (relations.length > 0) {
      const rows = relations.map((rel) => [
        tenantKey,
        rel.userKey,
        rel.departmentId,
        rel.isPrimary ? 1 : 0,
      ]);
      const cols = ["tenant_key", "user_key", "department_id", "is_primary"];
      for (const batch of chunkRows(rows, WRITE_BATCH_SIZE)) {
        const placeholders = batch.map(() => `(${cols.map(() => "?").join(",")})`).join(",");
        const sql = `INSERT INTO ${context.tables.orgUserDepartmentRel} (${cols.join(",")}) VALUES ${placeholders}`;
        await conn.execute(sql, batch.flat());
      }
    }

    await conn.commit();
    params.env?.log?.(
      `[lark] directory sync complete (${departments.length} depts, ${users.size} users)`,
    );
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

export function createLarkDirectorySyncService(): ClawdbotPluginService {
  const timers = new Map<string, NodeJS.Timeout>();
  const inFlight = new Set<string>();

  return {
    id: "lark-directory-sync",
    start: async (ctx) => {
      setLarkStateDir(ctx.stateDir);
      const accountIds = listLarkAccountIds(ctx.config);
      for (const accountId of accountIds) {
        const account = resolveLarkAccount({ cfg: ctx.config, accountId });
        if (!account.enabled) continue;
        if (account.config.directorySync?.enabled === false) continue;
        if (!resolveLarkMySqlContext(account)) continue;
        const env: LarkRuntimeEnv = {
          log: (message) => ctx.logger.info(message),
          error: (message) => ctx.logger.error(message),
        };
        scheduleLarkExtraction({ account, env });
        const intervalMinutes =
          account.config.directorySync?.intervalMinutes ?? DEFAULT_SYNC_INTERVAL_MINUTES;
        const intervalMs = Math.max(10, intervalMinutes) * 60 * 1000;
        const run = async () => {
          if (inFlight.has(account.accountId)) return;
          inFlight.add(account.accountId);
          try {
            await runLarkDirectorySync({ account, env });
          } catch (err) {
            env.error?.(`[lark] directory sync failed: ${String(err)}`);
          } finally {
            inFlight.delete(account.accountId);
          }
        };
        void run();
        const timer = setInterval(() => {
          void run();
        }, intervalMs);
        timers.set(account.accountId, timer);
      }
    },
    stop: async () => {
      for (const timer of timers.values()) {
        clearInterval(timer);
      }
      timers.clear();
      inFlight.clear();
    },
  };
}
