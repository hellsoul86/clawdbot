import type { ResolvedLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey, resolveLarkUserKey, type LarkUserIdBundle } from "./ids.js";
import { collectAllPages, coerceLarkPage, requestLarkApi } from "./lark-api.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import type { LarkRuntimeEnv } from "./types.js";

type LarkChatInfo = {
  chat_id: string;
  name?: string;
  description?: string;
  owner?: LarkUserIdBundle;
  owner_id_type?: string;
  member_count?: number;
  chat_mode?: string;
  chat_type?: string;
};

type LarkChatMember = {
  ids: LarkUserIdBundle;
  name?: string;
  role?: string;
  is_owner?: boolean;
  is_admin?: boolean;
  joined_at?: Date | null;
};

const CHAT_SYNC_TTL_MS = 10 * 60 * 1000;
const MEMBER_PAGE_SIZE = 50;

const chatSyncCache = new Map<string, number>();

function resolveChatSyncKey(account: ResolvedLarkAccount, chatId: string): string {
  return `${account.accountId}:${chatId}`;
}

function normalizeOwnerIds(value: unknown): LarkUserIdBundle | undefined {
  if (!value || typeof value !== "object") return undefined;
  const raw = value as Record<string, unknown>;
  return {
    user_id: typeof raw.user_id === "string" ? raw.user_id : undefined,
    open_id: typeof raw.open_id === "string" ? raw.open_id : undefined,
    union_id: typeof raw.union_id === "string" ? raw.union_id : undefined,
  };
}

function normalizeChatInfo(raw: Record<string, unknown>): LarkChatInfo | null {
  const chatId =
    typeof raw.chat_id === "string"
      ? raw.chat_id
      : typeof raw.chatId === "string"
        ? raw.chatId
        : "";
  if (!chatId) return null;
  const owner = normalizeOwnerIds(raw.owner_id ?? raw.owner);
  return {
    chat_id: chatId,
    name: typeof raw.name === "string" ? raw.name : undefined,
    description: typeof raw.description === "string" ? raw.description : undefined,
    owner,
    owner_id_type: typeof raw.owner_id_type === "string" ? raw.owner_id_type : undefined,
    member_count:
      typeof raw.member_count === "number" && Number.isFinite(raw.member_count)
        ? raw.member_count
        : undefined,
    chat_mode: typeof raw.chat_mode === "string" ? raw.chat_mode : undefined,
    chat_type: typeof raw.chat_type === "string" ? raw.chat_type : undefined,
  };
}

function normalizeMemberIds(raw: Record<string, unknown>): LarkUserIdBundle {
  const ids: LarkUserIdBundle = {
    user_id: typeof raw.user_id === "string" ? raw.user_id : undefined,
    open_id: typeof raw.open_id === "string" ? raw.open_id : undefined,
    union_id: typeof raw.union_id === "string" ? raw.union_id : undefined,
  };
  if (!ids.user_id && !ids.open_id && !ids.union_id) {
    const fallback =
      typeof raw.member_id === "string"
        ? raw.member_id
        : typeof raw.id === "string"
          ? raw.id
          : undefined;
    if (fallback) ids.user_id = fallback;
  }
  return ids;
}

function normalizeChatMember(raw: Record<string, unknown>): LarkChatMember | null {
  const ids = normalizeMemberIds(raw);
  const userKey = resolveLarkUserKey(ids);
  if (!userKey) return null;
  const role =
    typeof raw.role === "string"
      ? raw.role
      : typeof raw.member_role === "string"
        ? raw.member_role
        : undefined;
  const joinedAtRaw =
    typeof raw.join_time === "number"
      ? raw.join_time
      : typeof raw.join_time === "string"
        ? Number(raw.join_time)
        : typeof raw.joined_at === "number"
          ? raw.joined_at
          : typeof raw.joined_at === "string"
            ? Number(raw.joined_at)
            : undefined;
  const joinedAt =
    typeof joinedAtRaw === "number" && Number.isFinite(joinedAtRaw)
      ? new Date(joinedAtRaw)
      : null;
  return {
    ids,
    name: typeof raw.name === "string" ? raw.name : undefined,
    role,
    is_owner: Boolean(raw.is_owner) || role === "owner",
    is_admin: Boolean(raw.is_admin) || role === "admin",
    joined_at: joinedAt,
  };
}

async function fetchChatInfo(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
}): Promise<LarkChatInfo | null> {
  const data = await requestLarkApi<Record<string, unknown>>(params.account, {
    url: `/open-apis/im/v1/chats/${encodeURIComponent(params.chatId)}`,
    method: "GET",
  }, params.tenantKey);
  const raw =
    data && typeof data === "object"
      ? (data as Record<string, unknown>).chat ?? (data as Record<string, unknown>).data ?? data
      : data;
  if (!raw || typeof raw !== "object") return null;
  return normalizeChatInfo(raw as Record<string, unknown>);
}

async function fetchChatMembers(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
}): Promise<LarkChatMember[]> {
  return await collectAllPages(async (pageToken) => {
    const data = await requestLarkApi<unknown>(params.account, {
      url: `/open-apis/im/v1/chats/${encodeURIComponent(params.chatId)}/members`,
      method: "GET",
      params: {
        page_size: MEMBER_PAGE_SIZE,
        page_token: pageToken,
        member_id_type: "open_id",
      },
    }, params.tenantKey);
    const page = coerceLarkPage<Record<string, unknown>>(data);
    return {
      items: page.items
        .map((item) => normalizeChatMember(item))
        .filter(Boolean) as LarkChatMember[],
      hasMore: page.hasMore,
      nextPageToken: page.nextPageToken,
    };
  });
}

export async function syncLarkChatMetadata(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const info = await fetchChatInfo({
    account: params.account,
    chatId: params.chatId,
    tenantKey: params.tenantKey,
  });
  if (!info) return;
  const ownerKey = resolveLarkUserKey(info.owner ?? null);
  await context.pool.execute(
    `INSERT INTO ${context.tables.imChat} (
      tenant_key,
      chat_id,
      name,
      description,
      owner_id,
      owner_id_type,
      member_count,
      chat_mode,
      chat_type,
      last_synced_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      name = VALUES(name),
      description = VALUES(description),
      owner_id = VALUES(owner_id),
      owner_id_type = VALUES(owner_id_type),
      member_count = VALUES(member_count),
      chat_mode = VALUES(chat_mode),
      chat_type = VALUES(chat_type),
      last_synced_at = VALUES(last_synced_at)`,
    [
      tenantKey,
      info.chat_id,
      info.name ?? null,
      info.description ?? null,
      ownerKey,
      info.owner_id_type ?? null,
      info.member_count ?? null,
      info.chat_mode ?? null,
      info.chat_type ?? null,
      new Date(),
    ],
  );
}

export async function syncLarkChatMembers(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const members = await fetchChatMembers({
    account: params.account,
    chatId: params.chatId,
    tenantKey: params.tenantKey,
  });
  await context.pool.execute(
    `DELETE FROM ${context.tables.imChatMember} WHERE tenant_key = ? AND chat_id = ?`,
    [tenantKey, params.chatId],
  );
  if (members.length === 0) return;
  const cols = [
    "tenant_key",
    "chat_id",
    "user_key",
    "open_id",
    "user_id",
    "union_id",
    "name",
    "role",
    "is_owner",
    "is_admin",
    "joined_at",
  ];
  const rows = members.map((member) => {
    const userKey = resolveLarkUserKey(member.ids) ?? "";
    return [
      tenantKey,
      params.chatId,
      userKey,
      member.ids.open_id ?? null,
      member.ids.user_id ?? null,
      member.ids.union_id ?? null,
      member.name ?? null,
      member.role ?? null,
      member.is_owner ? 1 : 0,
      member.is_admin ? 1 : 0,
      member.joined_at ?? null,
    ];
  }).filter((row) => Boolean(row[2]));
  const placeholders = rows.map(() => `(${cols.map(() => "?").join(",")})`).join(",");
  await context.pool.execute(
    `INSERT INTO ${context.tables.imChatMember} (${cols.join(",")}) VALUES ${placeholders}`,
    rows.flat(),
  );
}

export async function ensureLarkChatMetadata(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const key = resolveChatSyncKey(params.account, params.chatId);
  const last = chatSyncCache.get(key) ?? 0;
  if (Date.now() - last < CHAT_SYNC_TTL_MS) return;
  chatSyncCache.set(key, Date.now());
  try {
    await syncLarkChatMetadata(params);
  } catch (err) {
    params.env?.error?.(`[lark] chat metadata sync failed: ${String(err)}`);
  }
}

export async function handleLarkChatUpdatedEvent(params: {
  account: ResolvedLarkAccount;
  event: Record<string, unknown>;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const chatId = typeof params.event.chat_id === "string" ? params.event.chat_id : "";
  if (!chatId) return;
  const tenantKey = typeof params.event.tenant_key === "string" ? params.event.tenant_key : undefined;
  await syncLarkChatMetadata({
    account: params.account,
    chatId,
    tenantKey,
    env: params.env,
  });
}

export async function handleLarkChatMemberEvent(params: {
  account: ResolvedLarkAccount;
  event: Record<string, unknown>;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const chatId = typeof params.event.chat_id === "string" ? params.event.chat_id : "";
  if (!chatId) return;
  const tenantKey = typeof params.event.tenant_key === "string" ? params.event.tenant_key : undefined;
  await syncLarkChatMetadata({
    account: params.account,
    chatId,
    tenantKey,
    env: params.env,
  });
  await syncLarkChatMembers({
    account: params.account,
    chatId,
    tenantKey,
    env: params.env,
  });
}
