import { createHash } from "node:crypto";

import type { ResolvedLarkAccount } from "./accounts.js";
import { parseTextContent, stripMentionTags } from "./content.js";
import { resolveLarkTenantKey, type LarkUserIdBundle } from "./ids.js";
import { coerceLarkPage, requestLarkApi } from "./lark-api.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import { syncLarkChatMetadata } from "./im-sync.js";
import type { LarkRuntimeEnv } from "./types.js";

type LarkHistoryMessage = {
  messageId: string;
  chatId: string;
  chatType?: string;
  messageType?: string;
  senderType?: string;
  senderIds?: LarkUserIdBundle;
  content?: string;
  createTimeMs?: number | null;
  threadId?: string;
  rootId?: string;
  raw: Record<string, unknown>;
};

const BOOTSTRAP_LIMIT = 100;

function parseTimestamp(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function normalizeSenderIds(raw?: Record<string, unknown> | null): LarkUserIdBundle | undefined {
  if (!raw) return undefined;
  const senderId =
    raw.sender_id && typeof raw.sender_id === "object" ? (raw.sender_id as Record<string, unknown>) : raw;
  const id = typeof senderId.id === "string" ? senderId.id : undefined;
  const idType = typeof senderId.id_type === "string" ? senderId.id_type : undefined;
  return {
    user_id:
      typeof senderId.user_id === "string"
        ? senderId.user_id
        : idType === "user_id"
          ? id
          : undefined,
    open_id:
      typeof senderId.open_id === "string"
        ? senderId.open_id
        : idType === "open_id"
          ? id
          : undefined,
    union_id:
      typeof senderId.union_id === "string"
        ? senderId.union_id
        : idType === "union_id"
          ? id
          : undefined,
  };
}

function normalizeHistoryMessage(raw: Record<string, unknown>): LarkHistoryMessage | null {
  const messageId =
    typeof raw.message_id === "string"
      ? raw.message_id
      : typeof raw.messageId === "string"
        ? raw.messageId
        : "";
  const chatId = typeof raw.chat_id === "string" ? raw.chat_id : "";
  if (!messageId || !chatId) return null;
  const sender = raw.sender && typeof raw.sender === "object" ? (raw.sender as Record<string, unknown>) : undefined;
  const body =
    raw.body && typeof raw.body === "object" ? (raw.body as Record<string, unknown>) : undefined;
  return {
    messageId,
    chatId,
    chatType: typeof raw.chat_type === "string" ? raw.chat_type : undefined,
    messageType:
      typeof raw.message_type === "string"
        ? raw.message_type
        : typeof raw.msg_type === "string"
          ? raw.msg_type
          : undefined,
    senderType: typeof sender?.sender_type === "string" ? sender?.sender_type : undefined,
    senderIds: normalizeSenderIds(sender),
    content:
      typeof raw.content === "string"
        ? raw.content
        : typeof body?.content === "string"
          ? body.content
          : undefined,
    createTimeMs: parseTimestamp(raw.create_time),
    threadId: typeof raw.thread_id === "string" ? raw.thread_id : undefined,
    rootId: typeof raw.root_id === "string" ? raw.root_id : undefined,
    raw,
  };
}

function computeDedupeHash(messageId: string, payload: string): string {
  return createHash("sha256").update(messageId).update(":").update(payload).digest("hex");
}

async function fetchRecentMessages(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
}): Promise<LarkHistoryMessage[]> {
  const data = await requestLarkApi<unknown>(params.account, {
    url: "/open-apis/im/v1/messages",
    method: "GET",
    params: {
      container_id_type: "chat",
      container_id: params.chatId,
      page_size: BOOTSTRAP_LIMIT,
      sort_type: "ByCreateTimeDesc",
    },
  }, params.tenantKey);
  const page = coerceLarkPage<Record<string, unknown>>(data);
  return page.items.map((item) => normalizeHistoryMessage(item)).filter(Boolean) as LarkHistoryMessage[];
}

async function isBootstrapDone(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
  env?: LarkRuntimeEnv;
}): Promise<boolean> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return true;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const [result] = await context.pool.execute(
    `SELECT bootstrap_done
     FROM ${context.tables.imChat}
     WHERE tenant_key = ? AND chat_id = ?
     LIMIT 1`,
    [tenantKey, params.chatId],
  );
  const rows = result as Array<Record<string, unknown>>;
  const row = rows[0];
  return Boolean(row?.bootstrap_done);
}

async function markBootstrapDone(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  await context.pool.execute(
    `UPDATE ${context.tables.imChat}
     SET bootstrap_done = 1
     WHERE tenant_key = ? AND chat_id = ?`,
    [tenantKey, params.chatId],
  );
}

async function persistHistoryMessage(params: {
  account: ResolvedLarkAccount;
  message: LarkHistoryMessage;
  tenantKey?: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const rawContent = params.message.content ?? "";
  const parsedText = parseTextContent(rawContent);
  const textContent = parsedText ? stripMentionTags(parsedText) : null;
  const rawEvent = JSON.stringify(params.message.raw);
  const dedupeHash = computeDedupeHash(params.message.messageId, rawEvent);
  const senderIds = params.message.senderIds ?? {};

  await context.pool.execute(
    `INSERT INTO ${context.tables.chat} (chat_id, chat_type, tenant_key, last_message_at_ms)
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       chat_type = VALUES(chat_type),
       tenant_key = VALUES(tenant_key),
       last_message_at_ms = VALUES(last_message_at_ms)`,
    [
      params.message.chatId,
      params.message.chatType ?? "group",
      tenantKey,
      params.message.createTimeMs,
    ],
  );

  const hasSenderId = Boolean(senderIds.open_id || senderIds.user_id || senderIds.union_id);
  if (hasSenderId) {
    await context.pool.execute(
      `INSERT INTO ${context.tables.user} (open_id, user_id, union_id, tenant_key, sender_type)
       VALUES (?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         open_id = VALUES(open_id),
         user_id = VALUES(user_id),
         union_id = VALUES(union_id),
         tenant_key = VALUES(tenant_key),
         sender_type = VALUES(sender_type)`,
      [
        senderIds.open_id ?? null,
        senderIds.user_id ?? null,
        senderIds.union_id ?? null,
        tenantKey,
        params.message.senderType ?? null,
      ],
    );
  }

  await context.pool.execute(
    `INSERT INTO ${context.tables.message} (
      message_id,
      chat_id,
      chat_type,
      message_type,
      sender_type,
      sender_open_id,
      sender_user_id,
      sender_union_id,
      tenant_key,
      thread_id,
      root_id,
      content,
      text_content,
      create_time_ms,
      dedupe_hash,
      raw_event
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      chat_id = VALUES(chat_id),
      chat_type = VALUES(chat_type),
      message_type = VALUES(message_type),
      sender_type = VALUES(sender_type),
      sender_open_id = VALUES(sender_open_id),
      sender_user_id = VALUES(sender_user_id),
      sender_union_id = VALUES(sender_union_id),
      tenant_key = VALUES(tenant_key),
      thread_id = VALUES(thread_id),
      root_id = VALUES(root_id),
      content = VALUES(content),
      text_content = VALUES(text_content),
      create_time_ms = VALUES(create_time_ms),
      dedupe_hash = VALUES(dedupe_hash),
      raw_event = VALUES(raw_event)`,
    [
      params.message.messageId,
      params.message.chatId,
      params.message.chatType ?? "group",
      params.message.messageType ?? "text",
      params.message.senderType ?? null,
      senderIds.open_id ?? null,
      senderIds.user_id ?? null,
      senderIds.union_id ?? null,
      tenantKey,
      params.message.threadId ?? null,
      params.message.rootId ?? null,
      rawContent,
      textContent,
      params.message.createTimeMs,
      dedupeHash,
      rawEvent,
    ],
  );
}

export async function bootstrapLarkChatHistory(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  tenantKey?: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  if (await isBootstrapDone(params)) return;
  await syncLarkChatMetadata({
    account: params.account,
    chatId: params.chatId,
    tenantKey: params.tenantKey,
    env: params.env,
  });
  const messages = await fetchRecentMessages({
    account: params.account,
    chatId: params.chatId,
    tenantKey: params.tenantKey,
  });
  for (const message of messages) {
    await persistHistoryMessage({
      account: params.account,
      message,
      tenantKey: params.tenantKey,
      env: params.env,
    });
  }
  await markBootstrapDone(params);
  params.env?.log?.(`[lark] bootstrap complete for chat ${params.chatId} (${messages.length} msgs)`);
}

function extractMemberIdCandidates(value: unknown): string[] {
  if (!value) return [];
  if (typeof value === "string") return [value];
  if (typeof value !== "object") return [];
  const raw = value as Record<string, unknown>;
  const candidates: string[] = [];
  if (typeof raw.user_id === "string") candidates.push(raw.user_id);
  if (typeof raw.open_id === "string") candidates.push(raw.open_id);
  if (typeof raw.union_id === "string") candidates.push(raw.union_id);
  if (typeof raw.member_id === "string") candidates.push(raw.member_id);
  if (raw.member_id && typeof raw.member_id === "object") {
    const nested = raw.member_id as Record<string, unknown>;
    if (typeof nested.user_id === "string") candidates.push(nested.user_id);
    if (typeof nested.open_id === "string") candidates.push(nested.open_id);
    if (typeof nested.union_id === "string") candidates.push(nested.union_id);
  }
  return candidates;
}

export async function maybeBootstrapFromMemberEvent(params: {
  account: ResolvedLarkAccount;
  event: Record<string, unknown>;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const botId = params.account.botUserId?.trim();
  if (!botId) return;
  const chatId = typeof params.event.chat_id === "string" ? params.event.chat_id : "";
  if (!chatId) return;
  const candidates = new Set<string>();
  const members = params.event.members;
  if (Array.isArray(members)) {
    for (const member of members) {
      for (const candidate of extractMemberIdCandidates(member)) {
        candidates.add(candidate);
      }
    }
  }
  const users = params.event.users;
  if (Array.isArray(users)) {
    for (const user of users) {
      if (user && typeof user === "object") {
        const userObj = user as Record<string, unknown>;
        for (const candidate of extractMemberIdCandidates(userObj.user_id ?? userObj)) {
          candidates.add(candidate);
        }
      }
    }
  }
  for (const candidate of extractMemberIdCandidates(params.event.member_id)) {
    candidates.add(candidate);
  }
  for (const candidate of extractMemberIdCandidates(params.event.user_id)) {
    candidates.add(candidate);
  }
  if (!candidates.has(botId)) return;
  await bootstrapLarkChatHistory({
    account: params.account,
    chatId,
    tenantKey: typeof params.event.tenant_key === "string" ? params.event.tenant_key : undefined,
    env: params.env,
  });
}
