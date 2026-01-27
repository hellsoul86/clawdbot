import { createHash } from "node:crypto";

import type { ResolvedLarkAccount } from "./accounts.js";
import { parseTextContent, stripMentionTags } from "./content.js";
import {
  ensureLarkSchema,
  resolveLarkMySqlContext,
  type LarkMySqlContext,
} from "./mysql.js";
import type { LarkMessageEvent, LarkRuntimeEnv } from "./types.js";

const writeQueues = new Map<string, Promise<void>>();

function parseCreateTimeMs(raw?: string): number | null {
  if (!raw) return null;
  const value = Number(raw);
  if (!Number.isFinite(value)) return null;
  return value;
}

function computeDedupeHash(messageId: string, payload: string): string {
  return createHash("sha256").update(messageId).update(":").update(payload).digest("hex");
}

async function persistMessage(params: {
  context: LarkMySqlContext;
  event: LarkMessageEvent;
}): Promise<void> {
  const { event, context } = params;
  const message = event.message;
  const sender = event.sender;
  const senderId = sender.sender_id ?? {};
  const tenantKey = event.tenant_key ?? null;
  const createTimeMs = parseCreateTimeMs(message.create_time);
  const rawContent = message.content ?? "";
  const parsedText = parseTextContent(rawContent);
  const textContent = parsedText ? stripMentionTags(parsedText) : null;
  const rawEvent = JSON.stringify(event);
  const dedupeHash = computeDedupeHash(message.message_id, rawEvent);
  const tables = context.tables;

  await context.pool.execute(
    `INSERT INTO ${tables.chat} (chat_id, chat_type, tenant_key, last_message_at_ms)
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       chat_type = VALUES(chat_type),
       tenant_key = VALUES(tenant_key),
       last_message_at_ms = VALUES(last_message_at_ms)`,
    [message.chat_id, message.chat_type, tenantKey, createTimeMs],
  );

  const hasSenderId = Boolean(senderId.open_id || senderId.user_id || senderId.union_id);
  if (hasSenderId) {
    await context.pool.execute(
      `INSERT INTO ${tables.user} (open_id, user_id, union_id, tenant_key, sender_type)
       VALUES (?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         open_id = VALUES(open_id),
         user_id = VALUES(user_id),
         union_id = VALUES(union_id),
         tenant_key = VALUES(tenant_key),
         sender_type = VALUES(sender_type)`,
      [
        senderId.open_id ?? null,
        senderId.user_id ?? null,
        senderId.union_id ?? null,
        tenantKey,
        sender.sender_type,
      ],
    );
  }

  await context.pool.execute(
    `INSERT INTO ${tables.message} (
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
      message.message_id,
      message.chat_id,
      message.chat_type,
      message.message_type,
      sender.sender_type,
      senderId.open_id ?? null,
      senderId.user_id ?? null,
      senderId.union_id ?? null,
      tenantKey,
      message.thread_id ?? null,
      message.root_id ?? null,
      rawContent,
      textContent,
      createTimeMs,
      dedupeHash,
      rawEvent,
    ],
  );
}

function enqueueWrite(params: {
  key: string;
  env?: LarkRuntimeEnv;
  task: () => Promise<void>;
}): void {
  const existing = writeQueues.get(params.key) ?? Promise.resolve();
  const next = existing
    .catch(() => undefined)
    .then(params.task)
    .catch((err) => {
      params.env?.error?.(`[lark] mysql write failed: ${String(err)}`);
    });
  writeQueues.set(params.key, next);
}

export function enqueueLarkMessagePersist(params: {
  account: ResolvedLarkAccount;
  event: LarkMessageEvent;
  env?: LarkRuntimeEnv;
}): void {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  enqueueWrite({
    key: context.key,
    env: params.env,
    task: async () => {
      await ensureLarkSchema(context, params.env);
      await persistMessage({ context, event: params.event });
    },
  });
}
