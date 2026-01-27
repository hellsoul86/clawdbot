import type { ResolvedLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey, resolveLarkUserKey, type LarkUserIdBundle } from "./ids.js";
import { sendLarkMessage } from "./messages.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import type { LarkRuntimeEnv } from "./types.js";

type TaskClassification = "automated" | "confirm" | "unsupported";

const CONFIRM_KEYWORDS = [/^(confirm|yes|yep|sure)\b/i, /^(确认|可以|好的)\b/];
const DECLINE_KEYWORDS = [/^(no|nope|skip)\b/i, /^(不用|忽略|取消)\b/];

const AUTOMATED_PATTERNS = [
  /meeting notes/i,
  /summar(i|y)ze/i,
  /\bsummary\b/i,
  /\brecap\b/i,
  /transcrib/i,
  /纪要/,
  /总结/,
  /整理/,
  /汇总/,
];

const CONFIRM_PATTERNS = [
  /\bapprove\b/i,
  /\bdeploy\b/i,
  /\bpurchase\b/i,
  /\bbuy\b/i,
  /\bpayment\b/i,
  /发布/,
  /审批/,
  /采购/,
  /转账/,
];

const UNSUPPORTED_PATTERNS = [
  /\blegal\b/i,
  /\bterminate\b/i,
  /\bdelete\b/i,
  /解雇/,
  /删库/,
  /删除生产/,
];

function classifyTask(content: string): TaskClassification | null {
  const trimmed = content.trim();
  if (!trimmed) return null;
  const hasRequest =
    /^\s*(please|can you|could you|pls)\b/i.test(trimmed) ||
    /帮(我|忙)/.test(trimmed) ||
    /^请/.test(trimmed);
  if (!hasRequest) return null;
  if (UNSUPPORTED_PATTERNS.some((pattern) => pattern.test(trimmed))) return "unsupported";
  if (AUTOMATED_PATTERNS.some((pattern) => pattern.test(trimmed))) return "automated";
  if (CONFIRM_PATTERNS.some((pattern) => pattern.test(trimmed))) return "confirm";
  return "confirm";
}

function buildTaskTitle(content: string): string {
  const trimmed = content.trim().replace(/\s+/g, " ");
  return trimmed.length > 240 ? `${trimmed.slice(0, 237)}...` : trimmed;
}

export async function recordLarkTaskFromMessage(params: {
  account: ResolvedLarkAccount;
  tenantKey?: string;
  chatId: string;
  chatType: "direct" | "group";
  senderId: string;
  senderIds?: LarkUserIdBundle | null;
  messageId: string;
  content: string;
  wasMentioned?: boolean;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const classification = classifyTask(params.content);
  if (!classification) return;
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const userKey = resolveLarkUserKey(params.senderIds ?? null) ?? params.senderId;
  const scopeType = params.chatType === "direct" ? "dm_user" : "group_user";
  const scopeId =
    params.chatType === "direct" ? params.senderId : `${params.chatId}:${params.senderId}`;

  const [existing] = await context.pool.execute(
    `SELECT id FROM ${context.tables.task} WHERE tenant_key = ? AND message_id = ? LIMIT 1`,
    [tenantKey, params.messageId],
  );
  if ((existing as Array<Record<string, unknown>>).length > 0) return;

  const status =
    classification === "unsupported"
      ? "unsupported"
      : classification === "confirm"
        ? "needs_confirmation"
        : "suggested";
  const title = buildTaskTitle(params.content);
  await context.pool.execute(
    `INSERT INTO ${context.tables.task} (
      tenant_key,
      scope_type,
      scope_id,
      chat_id,
      user_id,
      message_id,
      title,
      details,
      classification,
      status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantKey,
      scopeType,
      scopeId,
      params.chatId,
      userKey,
      params.messageId,
      title,
      params.content,
      classification,
      status,
    ],
  );

  if (classification === "unsupported") return;
  if (params.chatType === "group" && !params.wasMentioned) return;

  const suggestion =
    classification === "automated"
      ? `I can take this task: "${title}". Reply "confirm" to assign it to me.`
      : `This looks like a task requiring approval: "${title}". Reply "confirm" to assign it to me.`;
  const receiveIdType = params.senderIds?.open_id
    ? "open_id"
    : params.senderIds?.user_id
      ? "user_id"
      : params.senderIds?.union_id
        ? "union_id"
        : "open_id";
  const receiveId =
    params.senderIds?.open_id ??
    params.senderIds?.user_id ??
    params.senderIds?.union_id ??
    params.senderId;
  await sendLarkMessage({
    account: params.account,
    receiveIdType,
    receiveId,
    text: suggestion,
    tenantKey: params.tenantKey,
  });
}

export async function handleLarkTaskConfirmation(params: {
  account: ResolvedLarkAccount;
  tenantKey?: string;
  senderId: string;
  senderIds?: LarkUserIdBundle | null;
  content: string;
  env?: LarkRuntimeEnv;
}): Promise<boolean> {
  const trimmed = params.content.trim();
  if (!trimmed) return false;
  const isConfirm = CONFIRM_KEYWORDS.some((pattern) => pattern.test(trimmed));
  const isDecline = DECLINE_KEYWORDS.some((pattern) => pattern.test(trimmed));
  if (!isConfirm && !isDecline) return false;

  const context = resolveLarkMySqlContext(params.account);
  if (!context) return false;
  await ensureLarkSchema(context, params.env);
  const tenantKey = resolveLarkTenantKey(params.account, params.tenantKey);
  const userKey = resolveLarkUserKey(params.senderIds ?? null) ?? params.senderId;
  const [rows] = await context.pool.execute(
    `SELECT id, title, status
     FROM ${context.tables.task}
     WHERE tenant_key = ? AND user_id = ? AND status = "needs_confirmation"
     ORDER BY created_at DESC
     LIMIT 1`,
    [tenantKey, userKey],
  );
  const task = (rows as Array<Record<string, unknown>>)[0];
  if (!task) return false;
  const nextStatus = isConfirm ? "claimed" : "ignored";
  await context.pool.execute(
    `UPDATE ${context.tables.task}
     SET status = ?
     WHERE id = ?`,
    [nextStatus, task.id],
  );
  const response = isConfirm
    ? `Task confirmed: "${task.title}". I'll take it from here.`
    : `Got it. I won't proceed with "${task.title}".`;
  const receiveIdType = params.senderIds?.open_id
    ? "open_id"
    : params.senderIds?.user_id
      ? "user_id"
      : params.senderIds?.union_id
        ? "union_id"
        : "open_id";
  const receiveId =
    params.senderIds?.open_id ??
    params.senderIds?.user_id ??
    params.senderIds?.union_id ??
    params.senderId;
  await sendLarkMessage({
    account: params.account,
    receiveIdType,
    receiveId,
    text: response,
    tenantKey: params.tenantKey,
  });
  return true;
}
