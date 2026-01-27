import path from "node:path";
import { mkdir } from "node:fs/promises";

import type { ResolvedLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey } from "./ids.js";
import { getLarkClient, resolveTenantOptions } from "./client.js";
import { scheduleLarkExtraction } from "./extraction.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import { ensureLarkResourceDir } from "./storage.js";
import type { LarkMessageEvent, LarkRuntimeEnv } from "./types.js";

type LarkResourceType = "image" | "file" | "audio" | "media" | "doc";

type LarkMessageResource = {
  messageId: string;
  chatId: string;
  resourceType: LarkResourceType;
  fileKey: string;
  fileName?: string;
  mimeType?: string;
  sizeBytes?: number;
};

type ResourceRow = {
  id: number;
  messageId: string;
  chatId: string;
  resourceType: LarkResourceType;
  fileKey: string;
  fileName?: string;
  sizeBytes?: number;
  attempts: number;
};

const MAX_ATTEMPTS = 3;
const DEFAULT_MAX_RESOURCE_MB = 100;
const DOWNLOAD_BATCH_SIZE = 10;

const downloadInFlight = new Set<string>();

function parseContentJson(raw: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function pickString(obj: Record<string, unknown>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === "string" && value.trim()) return value;
  }
  return undefined;
}

function pickNumber(obj: Record<string, unknown>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
  }
  return undefined;
}

function sanitizeFileName(raw: string): string {
  return raw.replace(/[\\/:*?"<>|]+/g, "_").trim() || "resource";
}

function extractResources(params: {
  messageId: string;
  chatId: string;
  messageType: string;
  content: string;
}): LarkMessageResource[] {
  const parsed = parseContentJson(params.content);
  if (!parsed) return [];
  const messageType = params.messageType.toLowerCase();
  const resources: LarkMessageResource[] = [];

  const imageKey = pickString(parsed, ["image_key", "imageKey"]);
  const fileKey = pickString(parsed, ["file_key", "fileKey"]);
  const mediaKey = pickString(parsed, ["media_key", "mediaKey"]);
  const fileName = pickString(parsed, ["file_name", "fileName", "name"]);
  const mimeType = pickString(parsed, ["mime_type", "mimeType", "file_type"]);
  const sizeBytes = pickNumber(parsed, ["file_size", "size", "fileSize"]);
  const docToken = pickString(parsed, [
    "doc_token",
    "docx_token",
    "doc_id",
    "docx_id",
    "doc_uuid",
    "doc_key",
  ]);

  if (imageKey || messageType === "image") {
    const fileKeyValue = imageKey ?? fileKey;
    if (fileKeyValue) {
      resources.push({
        messageId: params.messageId,
        chatId: params.chatId,
        resourceType: "image",
        fileKey: fileKeyValue,
        fileName,
        mimeType,
        sizeBytes,
      });
    }
  }

  const binaryKey = fileKey ?? mediaKey;
  if (binaryKey && ["file", "audio", "media"].includes(messageType)) {
    resources.push({
      messageId: params.messageId,
      chatId: params.chatId,
      resourceType: messageType as LarkResourceType,
      fileKey: binaryKey,
      fileName,
      mimeType,
      sizeBytes,
    });
  } else if (binaryKey && !imageKey) {
    resources.push({
      messageId: params.messageId,
      chatId: params.chatId,
      resourceType: "file",
      fileKey: binaryKey,
      fileName,
      mimeType,
      sizeBytes,
    });
  }

  if (docToken) {
    resources.push({
      messageId: params.messageId,
      chatId: params.chatId,
      resourceType: "doc",
      fileKey: docToken,
      fileName: fileName ?? pickString(parsed, ["title", "doc_title"]),
    });
  }

  return resources;
}

async function upsertResources(params: {
  account: ResolvedLarkAccount;
  resources: LarkMessageResource[];
  tenantKey: string;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  if (params.resources.length === 0) return;
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context, params.env);
  const cols = [
    "tenant_key",
    "message_id",
    "chat_id",
    "resource_type",
    "file_key",
    "file_name",
    "mime_type",
    "size_bytes",
    "status",
  ];
  const rows = params.resources.map((resource) => [
    params.tenantKey,
    resource.messageId,
    resource.chatId,
    resource.resourceType,
    resource.fileKey,
    resource.fileName ?? null,
    resource.mimeType ?? null,
    resource.sizeBytes ?? null,
    resource.resourceType === "doc" ? "linked" : "pending",
  ]);
  const placeholders = rows.map(() => `(${cols.map(() => "?").join(",")})`).join(",");
  const sql = `INSERT INTO ${context.tables.messageResource} (${cols.join(",")}) VALUES ${placeholders}
    ON DUPLICATE KEY UPDATE
      file_name = VALUES(file_name),
      mime_type = VALUES(mime_type),
      size_bytes = VALUES(size_bytes)`;
  await context.pool.execute(sql, rows.flat());
}

async function fetchPendingResources(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  env?: LarkRuntimeEnv;
}): Promise<ResourceRow[]> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return [];
  await ensureLarkSchema(context, params.env);
  const [rows] = await context.pool.execute(
    `SELECT id, message_id, chat_id, resource_type, file_key, file_name, size_bytes, attempts
     FROM ${context.tables.messageResource}
     WHERE tenant_key = ?
       AND status IN ("pending", "failed")
       AND attempts < ?
     ORDER BY created_at ASC
     LIMIT ?`,
    [params.tenantKey, MAX_ATTEMPTS, DOWNLOAD_BATCH_SIZE],
  );
  return (rows as Array<Record<string, unknown>>).map((row) => ({
    id: Number(row.id),
    messageId: String(row.message_id ?? ""),
    chatId: String(row.chat_id ?? ""),
    resourceType: (row.resource_type as LarkResourceType) ?? "file",
    fileKey: String(row.file_key ?? ""),
    fileName: typeof row.file_name === "string" ? row.file_name : undefined,
    sizeBytes:
      typeof row.size_bytes === "number" && Number.isFinite(row.size_bytes)
        ? row.size_bytes
        : typeof row.size_bytes === "string"
          ? Number(row.size_bytes)
          : undefined,
    attempts: typeof row.attempts === "number" ? row.attempts : Number(row.attempts ?? 0),
  }));
}

async function updateResourceStatus(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  id: number;
  status: string;
  storagePath?: string | null;
  storageUrl?: string | null;
  error?: string | null;
  attempts?: number;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await context.pool.execute(
    `UPDATE ${context.tables.messageResource}
     SET status = ?,
         storage_path = ?,
         storage_url = ?,
         error = ?,
         attempts = COALESCE(?, attempts),
         last_attempt_at = ?
     WHERE tenant_key = ? AND id = ?`,
    [
      params.status,
      params.storagePath ?? null,
      params.storageUrl ?? null,
      params.error ?? null,
      params.attempts ?? null,
      new Date(),
      params.tenantKey,
      params.id,
    ],
  );
}

async function downloadResource(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  resource: ResourceRow;
  maxBytes: number;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const resource = params.resource;
  if (resource.resourceType === "doc") return;
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  const baseDir = await ensureLarkResourceDir();
  const safeName = sanitizeFileName(resource.fileName ?? resource.fileKey);
  const messageDir = path.join(baseDir, resource.messageId);
  await mkdir(messageDir, { recursive: true });
  const filePath = path.join(messageDir, safeName);

  if (resource.sizeBytes && resource.sizeBytes > params.maxBytes) {
    await updateResourceStatus({
      account: params.account,
      tenantKey: params.tenantKey,
      id: resource.id,
      status: "too_large",
      error: `size ${resource.sizeBytes} exceeds limit`,
    });
    return;
  }

  const client = getLarkClient(params.account);
  const response = await client.im.messageResource.get(
    {
      path: {
        message_id: resource.messageId,
        file_key: resource.fileKey,
      },
      params: {
        type: resource.resourceType,
      },
    },
    resolveTenantOptions(params.tenantKey),
  );

  const lengthHeader = response.headers?.["content-length"] ?? response.headers?.["Content-Length"];
  const contentLength = typeof lengthHeader === "string" ? Number(lengthHeader) : undefined;
  if (contentLength && contentLength > params.maxBytes) {
    await updateResourceStatus({
      account: params.account,
      tenantKey: params.tenantKey,
      id: resource.id,
      status: "too_large",
      error: `size ${contentLength} exceeds limit`,
    });
    return;
  }

  await response.writeFile(filePath);
  await updateResourceStatus({
    account: params.account,
    tenantKey: params.tenantKey,
    id: resource.id,
    status: "ready",
    storagePath: filePath,
    storageUrl: filePath,
  });
  scheduleLarkExtraction({ account: params.account, env: params.env });
}

async function processResourceQueue(params: {
  account: ResolvedLarkAccount;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const tenantKey = resolveLarkTenantKey(params.account);
  const maxMb = params.account.config.resourceMaxMb ?? DEFAULT_MAX_RESOURCE_MB;
  const maxBytes = Math.max(1, maxMb) * 1024 * 1024;
  let pending = await fetchPendingResources({
    account: params.account,
    tenantKey,
    env: params.env,
  });
  while (pending.length > 0) {
    for (const resource of pending) {
      try {
        await updateResourceStatus({
          account: params.account,
          tenantKey,
          id: resource.id,
          status: "downloading",
          attempts: resource.attempts + 1,
        });
        await downloadResource({
          account: params.account,
          tenantKey,
          resource,
          maxBytes,
          env: params.env,
        });
      } catch (err) {
        await updateResourceStatus({
          account: params.account,
          tenantKey,
          id: resource.id,
          status: "failed",
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
    pending = await fetchPendingResources({
      account: params.account,
      tenantKey,
      env: params.env,
    });
  }
}

export function enqueueLarkMessageResources(params: {
  account: ResolvedLarkAccount;
  event: LarkMessageEvent;
  env?: LarkRuntimeEnv;
}): void {
  const message = params.event.message;
  const content = message.content ?? "";
  const resources = extractResources({
    messageId: message.message_id,
    chatId: message.chat_id,
    messageType: message.message_type,
    content,
  });
  if (resources.length === 0) return;

  const tenantKey = resolveLarkTenantKey(params.account, params.event.tenant_key);
  void upsertResources({
    account: params.account,
    resources,
    tenantKey,
    env: params.env,
  }).then(() => {
    scheduleResourceDownloads({ account: params.account, env: params.env });
  });
}

function scheduleResourceDownloads(params: {
  account: ResolvedLarkAccount;
  env?: LarkRuntimeEnv;
}): void {
  const key = params.account.accountId;
  if (downloadInFlight.has(key)) return;
  downloadInFlight.add(key);
  setImmediate(() => {
    void processResourceQueue(params)
      .catch((err) => {
        params.env?.error?.(`[lark] resource download failed: ${String(err)}`);
      })
      .finally(() => {
        downloadInFlight.delete(key);
      });
  });
}
