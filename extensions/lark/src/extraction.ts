import path from "node:path";
import { readFile } from "node:fs/promises";

import type { ResolvedLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey } from "./ids.js";
import { recordLarkMemoryFromExtraction } from "./memory.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import type { LarkRuntimeEnv } from "./types.js";

type ExtractionTask = {
  resourceId: number;
  messageId: string;
  resourceType: string;
  storagePath: string;
  fileName?: string;
};

const extractionInFlight = new Set<string>();
const EXTRACTION_BATCH_SIZE = 5;

function decodeXmlEntities(value: string): string {
  return value
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

async function extractDocxText(filePath: string): Promise<string> {
  const { default: JSZip } = await import("jszip");
  const buffer = await readFile(filePath);
  const zip = await JSZip.loadAsync(buffer);
  const doc = zip.file("word/document.xml");
  if (!doc) return "";
  const xml = await doc.async("string");
  const stripped = xml
    .replace(/<\/w:p>/g, "\n")
    .replace(/<w:tab\/>/g, "\t")
    .replace(/<[^>]+>/g, "")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n");
  return decodeXmlEntities(stripped).trim();
}

async function runOcr(params: {
  filePath: string;
  languages: string[];
}): Promise<{ text: string; model: string }> {
  const { recognize } = await import("tesseract.js");
  const lang = params.languages.length > 0 ? params.languages.join("+") : "eng";
  const result = await recognize(params.filePath, lang);
  return { text: result.data?.text ?? "", model: `tesseract:${lang}` };
}

async function runOpenAiAsr(params: {
  filePath: string;
  apiKey: string;
  model: string;
  language?: string;
}): Promise<{ text: string; model: string }> {
  const buffer = await readFile(params.filePath);
  const fileName = path.basename(params.filePath);
  const form = new FormData();
  form.append("file", new Blob([buffer]), fileName);
  form.append("model", params.model);
  if (params.language) form.append("language", params.language);
  const response = await fetch("https://api.openai.com/v1/audio/transcriptions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${params.apiKey}`,
    },
    body: form,
  });
  const json = (await response.json()) as { text?: string; error?: { message?: string } };
  if (!response.ok) {
    throw new Error(json.error?.message ?? `OpenAI transcription failed (${response.status})`);
  }
  return { text: json.text ?? "", model: params.model };
}

async function listPendingExtractions(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  env?: LarkRuntimeEnv;
}): Promise<ExtractionTask[]> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return [];
  await ensureLarkSchema(context, params.env);
  const [rows] = await context.pool.execute(
    `SELECT r.id AS resource_id,
            r.message_id,
            r.resource_type,
            r.storage_path,
            r.file_name
     FROM ${context.tables.messageResource} r
     LEFT JOIN ${context.tables.contentExtraction} e
       ON e.tenant_key = r.tenant_key AND e.resource_id = r.id
     WHERE r.tenant_key = ?
       AND r.status = "ready"
       AND (e.id IS NULL OR e.status = "failed")
     ORDER BY r.updated_at ASC
     LIMIT ?`,
    [params.tenantKey, EXTRACTION_BATCH_SIZE],
  );
  return (rows as Array<Record<string, unknown>>)
    .map((row) => ({
      resourceId: Number(row.resource_id),
      messageId: String(row.message_id ?? ""),
      resourceType: String(row.resource_type ?? ""),
      storagePath: String(row.storage_path ?? ""),
      fileName: typeof row.file_name === "string" ? row.file_name : undefined,
    }))
    .filter((row) => row.resourceId && row.storagePath);
}

async function upsertExtraction(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  messageId: string;
  resourceId: number;
  resourceType: string;
  status: string;
  text?: string | null;
  language?: string | null;
  model?: string | null;
  error?: string | null;
}): Promise<void> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return;
  await ensureLarkSchema(context);
  await context.pool.execute(
    `INSERT INTO ${context.tables.contentExtraction} (
      tenant_key,
      message_id,
      resource_id,
      resource_type,
      language,
      text,
      model,
      status,
      error
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      language = VALUES(language),
      text = VALUES(text),
      model = VALUES(model),
      status = VALUES(status),
      error = VALUES(error)`,
    [
      params.tenantKey,
      params.messageId,
      params.resourceId,
      params.resourceType,
      params.language ?? null,
      params.text ?? null,
      params.model ?? null,
      params.status,
      params.error ?? null,
    ],
  );
}

async function processExtractionTask(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  task: ExtractionTask;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const extraction = params.account.config.extraction;
  if (extraction?.enabled === false) return;
  const resourceType = params.task.resourceType.toLowerCase();

  if (resourceType === "image") {
    if (extraction?.ocr?.enabled === false) return;
    const languages = extraction?.ocr?.languages ?? [];
    const result = await runOcr({ filePath: params.task.storagePath, languages });
    await upsertExtraction({
      account: params.account,
      tenantKey: params.tenantKey,
      messageId: params.task.messageId,
      resourceId: params.task.resourceId,
      resourceType,
      status: result.text ? "ready" : "empty",
      text: result.text,
      language: languages[0] ?? null,
      model: result.model,
    });
    if (result.text) {
      await recordLarkMemoryFromExtraction({
        account: params.account,
        tenantKey: params.tenantKey,
        messageId: params.task.messageId,
        content: result.text,
        env: params.env,
      });
    }
    return;
  }

  if (resourceType === "audio") {
    if (extraction?.asr?.enabled === false) return;
    const apiKey = extraction?.asr?.apiKey?.trim() || process.env.OPENAI_API_KEY;
    if (!apiKey) {
      await upsertExtraction({
        account: params.account,
        tenantKey: params.tenantKey,
        messageId: params.task.messageId,
        resourceId: params.task.resourceId,
        resourceType,
        status: "failed",
        error: "missing OpenAI API key",
      });
      return;
    }
    const model = extraction?.asr?.model?.trim() || "gpt-4o-transcribe";
    const result = await runOpenAiAsr({
      filePath: params.task.storagePath,
      apiKey,
      model,
      language: extraction?.asr?.language,
    });
    await upsertExtraction({
      account: params.account,
      tenantKey: params.tenantKey,
      messageId: params.task.messageId,
      resourceId: params.task.resourceId,
      resourceType,
      status: result.text ? "ready" : "empty",
      text: result.text,
      language: extraction?.asr?.language ?? null,
      model: result.model,
    });
    if (result.text) {
      await recordLarkMemoryFromExtraction({
        account: params.account,
        tenantKey: params.tenantKey,
        messageId: params.task.messageId,
        content: result.text,
        env: params.env,
      });
    }
    return;
  }

  if (resourceType === "file") {
    if (extraction?.docx?.enabled === false) return;
    const fileName = params.task.fileName?.toLowerCase() ?? "";
    if (!fileName.endsWith(".docx")) return;
    const text = await extractDocxText(params.task.storagePath);
    await upsertExtraction({
      account: params.account,
      tenantKey: params.tenantKey,
      messageId: params.task.messageId,
      resourceId: params.task.resourceId,
      resourceType: "docx",
      status: text ? "ready" : "empty",
      text,
      model: "docx",
    });
    if (text) {
      await recordLarkMemoryFromExtraction({
        account: params.account,
        tenantKey: params.tenantKey,
        messageId: params.task.messageId,
        content: text,
        env: params.env,
      });
    }
    return;
  }
}

async function processExtractionQueue(params: {
  account: ResolvedLarkAccount;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const tenantKey = resolveLarkTenantKey(params.account);
  let pending = await listPendingExtractions({
    account: params.account,
    tenantKey,
    env: params.env,
  });
  while (pending.length > 0) {
    for (const task of pending) {
      try {
        await upsertExtraction({
          account: params.account,
          tenantKey,
          messageId: task.messageId,
          resourceId: task.resourceId,
          resourceType: task.resourceType,
          status: "processing",
        });
        await processExtractionTask({ account: params.account, tenantKey, task, env: params.env });
      } catch (err) {
        await upsertExtraction({
          account: params.account,
          tenantKey,
          messageId: task.messageId,
          resourceId: task.resourceId,
          resourceType: task.resourceType,
          status: "failed",
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
    pending = await listPendingExtractions({
      account: params.account,
      tenantKey,
      env: params.env,
    });
  }
}

export function scheduleLarkExtraction(params: {
  account: ResolvedLarkAccount;
  env?: LarkRuntimeEnv;
}): void {
  const key = params.account.accountId;
  if (extractionInFlight.has(key)) return;
  extractionInFlight.add(key);
  setImmediate(() => {
    void processExtractionQueue(params)
      .catch((err) => {
        params.env?.error?.(`[lark] extraction failed: ${String(err)}`);
      })
      .finally(() => {
        extractionInFlight.delete(key);
      });
  });
}
