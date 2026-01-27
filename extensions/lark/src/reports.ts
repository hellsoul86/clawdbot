import type { ResolvedLarkAccount } from "./accounts.js";
import { listLarkAccountIds, resolveLarkAccount } from "./accounts.js";
import { resolveLarkTenantKey } from "./ids.js";
import { sendLarkMessage } from "./messages.js";
import { ensureLarkSchema, resolveLarkMySqlContext } from "./mysql.js";
import type { LarkRuntimeEnv } from "./types.js";

const DAY_MS = 24 * 60 * 60 * 1000;

async function buildReport(params: {
  account: ResolvedLarkAccount;
  tenantKey: string;
  sinceMs: number;
  env?: LarkRuntimeEnv;
}): Promise<string> {
  const context = resolveLarkMySqlContext(params.account);
  if (!context) return "No report data (storage disabled).";
  await ensureLarkSchema(context, params.env);
  const [taskRows] = await context.pool.execute(
    `SELECT title, status, classification
     FROM ${context.tables.task}
     WHERE tenant_key = ? AND created_at >= FROM_UNIXTIME(?)
     ORDER BY created_at DESC
     LIMIT 20`,
    [params.tenantKey, Math.floor(params.sinceMs / 1000)],
  );
  const tasks = (taskRows as Array<Record<string, unknown>>).map((row) => ({
    title: String(row.title ?? ""),
    status: String(row.status ?? ""),
    classification: String(row.classification ?? ""),
  }));
  const [memoryRows] = await context.pool.execute(
    `SELECT content
     FROM ${context.tables.memoryItem}
     WHERE tenant_key = ? AND created_at >= FROM_UNIXTIME(?)
     ORDER BY created_at DESC
     LIMIT 10`,
    [params.tenantKey, Math.floor(params.sinceMs / 1000)],
  );
  const memories = (memoryRows as Array<Record<string, unknown>>)
    .map((row) => (typeof row.content === "string" ? row.content : ""))
    .filter(Boolean);

  const lines: string[] = [];
  lines.push("Report");
  if (tasks.length > 0) {
    lines.push("\nTasks:");
    for (const task of tasks) {
      lines.push(`- [${task.status}] ${task.title}`);
    }
  }
  if (memories.length > 0) {
    lines.push("\nMemory highlights:");
    for (const item of memories) {
      const snippet = item.replace(/\s+/g, " ").slice(0, 140);
      lines.push(`- ${snippet}`);
    }
  }
  if (lines.length === 1) {
    lines.push("\nNo activity recorded.");
  }
  return lines.join("\n");
}

async function sendReport(params: {
  account: ResolvedLarkAccount;
  targetChatId: string;
  sinceMs: number;
  env?: LarkRuntimeEnv;
}): Promise<void> {
  const tenantKey = resolveLarkTenantKey(params.account);
  const text = await buildReport({
    account: params.account,
    tenantKey,
    sinceMs: params.sinceMs,
    env: params.env,
  });
  await sendLarkMessage({
    account: params.account,
    receiveIdType: "chat_id",
    receiveId: params.targetChatId,
    text,
    tenantKey,
  });
}

export function createLarkReportService() {
  const timers = new Map<string, NodeJS.Timeout>();
  return {
    id: "lark-reports",
    start: (ctx: { config: unknown; logger: { info: (msg: string) => void; error: (msg: string) => void } }) => {
      const cfg = ctx.config as any;
      const ids = listLarkAccountIds(cfg ?? {});
      for (const accountId of ids) {
        const account = resolveLarkAccount({ cfg, accountId });
        if (!account) continue;
        const reports = account.config.reports;
        if (!reports || reports.enabled !== true) continue;
        const env: LarkRuntimeEnv = {
          log: ctx.logger.info,
          error: ctx.logger.error,
        };
        if (reports.dailyChatId) {
          const run = () => {
            void sendReport({
              account,
              targetChatId: reports.dailyChatId,
              sinceMs: Date.now() - DAY_MS,
              env,
            }).catch((err) => {
              env.error?.(`[lark] daily report failed: ${String(err)}`);
            });
          };
          run();
          timers.set(`daily:${account.accountId}`, setInterval(run, DAY_MS));
        }
        if (reports.weeklyChatId) {
          const run = () => {
            void sendReport({
              account,
              targetChatId: reports.weeklyChatId,
              sinceMs: Date.now() - DAY_MS * 7,
              env,
            }).catch((err) => {
              env.error?.(`[lark] weekly report failed: ${String(err)}`);
            });
          };
          run();
          timers.set(`weekly:${account.accountId}`, setInterval(run, DAY_MS * 7));
        }
        if (reports.monthlyChatId) {
          const run = () => {
            void sendReport({
              account,
              targetChatId: reports.monthlyChatId,
              sinceMs: Date.now() - DAY_MS * 30,
              env,
            }).catch((err) => {
              env.error?.(`[lark] monthly report failed: ${String(err)}`);
            });
          };
          run();
          timers.set(`monthly:${account.accountId}`, setInterval(run, DAY_MS * 30));
        }
      }
    },
    stop: () => {
      for (const timer of timers.values()) clearInterval(timer);
      timers.clear();
    },
  };
}
