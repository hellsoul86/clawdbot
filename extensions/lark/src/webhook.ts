import type { IncomingMessage, ServerResponse } from "node:http";

import * as lark from "@larksuiteoapi/node-sdk";
import type { ClawdbotConfig } from "clawdbot/plugin-sdk";

import type { ResolvedLarkAccount } from "./accounts.js";
import { handleLarkMessageEvent } from "./inbound.js";
import { bootstrapLarkChatHistory, maybeBootstrapFromMemberEvent } from "./history.js";
import { handleLarkChatMemberEvent, handleLarkChatUpdatedEvent } from "./im-sync.js";
import type { LarkRuntimeEnv } from "./types.js";
import { getLarkRuntime } from "./runtime.js";

type WebhookTarget = {
  account: ResolvedLarkAccount;
  config: ClawdbotConfig;
  env: LarkRuntimeEnv;
  dispatcher: lark.EventDispatcher;
  path: string;
  statusSink?: (patch: { lastInboundAt?: number; lastOutboundAt?: number }) => void;
};

const webhookTargets = new Map<string, WebhookTarget[]>();

function normalizeWebhookPath(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return "/";
  const withSlash = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  if (withSlash.length > 1 && withSlash.endsWith("/")) {
    return withSlash.slice(0, -1);
  }
  return withSlash;
}

function resolveWebhookPath(account: ResolvedLarkAccount): string {
  const configured = account.config.webhookPath?.trim();
  if (configured) return normalizeWebhookPath(configured);
  return "/lark-webhook";
}

export function createLarkEventDispatcher(target: {
  account: ResolvedLarkAccount;
  config: ClawdbotConfig;
  env: LarkRuntimeEnv;
  statusSink?: (patch: { lastInboundAt?: number; lastOutboundAt?: number }) => void;
}): lark.EventDispatcher {
  const dispatcher = new lark.EventDispatcher({
    verificationToken: target.account.verificationToken,
    encryptKey: target.account.encryptKey,
    loggerLevel: lark.LoggerLevel.info,
  });

  dispatcher.register({
    "im.message.receive_v1": async (data) => {
      void handleLarkMessageEvent({
        data,
        account: target.account,
        config: target.config,
        runtime: getLarkRuntime(),
        env: target.env,
        statusSink: target.statusSink,
      }).catch((err) => {
        target.env.error?.(`[lark] inbound handler failed: ${String(err)}`);
      });
      return {};
    },
    "im.chat.updated_v1": async (data) => {
      void handleLarkChatUpdatedEvent({
        account: target.account,
        event: data as Record<string, unknown>,
        env: target.env,
      }).catch((err) => {
        target.env.error?.(`[lark] chat update handler failed: ${String(err)}`);
      });
      return {};
    },
    "im.chat.member.user.added_v1": async (data) => {
      void handleLarkChatMemberEvent({
        account: target.account,
        event: data as Record<string, unknown>,
        env: target.env,
      }).catch((err) => {
        target.env.error?.(`[lark] chat member add handler failed: ${String(err)}`);
      });
      void maybeBootstrapFromMemberEvent({
        account: target.account,
        event: data as Record<string, unknown>,
        env: target.env,
      }).catch((err) => {
        target.env.error?.(`[lark] bootstrap check failed: ${String(err)}`);
      });
      return {};
    },
    "im.chat.member.user.deleted_v1": async (data) => {
      void handleLarkChatMemberEvent({
        account: target.account,
        event: data as Record<string, unknown>,
        env: target.env,
      }).catch((err) => {
        target.env.error?.(`[lark] chat member remove handler failed: ${String(err)}`);
      });
      return {};
    },
    "im.chat.member.bot.added_v1": async (data) => {
      void handleLarkChatMemberEvent({
        account: target.account,
        event: data as Record<string, unknown>,
        env: target.env,
      }).catch((err) => {
        target.env.error?.(`[lark] chat bot add handler failed: ${String(err)}`);
      });
      const chatId = typeof (data as Record<string, unknown>).chat_id === "string"
        ? (data as Record<string, unknown>).chat_id
        : "";
      if (chatId) {
        void bootstrapLarkChatHistory({
          account: target.account,
          chatId,
          tenantKey: typeof (data as Record<string, unknown>).tenant_key === "string"
            ? (data as Record<string, unknown>).tenant_key
            : undefined,
          env: target.env,
        }).catch((err) => {
          target.env.error?.(`[lark] bootstrap failed: ${String(err)}`);
        });
      }
      return {};
    },
  });

  return dispatcher;
}

export function registerLarkWebhookTarget(target: {
  account: ResolvedLarkAccount;
  config: ClawdbotConfig;
  env: LarkRuntimeEnv;
  statusSink?: (patch: { lastInboundAt?: number; lastOutboundAt?: number }) => void;
}): () => void {
  const path = resolveWebhookPath(target.account);
  const dispatcher = createLarkEventDispatcher(target);
  const normalized = normalizeWebhookPath(path);
  const entry: WebhookTarget = {
    account: target.account,
    config: target.config,
    env: target.env,
    dispatcher,
    path: normalized,
    statusSink: target.statusSink,
  };

  const existing = webhookTargets.get(normalized) ?? [];
  webhookTargets.set(normalized, [...existing, entry]);

  return () => {
    const updated = (webhookTargets.get(normalized) ?? []).filter((item) => item !== entry);
    if (updated.length > 0) {
      webhookTargets.set(normalized, updated);
    } else {
      webhookTargets.delete(normalized);
    }
  };
}

async function readJsonBody(req: IncomingMessage, maxBytes: number) {
  const chunks: Buffer[] = [];
  let total = 0;
  return await new Promise<{ ok: boolean; value?: unknown; error?: string }>((resolve) => {
    let resolved = false;
    const finish = (value: { ok: boolean; value?: unknown; error?: string }) => {
      if (resolved) return;
      resolved = true;
      req.removeAllListeners();
      resolve(value);
    };
    req.on("data", (chunk: Buffer) => {
      total += chunk.length;
      if (total > maxBytes) {
        finish({ ok: false, error: "payload too large" });
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        if (!raw.trim()) {
          finish({ ok: false, error: "empty payload" });
          return;
        }
        finish({ ok: true, value: JSON.parse(raw) as unknown });
      } catch (err) {
        finish({ ok: false, error: err instanceof Error ? err.message : String(err) });
      }
    });
    req.on("error", (err) => {
      finish({ ok: false, error: err instanceof Error ? err.message : String(err) });
    });
  });
}

function selectTarget(targets: WebhookTarget[], body: Record<string, unknown>): WebhookTarget {
  const token = typeof body.token === "string" ? body.token : undefined;
  if (token) {
    const matched = targets.find((entry) => entry.account.verificationToken === token);
    if (matched) return matched;
  }
  return targets[0];
}

export async function handleLarkWebhookRequest(
  req: IncomingMessage,
  res: ServerResponse,
): Promise<boolean> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const path = normalizeWebhookPath(url.pathname);
  const targets = webhookTargets.get(path);
  if (!targets || targets.length === 0) return false;

  if (req.method !== "POST") {
    res.statusCode = 405;
    res.setHeader("Allow", "POST");
    res.end("Method Not Allowed");
    return true;
  }

  const body = await readJsonBody(req, 1024 * 1024);
  if (!body.ok) {
    res.statusCode = body.error === "payload too large" ? 413 : 400;
    res.end(body.error ?? "invalid payload");
    return true;
  }

  if (!body.value || typeof body.value !== "object" || Array.isArray(body.value)) {
    res.statusCode = 400;
    res.end("invalid payload");
    return true;
  }

  const payload = body.value as Record<string, unknown>;
  const target = selectTarget(targets, payload);
  const assigned = Object.assign(Object.create({ headers: req.headers }), payload);

  const { isChallenge, challenge } = lark.generateChallenge(assigned, {
    encryptKey: target.dispatcher.encryptKey,
  });
  if (isChallenge) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end(JSON.stringify(challenge));
    return true;
  }

  try {
    const value = await target.dispatcher.invoke(assigned);
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end(JSON.stringify(value ?? {}));
  } catch (err) {
    target.env.error?.(`[lark] webhook invoke failed: ${String(err)}`);
    res.statusCode = 500;
    res.end("Internal Server Error");
  }

  return true;
}
