import * as lark from "@larksuiteoapi/node-sdk";

import type {
  ChannelDock,
  ChannelPlugin,
  ClawdbotConfig,
} from "clawdbot/plugin-sdk";
import {
  buildChannelConfigSchema,
  DEFAULT_ACCOUNT_ID,
  deleteAccountFromConfigSection,
  formatPairingApproveHint,
  setAccountEnabledInConfigSection,
} from "clawdbot/plugin-sdk";

import {
  listLarkAccountIds,
  resolveDefaultLarkAccountId,
  resolveLarkAccount,
  type ResolvedLarkAccount,
} from "./accounts.js";
import { LarkConfigSchema } from "./config-schema.js";
import {
  listLarkDirectoryGroupMembers,
  listLarkDirectoryGroups,
  listLarkDirectoryPeers,
  resolveLarkDirectorySelf,
} from "./directory-store.js";
import { createLarkEventDispatcher, registerLarkWebhookTarget } from "./webhook.js";
import { replyLarkMessage, sendLarkMessage } from "./messages.js";

const meta = {
  id: "lark",
  label: "Lark",
  selectionLabel: "Lark (Enterprise)",
  docsPath: "/channels/lark",
  docsLabel: "lark",
  blurb: "Enterprise Lark/Feishu bot integration.",
  order: 65,
  aliases: ["feishu"],
  quickstartAllowFrom: true,
};

const activeWebhooks = new Map<string, () => void>();
const activeWsClients = new Map<string, lark.WSClient>();

function normalizeLarkTarget(raw: string): string | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  return trimmed.replace(/^(lark|feishu):/i, "");
}

function resolveOutboundTarget(raw: string): {
  receiveIdType: "open_id" | "user_id" | "union_id" | "email" | "chat_id";
  receiveId: string;
} | null {
  const normalized = normalizeLarkTarget(raw);
  if (!normalized) return null;
  const [prefix, rest] = normalized.split(":", 2);
  const value = rest ?? "";
  const tag = rest ? prefix.toLowerCase() : "";

  if (tag === "chat" || tag === "chat_id") {
    return { receiveIdType: "chat_id", receiveId: value };
  }
  if (tag === "user" || tag === "user_id") {
    return { receiveIdType: "user_id", receiveId: value };
  }
  if (tag === "open" || tag === "open_id") {
    return { receiveIdType: "open_id", receiveId: value };
  }
  if (tag === "union" || tag === "union_id") {
    return { receiveIdType: "union_id", receiveId: value };
  }
  if (tag === "email") {
    return { receiveIdType: "email", receiveId: value };
  }

  return { receiveIdType: "chat_id", receiveId: normalized };
}

export const larkDock: ChannelDock = {
  id: "lark",
  capabilities: {
    chatTypes: ["direct", "group"],
    reactions: false,
    threads: false,
    media: true,
    nativeCommands: false,
    blockStreaming: true,
  },
  outbound: { textChunkLimit: 4000 },
  config: {
    resolveAllowFrom: ({ cfg, accountId }) =>
      (resolveLarkAccount({ cfg: cfg as ClawdbotConfig, accountId }).config.allowFrom ?? []).map(
        (entry) => String(entry),
      ),
    formatAllowFrom: ({ allowFrom }) =>
      allowFrom
        .map((entry) => String(entry).trim())
        .filter(Boolean)
        .map((entry) => entry.replace(/^(lark|feishu):/i, ""))
        .map((entry) => entry.toLowerCase()),
  },
  groups: {
    resolveRequireMention: ({ cfg, accountId }) =>
      resolveLarkAccount({ cfg: cfg as ClawdbotConfig, accountId }).config.requireMention ??
      true,
  },
};

export const larkPlugin: ChannelPlugin<ResolvedLarkAccount> = {
  id: "lark",
  meta,
  capabilities: {
    chatTypes: ["direct", "group"],
    reactions: false,
    threads: false,
    media: true,
    nativeCommands: false,
    blockStreaming: true,
  },
  reload: { configPrefixes: ["channels.lark"] },
  configSchema: buildChannelConfigSchema(LarkConfigSchema),
  config: {
    listAccountIds: (cfg) => listLarkAccountIds(cfg as ClawdbotConfig),
    resolveAccount: (cfg, accountId) => resolveLarkAccount({ cfg: cfg as ClawdbotConfig, accountId }),
    defaultAccountId: (cfg) => resolveDefaultLarkAccountId(cfg as ClawdbotConfig),
    setAccountEnabled: ({ cfg, accountId, enabled }) =>
      setAccountEnabledInConfigSection({
        cfg: cfg as ClawdbotConfig,
        sectionKey: "lark",
        accountId,
        enabled,
        allowTopLevel: true,
      }),
    deleteAccount: ({ cfg, accountId }) =>
      deleteAccountFromConfigSection({
        cfg: cfg as ClawdbotConfig,
        sectionKey: "lark",
        accountId,
        clearBaseFields: [
          "appId",
          "appSecret",
          "verificationToken",
          "encryptKey",
          "botUserId",
          "name",
        ],
      }),
    isConfigured: (account) => Boolean(account.appId && account.appSecret),
    describeAccount: (account) => ({
      accountId: account.accountId,
      name: account.name,
      enabled: account.enabled,
      configured: Boolean(account.appId && account.appSecret),
    }),
    resolveAllowFrom: ({ cfg, accountId }) =>
      (resolveLarkAccount({ cfg: cfg as ClawdbotConfig, accountId }).config.allowFrom ?? []).map(
        (entry) => String(entry),
      ),
    formatAllowFrom: ({ allowFrom }) =>
      allowFrom
        .map((entry) => String(entry).trim())
        .filter(Boolean)
        .map((entry) => entry.replace(/^(lark|feishu):/i, ""))
        .map((entry) => entry.toLowerCase()),
  },
  security: {
    resolveDmPolicy: ({ cfg, accountId, account }) => {
      const resolvedAccountId = accountId ?? account.accountId ?? DEFAULT_ACCOUNT_ID;
      const useAccountPath = Boolean((cfg as ClawdbotConfig).channels?.lark?.accounts?.[resolvedAccountId]);
      const basePath = useAccountPath
        ? `channels.lark.accounts.${resolvedAccountId}.`
        : "channels.lark.";
      return {
        policy: account.config.dmPolicy ?? "open",
        allowFrom: account.config.allowFrom ?? [],
        policyPath: `${basePath}dmPolicy`,
        allowFromPath: basePath,
        approveHint: formatPairingApproveHint("lark"),
        normalizeEntry: (raw) => raw.replace(/^(lark|feishu):/i, ""),
      };
    },
  },
  groups: {
    resolveRequireMention: ({ cfg, accountId }) =>
      resolveLarkAccount({ cfg: cfg as ClawdbotConfig, accountId }).config.requireMention ??
      true,
  },
  messaging: {
    normalizeTarget: normalizeLarkTarget,
    targetResolver: {
      looksLikeId: (raw, normalized) => Boolean((normalized ?? raw).trim()),
      hint: "<chat_id|user_id|open_id|chat:ID|user:ID|open:ID>",
    },
  },
  outbound: {
    deliveryMode: "direct",
    textChunkLimit: 4000,
    sendText: async ({ cfg, to, text, replyToId, accountId }) => {
      const account = resolveLarkAccount({ cfg: cfg as ClawdbotConfig, accountId });
      if (!account.appId || !account.appSecret) {
        throw new Error("Lark appId/appSecret not configured");
      }
      const target = resolveOutboundTarget(to);
      if (!target) {
        throw new Error("Missing Lark target");
      }
      if (replyToId && account.replyMode === "reply") {
        const reply = await replyLarkMessage({
          account,
          messageId: replyToId,
          text,
        });
        return { channel: "lark", messageId: reply.messageId, chatId: reply.chatId };
      }
      const send = await sendLarkMessage({
        account,
        receiveIdType: target.receiveIdType,
        receiveId: target.receiveId,
        text,
      });
      return { channel: "lark", messageId: send.messageId, chatId: send.chatId };
    },
    sendMedia: async ({ cfg, to, text, mediaUrl, replyToId, accountId }) => {
      const body = [text?.trim(), mediaUrl?.trim()].filter(Boolean).join("\n");
      if (!body) {
        throw new Error("Lark media send requires text or media url");
      }
      return await larkPlugin.outbound!.sendText!({
        cfg,
        to,
        text: body,
        replyToId,
        accountId,
      });
    },
  },
  directory: {
    self: async ({ cfg, accountId }) =>
      resolveLarkDirectorySelf({ cfg: cfg as ClawdbotConfig, accountId }),
    listPeers: async ({ cfg, accountId, query, limit }) =>
      listLarkDirectoryPeers({
        cfg: cfg as ClawdbotConfig,
        accountId,
        query,
        limit,
      }),
    listPeersLive: async ({ cfg, accountId, query, limit }) =>
      listLarkDirectoryPeers({
        cfg: cfg as ClawdbotConfig,
        accountId,
        query,
        limit,
      }),
    listGroups: async ({ cfg, accountId, query, limit }) =>
      listLarkDirectoryGroups({
        cfg: cfg as ClawdbotConfig,
        accountId,
        query,
        limit,
      }),
    listGroupMembers: async ({ cfg, accountId, groupId, limit }) =>
      listLarkDirectoryGroupMembers({
        cfg: cfg as ClawdbotConfig,
        accountId,
        groupId,
        limit,
      }),
  },
  gateway: {
    startAccount: async (ctx) => {
      const account = resolveLarkAccount({ cfg: ctx.cfg as ClawdbotConfig, accountId: ctx.accountId });
      ctx.setStatus({
        accountId: account.accountId,
        configured: Boolean(account.appId && account.appSecret),
        enabled: account.enabled,
        running: true,
        lastStartAt: Date.now(),
        mode: account.mode,
      });
      if (!account.appId || !account.appSecret) {
        throw new Error("Lark appId/appSecret not configured");
      }

      const statusSink = (patch: { lastInboundAt?: number; lastOutboundAt?: number }) => {
        ctx.setStatus({ ...ctx.getStatus(), ...patch });
      };

      if (account.mode === "ws") {
        const dispatcher = createLarkEventDispatcher({
          account,
          config: ctx.cfg as ClawdbotConfig,
          env: {
            log: (message) => ctx.log?.info(message),
            error: (message) => ctx.log?.error(message),
          },
          statusSink,
        });
        const ws = new lark.WSClient({
          appId: account.appId,
          appSecret: account.appSecret,
          domain: account.region === "feishu" ? lark.Domain.Feishu : lark.Domain.Lark,
          loggerLevel: lark.LoggerLevel.info,
          autoReconnect: true,
        });
        await ws.start({ eventDispatcher: dispatcher });
        activeWsClients.set(account.accountId, ws);
        return;
      }

      const unregister = registerLarkWebhookTarget({
        account,
        config: ctx.cfg as ClawdbotConfig,
        env: {
          log: (message) => ctx.log?.info(message),
          error: (message) => ctx.log?.error(message),
        },
        statusSink,
      });
      activeWebhooks.set(account.accountId, unregister);
    },
    stopAccount: async (ctx) => {
      const unregister = activeWebhooks.get(ctx.accountId);
      if (unregister) {
        unregister();
        activeWebhooks.delete(ctx.accountId);
      }
      if (activeWsClients.has(ctx.accountId)) {
        activeWsClients.delete(ctx.accountId);
      }
      ctx.setStatus({ ...ctx.getStatus(), running: false, lastStopAt: Date.now() });
    },
    loginWithQrStart: async () => {
      return { message: "Lark does not support QR login in this plugin." };
    },
  },
};
