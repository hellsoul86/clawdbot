import type { ClawdbotConfig, HistoryEntry, PluginRuntime } from "clawdbot/plugin-sdk";
import {
  DEFAULT_GROUP_HISTORY_LIMIT,
  buildPendingHistoryContextFromMap,
  clearHistoryEntriesIfEnabled,
  recordPendingHistoryEntryIfEnabled,
} from "clawdbot/plugin-sdk";

import type { ResolvedLarkAccount } from "./accounts.js";
import { parseTextContent, stripMentionTags } from "./content.js";
import { resolveLarkChatProfile, resolveLarkSenderProfile } from "./directory-store.js";
import { ensureLarkChatMetadata } from "./im-sync.js";
import { buildLarkMemoryContext, recordLarkMemoryFromMessage } from "./memory.js";
import { replyLarkMessage, sendLarkMessage } from "./messages.js";
import { enqueueLarkMessagePersist } from "./persistence.js";
import { decideProactiveReply, recordProactiveSent } from "./proactive.js";
import { enqueueLarkMessageResources } from "./resource.js";
import { handleLarkTaskConfirmation, recordLarkTaskFromMessage } from "./task.js";
import type { LarkMention, LarkMessageEvent, LarkRuntimeEnv } from "./types.js";
import { buildLarkKnowledgeContext } from "./knowledge.js";

const groupHistories = new Map<string, HistoryEntry[]>();

function resolveSenderId(event: LarkMessageEvent): string | undefined {
  const sender = event.sender?.sender_id;
  return sender?.open_id || sender?.user_id || sender?.union_id;
}

function resolveWasMentioned(mentions: LarkMention[] | undefined, botUserId?: string): boolean {
  const target = botUserId?.trim();
  if (!target || !mentions || mentions.length === 0) return false;
  return mentions.some((mention) => {
    const id = mention.id;
    return id?.open_id === target || id?.user_id === target || id?.union_id === target;
  });
}

export async function handleLarkMessageEvent(params: {
  data: LarkMessageEvent;
  account: ResolvedLarkAccount;
  config: ClawdbotConfig;
  runtime: PluginRuntime;
  env?: LarkRuntimeEnv;
  statusSink?: (patch: { lastInboundAt?: number; lastOutboundAt?: number }) => void;
}): Promise<void> {
  const { data, account, config, runtime } = params;
  if (!data?.message || !data.sender) return;

  enqueueLarkMessagePersist({ account, event: data, env: params.env });
  enqueueLarkMessageResources({ account, event: data, env: params.env });

  if (data.sender.sender_type !== "user") return;
  if (data.message.message_type !== "text") return;

  const chatId = data.message.chat_id;
  if (!chatId) return;

  const rawText = parseTextContent(data.message.content);
  if (!rawText) return;

  const text = stripMentionTags(rawText);
  if (!text) return;

  const senderId = resolveSenderId(data) ?? chatId;
  const chatType = data.message.chat_type === "p2p" ? "direct" : "group";
  const isDirect = chatType === "direct";
  const requireMention = !isDirect && (account.config.requireMention ?? true);
  const wasMentioned = resolveWasMentioned(data.message.mentions, account.botUserId);
  const historyLimit = Math.max(
    0,
    account.config.historyLimit ??
      config.messages?.groupChat?.historyLimit ??
      DEFAULT_GROUP_HISTORY_LIMIT,
  );
  const timestampValue = data.message.create_time ? Number(data.message.create_time) : undefined;
  const timestampMs = Number.isFinite(timestampValue) ? timestampValue : undefined;

  const handledTaskConfirm = await handleLarkTaskConfirmation({
    account,
    tenantKey: data.tenant_key,
    senderId,
    senderIds: data.sender.sender_id,
    content: text,
    env: params.env,
  });
  if (handledTaskConfirm) return;

  if (!isDirect) {
    void ensureLarkChatMetadata({
      account,
      chatId,
      tenantKey: data.tenant_key,
      env: params.env,
    });
  }

  void recordLarkMemoryFromMessage({
    account,
    tenantKey: data.tenant_key,
    chatId,
    chatType,
    senderId,
    messageId: data.message.message_id,
    content: text,
    env: params.env,
  });

  void recordLarkTaskFromMessage({
    account,
    tenantKey: data.tenant_key,
    chatId,
    chatType,
    senderId,
    senderIds: data.sender.sender_id,
    messageId: data.message.message_id,
    content: text,
    wasMentioned,
    env: params.env,
  });

  if (requireMention && !wasMentioned) {
    const decision = decideProactiveReply({
      account,
      chatId,
      content: text,
    });
    if (decision.shouldSend) {
      const receiveIdType = data.sender.sender_id?.open_id
        ? "open_id"
        : data.sender.sender_id?.user_id
          ? "user_id"
          : data.sender.sender_id?.union_id
            ? "union_id"
            : "open_id";
      const receiveId =
        data.sender.sender_id?.open_id ??
        data.sender.sender_id?.user_id ??
        data.sender.sender_id?.union_id ??
        senderId;
      const proactiveText =
        decision.mode === "dm"
          ? `I noticed a request in the group. If you want my help, reply here or mention me in the group.`
          : `I noticed a request. If you'd like me to help, mention me.`;
      if (decision.mode === "dm") {
        void sendLarkMessage({
          account,
          receiveIdType,
          receiveId,
          text: proactiveText,
          tenantKey: data.tenant_key,
        }).catch((err) => {
          params.env?.error?.(`[lark] proactive DM failed: ${String(err)}`);
        });
      } else {
        void sendLarkMessage({
          account,
          receiveIdType: "chat_id",
          receiveId: chatId,
          text: proactiveText,
          tenantKey: data.tenant_key,
        }).catch((err) => {
          params.env?.error?.(`[lark] proactive reply failed: ${String(err)}`);
        });
      }
      recordProactiveSent({ accountId: account.accountId, chatId });
    }
    recordPendingHistoryEntryIfEnabled({
      historyMap: groupHistories,
      historyKey: chatId,
      limit: historyLimit,
      entry: {
        sender: senderId,
        body: text,
        timestamp: timestampMs,
        messageId: data.message.message_id,
      },
    });
    return;
  }

  const senderProfile = await resolveLarkSenderProfile({
    account,
    senderIds: data.sender.sender_id,
    env: params.env,
  });
  const chatProfile = isDirect
    ? null
    : await resolveLarkChatProfile({
        account,
        chatId,
        env: params.env,
      });

  const route = runtime.channel.routing.resolveAgentRoute({
    cfg: config,
    channel: "lark",
    accountId: account.accountId,
    peer: {
      kind: isDirect ? "dm" : "group",
      id: isDirect ? senderId : chatId,
    },
  });

  const sessionKey = route.sessionKey;
  const toTarget = `chat:${chatId}`;
  const messageThreadId = data.message.thread_id || data.message.root_id;
  let combinedBody = text;
  if (!isDirect && historyLimit > 0) {
    combinedBody = buildPendingHistoryContextFromMap({
      historyMap: groupHistories,
      historyKey: chatId,
      limit: historyLimit,
      currentMessage: combinedBody,
      formatEntry: (entry) =>
        `${entry.sender}: ${entry.body}${entry.messageId ? ` [id:${entry.messageId}]` : ""}`,
    });
  }
  const memoryContext = await buildLarkMemoryContext({
    account,
    tenantKey: data.tenant_key,
    chatId,
    chatType,
    senderId,
    query: text,
    env: params.env,
  });
  const knowledgeContext = await buildLarkKnowledgeContext({
    account,
    tenantKey: data.tenant_key,
    chatType,
    chatId,
    senderId,
    query: text,
    env: params.env,
  });
  const contextPrefix = [memoryContext, knowledgeContext].filter(Boolean).join("\n\n");
  const bodyForAgent = contextPrefix ? `${contextPrefix}\n\n${combinedBody}` : combinedBody;

  const ctxPayload = runtime.channel.reply.finalizeInboundContext({
    Body: combinedBody,
    BodyForAgent: bodyForAgent,
    RawBody: text,
    CommandBody: text,
    From: isDirect ? `lark:${senderId}` : `lark:chat:${chatId}`,
    To: toTarget,
    SessionKey: sessionKey,
    AccountId: route.accountId,
    ChatType: chatType,
    GroupSubject: chatProfile?.name,
    GroupMembers: chatProfile?.memberCount ? `${chatProfile.memberCount}` : undefined,
    SenderId: senderId,
    SenderName: senderProfile?.name,
    SenderTag: senderProfile?.departmentChain,
    Provider: "lark",
    Surface: "lark",
    MessageSid: data.message.message_id,
    ReplyToId: data.message.message_id,
    MessageThreadId: messageThreadId ?? undefined,
    Timestamp: timestampMs,
    WasMentioned: isDirect ? undefined : wasMentioned,
    GroupSystemPrompt: account.config.systemPrompt?.trim() || undefined,
    CommandAuthorized: true,
    OriginatingChannel: "lark",
    OriginatingTo: toTarget,
  });

  params.statusSink?.({ lastInboundAt: Date.now() });

  const storePath = runtime.channel.session.resolveStorePath(config.session?.store, {
    agentId: route.agentId,
  });

  await runtime.channel.session.recordInboundSession({
    storePath,
    sessionKey,
    ctx: ctxPayload,
    updateLastRoute: isDirect
      ? {
          sessionKey: route.mainSessionKey,
          channel: "lark",
          to: toTarget,
          accountId: route.accountId,
        }
      : undefined,
    onRecordError: (err) => {
      params.env?.error?.(`[lark] failed updating session meta: ${String(err)}`);
    },
  });

  const responsePrefix = runtime.channel.reply.resolveEffectiveMessagesConfig(config, route.agentId)
    .responsePrefix;
  const humanDelay = runtime.channel.reply.resolveHumanDelayConfig(config, route.agentId);

  const defaultReplyId = account.replyMode === "reply" ? data.message.message_id : undefined;
  const tenantKey = data.tenant_key;

  await runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
    ctx: ctxPayload,
    cfg: config,
    dispatcherOptions: {
      responsePrefix,
      humanDelay,
      deliver: async (payload) => {
        const replyToId =
          payload.replyToId ?? (payload.replyToCurrent ? data.message.message_id : defaultReplyId);
        const content = payload.text?.trim();
        if (!content) return;
        if (replyToId) {
          await replyLarkMessage({
            account,
            messageId: replyToId,
            text: content,
            tenantKey,
          });
        } else {
          await sendLarkMessage({
            account,
            receiveIdType: "chat_id",
            receiveId: chatId,
            text: content,
            tenantKey,
          });
        }
        params.statusSink?.({ lastOutboundAt: Date.now() });
      },
      onError: (err) => {
        params.env?.error?.(`[lark] reply failed: ${String(err)}`);
      },
    },
  });

  if (!isDirect) {
    clearHistoryEntriesIfEnabled({
      historyMap: groupHistories,
      historyKey: chatId,
      limit: historyLimit,
    });
  }
}
