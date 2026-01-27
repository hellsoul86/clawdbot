import type { ResolvedLarkAccount } from "./accounts.js";
import { getLarkClient, resolveTenantOptions } from "./client.js";

export type LarkSendResult = {
  messageId: string;
  chatId?: string;
};

function normalizeApiError(result: { code?: number; msg?: string }, fallback: string): string {
  if (typeof result.code === "number" && result.code !== 0) {
    const msg = result.msg ? `: ${result.msg}` : "";
    return `Lark API error ${result.code}${msg}`;
  }
  return fallback;
}

export async function sendLarkMessage(params: {
  account: ResolvedLarkAccount;
  receiveIdType: "open_id" | "user_id" | "union_id" | "email" | "chat_id";
  receiveId: string;
  text: string;
  tenantKey?: string;
  uuid?: string;
}): Promise<LarkSendResult> {
  const client = getLarkClient(params.account);
  const response = await client.im.message.create(
    {
      params: { receive_id_type: params.receiveIdType },
      data: {
        receive_id: params.receiveId,
        msg_type: "text",
        content: JSON.stringify({ text: params.text }),
        uuid: params.uuid,
      },
    },
    resolveTenantOptions(params.tenantKey),
  );
  if (response?.code && response.code !== 0) {
    throw new Error(normalizeApiError(response, "Lark send failed"));
  }
  return {
    messageId: response?.data?.message_id ?? "",
    chatId: response?.data?.chat_id,
  };
}

export async function replyLarkMessage(params: {
  account: ResolvedLarkAccount;
  messageId: string;
  text: string;
  tenantKey?: string;
  replyInThread?: boolean;
  uuid?: string;
}): Promise<LarkSendResult> {
  const client = getLarkClient(params.account);
  const response = await client.im.message.reply(
    {
      path: { message_id: params.messageId },
      data: {
        msg_type: "text",
        content: JSON.stringify({ text: params.text }),
        reply_in_thread: params.replyInThread,
        uuid: params.uuid,
      },
    },
    resolveTenantOptions(params.tenantKey),
  );
  if (response?.code && response.code !== 0) {
    throw new Error(normalizeApiError(response, "Lark reply failed"));
  }
  return {
    messageId: response?.data?.message_id ?? "",
    chatId: response?.data?.chat_id,
  };
}
