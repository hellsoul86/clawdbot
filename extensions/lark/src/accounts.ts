import type { ClawdbotConfig } from "clawdbot/plugin-sdk";
import { DEFAULT_ACCOUNT_ID, normalizeAccountId } from "clawdbot/plugin-sdk";

import type { LarkAccountConfig, LarkConfig } from "./config-schema.js";

export type ResolvedLarkAccount = {
  accountId: string;
  name?: string;
  enabled: boolean;
  config: LarkAccountConfig;
  appId?: string;
  appSecret?: string;
  verificationToken?: string;
  encryptKey?: string;
  botUserId?: string;
  region: "lark" | "feishu";
  mode: "webhook" | "ws";
  replyMode: "reply" | "send";
};

const ENV_APP_ID = "LARK_APP_ID";
const ENV_APP_SECRET = "LARK_APP_SECRET";
const ENV_VERIFICATION_TOKEN = "LARK_VERIFICATION_TOKEN";
const ENV_ENCRYPT_KEY = "LARK_ENCRYPT_KEY";
const ENV_BOT_USER_ID = "LARK_BOT_USER_ID";

function listConfiguredAccountIds(cfg: ClawdbotConfig): string[] {
  const accounts = (cfg.channels?.lark as LarkConfig | undefined)?.accounts;
  if (!accounts || typeof accounts !== "object") return [];
  return Object.keys(accounts).filter(Boolean);
}

export function listLarkAccountIds(cfg: ClawdbotConfig): string[] {
  const ids = listConfiguredAccountIds(cfg);
  if (ids.length === 0) return [DEFAULT_ACCOUNT_ID];
  return ids.sort((a, b) => a.localeCompare(b));
}

export function resolveDefaultLarkAccountId(cfg: ClawdbotConfig): string {
  const channel = cfg.channels?.lark as LarkConfig | undefined;
  if (channel?.defaultAccount?.trim()) return channel.defaultAccount.trim();
  const ids = listLarkAccountIds(cfg);
  if (ids.includes(DEFAULT_ACCOUNT_ID)) return DEFAULT_ACCOUNT_ID;
  return ids[0] ?? DEFAULT_ACCOUNT_ID;
}

function resolveAccountConfig(cfg: ClawdbotConfig, accountId: string): LarkAccountConfig | undefined {
  const accounts = (cfg.channels?.lark as LarkConfig | undefined)?.accounts;
  if (!accounts || typeof accounts !== "object") return undefined;
  return accounts[accountId] as LarkAccountConfig | undefined;
}

function mergeLarkAccountConfig(cfg: ClawdbotConfig, accountId: string): LarkAccountConfig {
  const raw = (cfg.channels?.lark ?? {}) as LarkConfig;
  const { accounts: _ignored, defaultAccount: _ignored2, ...base } = raw;
  const account = resolveAccountConfig(cfg, accountId) ?? {};
  return { ...base, ...account } as LarkAccountConfig;
}

function resolveEnvFallback(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function applyEnvOverrides(params: {
  accountId: string;
  account: LarkAccountConfig;
}): LarkAccountConfig {
  if (params.accountId !== DEFAULT_ACCOUNT_ID) return params.account;
  return {
    ...params.account,
    appId: params.account.appId ?? resolveEnvFallback(process.env[ENV_APP_ID]),
    appSecret: params.account.appSecret ?? resolveEnvFallback(process.env[ENV_APP_SECRET]),
    verificationToken:
      params.account.verificationToken ??
      resolveEnvFallback(process.env[ENV_VERIFICATION_TOKEN]),
    encryptKey: params.account.encryptKey ?? resolveEnvFallback(process.env[ENV_ENCRYPT_KEY]),
    botUserId: params.account.botUserId ?? resolveEnvFallback(process.env[ENV_BOT_USER_ID]),
  };
}

export function resolveLarkAccount(params: {
  cfg: ClawdbotConfig;
  accountId?: string | null;
}): ResolvedLarkAccount {
  const accountId = normalizeAccountId(params.accountId);
  const baseEnabled = (params.cfg.channels?.lark as LarkConfig | undefined)?.enabled !== false;
  const merged = applyEnvOverrides({
    accountId,
    account: mergeLarkAccountConfig(params.cfg, accountId),
  });
  const accountEnabled = merged.enabled !== false;
  const enabled = baseEnabled && accountEnabled;
  const region = merged.region === "feishu" ? "feishu" : "lark";
  const mode = merged.mode === "ws" ? "ws" : "webhook";
  const replyMode = merged.replyMode === "send" ? "send" : "reply";

  return {
    accountId,
    name: merged.name?.trim() || undefined,
    enabled,
    config: merged,
    appId: merged.appId?.trim() || undefined,
    appSecret: merged.appSecret?.trim() || undefined,
    verificationToken: merged.verificationToken?.trim() || undefined,
    encryptKey: merged.encryptKey?.trim() || undefined,
    botUserId: merged.botUserId?.trim() || undefined,
    region,
    mode,
    replyMode,
  };
}
