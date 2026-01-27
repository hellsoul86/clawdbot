import type { ResolvedLarkAccount } from "./accounts.js";

type ProactiveDecision = {
  shouldSend: boolean;
  mode: "public" | "dm";
};

type ProactiveState = {
  dateKey: string;
  count: number;
  lastAt: number;
};

const DEFAULT_MAX_PER_DAY = 2;
const DEFAULT_COOLDOWN_MINUTES = 30;

const proactiveState = new Map<string, ProactiveState>();

function buildDateKey(now: Date): string {
  return `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`;
}

function isCandidateMessage(content: string): boolean {
  const trimmed = content.trim();
  if (!trimmed) return false;
  if (/\?$/.test(trimmed)) return true;
  if (/[？]$/.test(trimmed)) return true;
  if (/(please|help|assist|anyone)\b/i.test(trimmed)) return true;
  if (/(帮忙|帮我|需要|能不能|可以吗|麻烦)/.test(trimmed)) return true;
  return false;
}

export function decideProactiveReply(params: {
  account: ResolvedLarkAccount;
  chatId: string;
  content: string;
}): ProactiveDecision {
  const config = params.account.config.proactive;
  if (!config || config.enabled !== true) {
    return { shouldSend: false, mode: "public" };
  }
  const allowlist = (config.allowlistChatIds ?? []).map((entry) => entry.trim()).filter(Boolean);
  if (allowlist.length > 0 && !allowlist.includes(params.chatId)) {
    return { shouldSend: false, mode: "public" };
  }
  if (!isCandidateMessage(params.content)) {
    return { shouldSend: false, mode: "public" };
  }
  const now = new Date();
  const key = `${params.account.accountId}:${params.chatId}`;
  const state = proactiveState.get(key);
  const dateKey = buildDateKey(now);
  const maxPerDay = config.maxPerDay ?? DEFAULT_MAX_PER_DAY;
  const cooldownMs = (config.cooldownMinutes ?? DEFAULT_COOLDOWN_MINUTES) * 60 * 1000;
  if (state) {
    if (state.dateKey !== dateKey) {
      proactiveState.set(key, { dateKey, count: 0, lastAt: 0 });
    } else {
      if (state.count >= maxPerDay) return { shouldSend: false, mode: "public" };
      if (now.getTime() - state.lastAt < cooldownMs) {
        return { shouldSend: false, mode: "public" };
      }
    }
  }
  const mode = config.mode === "dm" ? "dm" : "public";
  return { shouldSend: true, mode };
}

export function recordProactiveSent(params: {
  accountId: string;
  chatId: string;
}): void {
  const now = new Date();
  const dateKey = buildDateKey(now);
  const key = `${params.accountId}:${params.chatId}`;
  const state = proactiveState.get(key);
  if (!state || state.dateKey !== dateKey) {
    proactiveState.set(key, { dateKey, count: 1, lastAt: now.getTime() });
    return;
  }
  proactiveState.set(key, {
    dateKey,
    count: state.count + 1,
    lastAt: now.getTime(),
  });
}
