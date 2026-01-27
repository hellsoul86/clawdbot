import type { ResolvedLarkAccount } from "./accounts.js";

export type LarkUserIdBundle = {
  user_id?: string;
  open_id?: string;
  union_id?: string;
};

export function resolveLarkTenantKey(
  account: ResolvedLarkAccount,
  tenantKey?: string | null,
): string {
  const trimmed = tenantKey?.trim();
  if (trimmed) return trimmed;
  return account.accountId;
}

export function resolveLarkUserKey(ids?: LarkUserIdBundle | null): string | null {
  if (!ids) return null;
  const userId = ids.user_id?.trim();
  if (userId) return userId;
  const openId = ids.open_id?.trim();
  if (openId) return openId;
  const unionId = ids.union_id?.trim();
  if (unionId) return unionId;
  return null;
}
