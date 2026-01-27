import { getLarkClient, resolveTenantOptions } from "./client.js";
import type { ResolvedLarkAccount } from "./accounts.js";

export type LarkRequestOptions = {
  url: string;
  method: "GET" | "POST";
  params?: Record<string, string | number | boolean | undefined>;
  data?: Record<string, unknown>;
};

export type LarkApiResponse<T> = {
  code?: number;
  msg?: string;
  data?: T;
};

export type LarkRequestClient = {
  request: (
    options: LarkRequestOptions,
    opts?: ReturnType<typeof resolveTenantOptions>,
  ) => Promise<LarkApiResponse<unknown>>;
};

export type LarkPage<T> = {
  items: T[];
  hasMore: boolean;
  nextPageToken?: string;
};

export async function requestLarkApi<T>(
  account: ResolvedLarkAccount,
  options: LarkRequestOptions,
  tenantKey?: string,
): Promise<T> {
  const client = getLarkClient(account) as unknown as LarkRequestClient;
  const response = await client.request(options, resolveTenantOptions(tenantKey));
  if (response?.code && response.code !== 0) {
    const msg = response.msg ? `: ${response.msg}` : "";
    throw new Error(`Lark API error ${response.code}${msg}`);
  }
  return (response?.data ?? {}) as T;
}

export function coerceLarkPage<T>(data: unknown): LarkPage<T> {
  if (!data || typeof data !== "object") {
    return { items: [], hasMore: false };
  }
  const obj = data as Record<string, unknown>;
  const itemsRaw =
    obj.items ?? obj.user_list ?? obj.department_list ?? obj.users ?? obj.departments ?? obj.members;
  const items = Array.isArray(itemsRaw) ? (itemsRaw as T[]) : [];
  const hasMore = Boolean(obj.has_more ?? obj.hasMore);
  const nextPageToken =
    typeof obj.page_token === "string"
      ? obj.page_token
      : typeof obj.next_page_token === "string"
        ? obj.next_page_token
        : typeof obj.pageToken === "string"
          ? obj.pageToken
          : undefined;
  return { items, hasMore, nextPageToken };
}

export async function collectAllPages<T>(
  fetchPage: (pageToken?: string) => Promise<LarkPage<T>>,
): Promise<T[]> {
  const items: T[] = [];
  let pageToken: string | undefined;
  let hasMore = true;
  while (hasMore) {
    const page = await fetchPage(pageToken);
    items.push(...page.items);
    pageToken = page.nextPageToken;
    hasMore = page.hasMore && Boolean(pageToken);
  }
  return items;
}
