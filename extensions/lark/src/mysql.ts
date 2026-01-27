import mysql, { type Pool } from "mysql2/promise";

import type { ResolvedLarkAccount } from "./accounts.js";
import type { LarkRuntimeEnv } from "./types.js";

export type ResolvedLarkMySqlConfig = {
  host: string;
  port: number;
  user: string;
  password?: string;
  database: string;
  tablePrefix: string;
  connectionLimit: number;
  queueLimit: number;
  waitForConnections: boolean;
};

export type TableNames = {
  message: string;
  chat: string;
  user: string;
  orgDepartment: string;
  orgUser: string;
  orgUserDepartmentRel: string;
  imChat: string;
  imChatMember: string;
  messageResource: string;
  contentExtraction: string;
  memoryItem: string;
  memoryEmbedding: string;
  task: string;
  kbDoc: string;
};

export type LarkMySqlContext = {
  key: string;
  config: ResolvedLarkMySqlConfig;
  pool: Pool;
  tables: TableNames;
};

const pools = new Map<string, Pool>();
const schemaReady = new Map<string, Promise<void>>();

function normalizeTablePrefix(raw?: string): string {
  const trimmed = raw?.trim();
  if (!trimmed) return "";
  if (!/^[a-zA-Z0-9_]+$/.test(trimmed)) return "";
  return trimmed;
}

export function resolveLarkMySqlConfig(account: ResolvedLarkAccount): ResolvedLarkMySqlConfig | null {
  const cfg = account.config.mysql;
  if (!cfg || cfg.enabled === false) return null;
  const host = cfg.host?.trim() ?? "";
  const user = cfg.user?.trim() ?? "";
  const database = cfg.database?.trim() ?? "";
  if (!host || !user || !database) return null;

  return {
    host,
    port: cfg.port ?? 3306,
    user,
    password: cfg.password?.trim() || undefined,
    database,
    tablePrefix: normalizeTablePrefix(cfg.tablePrefix),
    connectionLimit: Math.max(1, cfg.connectionLimit ?? 4),
    queueLimit: Math.max(0, cfg.queueLimit ?? 0),
    waitForConnections: cfg.waitForConnections ?? true,
  };
}

function buildPoolKey(config: ResolvedLarkMySqlConfig): string {
  return `${config.host}:${config.port}/${config.database}?user=${config.user}`;
}

function buildTableNames(prefix: string): TableNames {
  const safePrefix = prefix ? `${prefix}` : "";
  return {
    message: `\`${safePrefix}lark_message\``,
    chat: `\`${safePrefix}lark_chat\``,
    user: `\`${safePrefix}lark_user\``,
    orgDepartment: `\`${safePrefix}org_department\``,
    orgUser: `\`${safePrefix}org_user\``,
    orgUserDepartmentRel: `\`${safePrefix}org_user_department_rel\``,
    imChat: `\`${safePrefix}im_chat\``,
    imChatMember: `\`${safePrefix}im_chat_member\``,
    messageResource: `\`${safePrefix}message_resource\``,
    contentExtraction: `\`${safePrefix}content_extraction\``,
    memoryItem: `\`${safePrefix}memory_item\``,
    memoryEmbedding: `\`${safePrefix}memory_embedding\``,
    task: `\`${safePrefix}task\``,
    kbDoc: `\`${safePrefix}kb_doc\``,
  };
}

function getPool(config: ResolvedLarkMySqlConfig): Pool {
  const key = buildPoolKey(config);
  const existing = pools.get(key);
  if (existing) return existing;
  const pool = mysql.createPool({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    waitForConnections: config.waitForConnections,
    connectionLimit: config.connectionLimit,
    queueLimit: config.queueLimit,
  });
  pools.set(key, pool);
  return pool;
}

export function resolveLarkMySqlContext(account: ResolvedLarkAccount): LarkMySqlContext | null {
  const config = resolveLarkMySqlConfig(account);
  if (!config) return null;
  const key = `${buildPoolKey(config)}:${config.tablePrefix}`;
  return {
    key,
    config,
    pool: getPool(config),
    tables: buildTableNames(config.tablePrefix),
  };
}

export async function ensureLarkSchema(
  context: LarkMySqlContext,
  env?: LarkRuntimeEnv,
): Promise<void> {
  const existing = schemaReady.get(context.key);
  if (existing) return await existing;

  const tables = context.tables;
  const task = (async () => {
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.chat} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        chat_id VARCHAR(64) NOT NULL,
        chat_type VARCHAR(32) NOT NULL,
        tenant_key VARCHAR(64) NULL,
        name VARCHAR(255) NULL,
        owner_id VARCHAR(64) NULL,
        owner_id_type VARCHAR(32) NULL,
        member_count INT NULL,
        last_message_at_ms BIGINT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_chat_id (chat_id),
        KEY idx_tenant_key (tenant_key)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.user} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        open_id VARCHAR(64) NULL,
        user_id VARCHAR(64) NULL,
        union_id VARCHAR(64) NULL,
        tenant_key VARCHAR(64) NULL,
        sender_type VARCHAR(32) NULL,
        name VARCHAR(255) NULL,
        email VARCHAR(255) NULL,
        department_id VARCHAR(64) NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_open_id (open_id),
        UNIQUE KEY uniq_user_id (user_id),
        UNIQUE KEY uniq_union_id (union_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.message} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        message_id VARCHAR(64) NOT NULL,
        chat_id VARCHAR(64) NOT NULL,
        chat_type VARCHAR(32) NOT NULL,
        message_type VARCHAR(32) NOT NULL,
        sender_type VARCHAR(32) NULL,
        sender_open_id VARCHAR(64) NULL,
        sender_user_id VARCHAR(64) NULL,
        sender_union_id VARCHAR(64) NULL,
        tenant_key VARCHAR(64) NULL,
        thread_id VARCHAR(64) NULL,
        root_id VARCHAR(64) NULL,
        content TEXT NULL,
        text_content TEXT NULL,
        create_time_ms BIGINT NULL,
        dedupe_hash CHAR(64) NOT NULL,
        raw_event LONGTEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_message_id (message_id),
        KEY idx_chat_id (chat_id),
        KEY idx_sender_user_id (sender_user_id),
        KEY idx_sender_open_id (sender_open_id),
        KEY idx_create_time (create_time_ms)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.orgDepartment} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        department_id VARCHAR(64) NOT NULL,
        name VARCHAR(255) NOT NULL,
        parent_department_id VARCHAR(64) NULL,
        leader_user_id VARCHAR(64) NULL,
        status VARCHAR(32) NULL,
        member_count INT NULL,
        synced_at TIMESTAMP NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_department (tenant_key, department_id),
        KEY idx_parent_department_id (parent_department_id),
        KEY idx_tenant_key (tenant_key)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.orgUser} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        user_key VARCHAR(64) NOT NULL,
        open_id VARCHAR(64) NULL,
        user_id VARCHAR(64) NULL,
        union_id VARCHAR(64) NULL,
        name VARCHAR(255) NULL,
        email VARCHAR(255) NULL,
        mobile VARCHAR(64) NULL,
        status VARCHAR(32) NULL,
        job_title VARCHAR(128) NULL,
        synced_at TIMESTAMP NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_user (tenant_key, user_key),
        KEY idx_open_id (open_id),
        KEY idx_user_id (user_id),
        KEY idx_union_id (union_id),
        KEY idx_tenant_key (tenant_key)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.orgUserDepartmentRel} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        user_key VARCHAR(64) NOT NULL,
        department_id VARCHAR(64) NOT NULL,
        is_primary TINYINT(1) NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_user_department (tenant_key, user_key, department_id),
        KEY idx_department_id (department_id),
        KEY idx_user_key (user_key),
        KEY idx_tenant_key (tenant_key)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.imChat} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        chat_id VARCHAR(64) NOT NULL,
        name VARCHAR(255) NULL,
        description TEXT NULL,
        owner_id VARCHAR(64) NULL,
        owner_id_type VARCHAR(32) NULL,
        member_count INT NULL,
        chat_mode VARCHAR(32) NULL,
        chat_type VARCHAR(32) NULL,
        last_synced_at TIMESTAMP NULL,
        bootstrap_done TINYINT(1) NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_im_chat (tenant_key, chat_id),
        KEY idx_owner_id (owner_id),
        KEY idx_tenant_key (tenant_key)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.imChatMember} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        chat_id VARCHAR(64) NOT NULL,
        user_key VARCHAR(64) NOT NULL,
        open_id VARCHAR(64) NULL,
        user_id VARCHAR(64) NULL,
        union_id VARCHAR(64) NULL,
        name VARCHAR(255) NULL,
        role VARCHAR(32) NULL,
        is_owner TINYINT(1) NOT NULL DEFAULT 0,
        is_admin TINYINT(1) NOT NULL DEFAULT 0,
        joined_at TIMESTAMP NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_im_chat_member (tenant_key, chat_id, user_key),
        KEY idx_chat_id (chat_id),
        KEY idx_user_key (user_key),
        KEY idx_tenant_key (tenant_key)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.messageResource} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        message_id VARCHAR(64) NOT NULL,
        chat_id VARCHAR(64) NOT NULL,
        resource_type VARCHAR(32) NOT NULL,
        file_key VARCHAR(128) NOT NULL,
        file_name VARCHAR(255) NULL,
        mime_type VARCHAR(128) NULL,
        size_bytes BIGINT NULL,
        status VARCHAR(32) NOT NULL,
        storage_path TEXT NULL,
        storage_url TEXT NULL,
        error TEXT NULL,
        attempts INT NOT NULL DEFAULT 0,
        last_attempt_at TIMESTAMP NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_message_resource (tenant_key, message_id, file_key),
        KEY idx_chat_id (chat_id),
        KEY idx_status (status)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.contentExtraction} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        message_id VARCHAR(64) NOT NULL,
        resource_id BIGINT UNSIGNED NULL,
        resource_type VARCHAR(32) NOT NULL,
        language VARCHAR(32) NULL,
        text LONGTEXT NULL,
        summary TEXT NULL,
        model VARCHAR(128) NULL,
        status VARCHAR(32) NOT NULL,
        error TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_content_resource (tenant_key, resource_id),
        KEY idx_message_id (message_id),
        KEY idx_status (status)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.memoryItem} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        scope_type VARCHAR(32) NOT NULL,
        scope_id VARCHAR(128) NOT NULL,
        message_id VARCHAR(64) NULL,
        chat_id VARCHAR(64) NULL,
        user_id VARCHAR(64) NULL,
        content TEXT NOT NULL,
        summary TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_scope (tenant_key, scope_type, scope_id),
        KEY idx_message_id (message_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.memoryEmbedding} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        memory_item_id BIGINT UNSIGNED NOT NULL,
        model VARCHAR(128) NOT NULL,
        vector_json LONGTEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_embedding (tenant_key, memory_item_id, model),
        KEY idx_memory_item (memory_item_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.task} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        scope_type VARCHAR(32) NOT NULL,
        scope_id VARCHAR(128) NOT NULL,
        chat_id VARCHAR(64) NULL,
        user_id VARCHAR(64) NULL,
        message_id VARCHAR(64) NULL,
        title VARCHAR(255) NOT NULL,
        details TEXT NULL,
        classification VARCHAR(32) NOT NULL,
        status VARCHAR(32) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_scope (tenant_key, scope_type, scope_id),
        KEY idx_status (status),
        KEY idx_message_id (message_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
    await context.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${tables.kbDoc} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_key VARCHAR(64) NOT NULL,
        scope_type VARCHAR(32) NOT NULL,
        scope_id VARCHAR(128) NOT NULL,
        title VARCHAR(255) NOT NULL,
        tags VARCHAR(512) NULL,
        source_ids TEXT NULL,
        content LONGTEXT NOT NULL,
        version INT NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_scope (tenant_key, scope_type, scope_id),
        KEY idx_title (title)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    );
  })();

  schemaReady.set(context.key, task);
  try {
    await task;
  } catch (err) {
    schemaReady.delete(context.key);
    env?.error?.(`[lark] mysql schema init failed: ${String(err)}`);
    throw err;
  }
}
