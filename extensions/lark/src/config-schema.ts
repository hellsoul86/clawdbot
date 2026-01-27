import { z } from "zod";

const allowFromEntry = z.union([z.string(), z.number()]);

const LarkMySqlSchema = z.object({
  enabled: z.boolean().optional(),
  host: z.string().optional(),
  port: z.number().int().positive().optional(),
  user: z.string().optional(),
  password: z.string().optional(),
  database: z.string().optional(),
  tablePrefix: z.string().optional(),
  connectionLimit: z.number().int().positive().optional(),
  queueLimit: z.number().int().min(0).optional(),
  waitForConnections: z.boolean().optional(),
});

const LarkDirectorySyncSchema = z.object({
  enabled: z.boolean().optional(),
  intervalMinutes: z.number().int().min(10).optional(),
  rootDepartmentId: z.string().optional(),
  userIdType: z.enum(["open_id", "user_id", "union_id"]).optional(),
  departmentIdType: z.enum(["department_id", "open_department_id"]).optional(),
});

const LarkExtractionSchema = z.object({
  enabled: z.boolean().optional(),
  ocr: z
    .object({
      enabled: z.boolean().optional(),
      languages: z.array(z.string()).optional(),
    })
    .optional(),
  asr: z
    .object({
      enabled: z.boolean().optional(),
      provider: z.enum(["openai"]).optional(),
      apiKey: z.string().optional(),
      model: z.string().optional(),
      language: z.string().optional(),
    })
    .optional(),
  docx: z
    .object({
      enabled: z.boolean().optional(),
    })
    .optional(),
});

const LarkMemorySchema = z.object({
  enabled: z.boolean().optional(),
  summaryIntervalMinutes: z.number().int().min(5).optional(),
  summaryMaxItems: z.number().int().min(1).optional(),
  retrievalLimit: z.number().int().min(1).optional(),
});

const LarkProactiveSchema = z.object({
  enabled: z.boolean().optional(),
  allowlistChatIds: z.array(z.string()).optional(),
  maxPerDay: z.number().int().min(1).optional(),
  cooldownMinutes: z.number().int().min(1).optional(),
  mode: z.enum(["public", "dm"]).optional(),
});

const LarkReportsSchema = z.object({
  enabled: z.boolean().optional(),
  dailyChatId: z.string().optional(),
  weeklyChatId: z.string().optional(),
  monthlyChatId: z.string().optional(),
});

export const LarkAccountSchema = z.object({
  name: z.string().optional(),
  enabled: z.boolean().optional(),
  appId: z.string().optional(),
  appSecret: z.string().optional(),
  verificationToken: z.string().optional(),
  encryptKey: z.string().optional(),
  botUserId: z.string().optional(),
  region: z.enum(["lark", "feishu"]).optional(),
  mode: z.enum(["webhook", "ws"]).optional(),
  webhookPath: z.string().optional(),
  replyMode: z.enum(["reply", "send"]).optional(),
  dmPolicy: z.enum(["pairing", "allowlist", "open", "disabled"]).optional(),
  allowFrom: z.array(allowFromEntry).optional(),
  requireMention: z.boolean().optional(),
  historyLimit: z.number().int().min(0).optional(),
  systemPrompt: z.string().optional(),
  mysql: LarkMySqlSchema.optional(),
  directorySync: LarkDirectorySyncSchema.optional(),
  resourceMaxMb: z.number().int().min(1).optional(),
  extraction: LarkExtractionSchema.optional(),
  memory: LarkMemorySchema.optional(),
  proactive: LarkProactiveSchema.optional(),
  reports: LarkReportsSchema.optional(),
});

export const LarkConfigSchema = LarkAccountSchema.extend({
  accounts: z.object({}).catchall(LarkAccountSchema).optional(),
  defaultAccount: z.string().optional(),
});

export type LarkAccountConfig = z.infer<typeof LarkAccountSchema>;
export type LarkConfig = z.infer<typeof LarkConfigSchema>;
