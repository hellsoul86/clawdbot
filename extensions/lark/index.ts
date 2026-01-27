import type { ClawdbotPluginApi } from "clawdbot/plugin-sdk";
import { emptyPluginConfigSchema } from "clawdbot/plugin-sdk";

import { larkDock, larkPlugin } from "./src/channel.js";
import { createLarkDirectorySyncService } from "./src/directory-sync.js";
import { createLarkMemoryService } from "./src/memory.js";
import { createLarkReportService } from "./src/reports.js";
import { handleLarkWebhookRequest } from "./src/webhook.js";
import { setLarkRuntime } from "./src/runtime.js";

const plugin = {
  id: "lark",
  name: "Lark",
  description: "Lark/Feishu enterprise channel plugin",
  configSchema: emptyPluginConfigSchema(),
  register(api: ClawdbotPluginApi) {
    setLarkRuntime(api.runtime);
    api.registerChannel({ plugin: larkPlugin, dock: larkDock });
    api.registerHttpHandler(handleLarkWebhookRequest);
    api.registerService(createLarkDirectorySyncService());
    api.registerService(createLarkMemoryService());
    api.registerService(createLarkReportService());
  },
};

export default plugin;
