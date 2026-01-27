import path from "node:path";
import os from "node:os";
import { mkdir } from "node:fs/promises";

let larkStateDir: string | null = null;

export function setLarkStateDir(dir: string): void {
  larkStateDir = dir;
}

export function resolveLarkResourceDir(): string {
  const base =
    larkStateDir ?? process.env.CLAWDBOT_STATE_DIR ?? path.join(os.homedir(), ".clawdbot");
  return path.join(base, "lark", "resources");
}

export async function ensureLarkResourceDir(): Promise<string> {
  const dir = resolveLarkResourceDir();
  await mkdir(dir, { recursive: true });
  return dir;
}
