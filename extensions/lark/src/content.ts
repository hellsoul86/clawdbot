export function parseTextContent(raw: string): string | null {
  try {
    const parsed = JSON.parse(raw) as { text?: string };
    return typeof parsed.text === "string" ? parsed.text : null;
  } catch {
    return null;
  }
}

export function stripMentionTags(text: string): string {
  return text.replace(/<at\s+[^>]*>(.*?)<\/at>/gi, "$1").trim();
}
