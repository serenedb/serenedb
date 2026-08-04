export function parseHotkey(hotkey: string): { mod: boolean; key: string } {
    const parts = hotkey.toLowerCase().split("+");
    const key = parts[parts.length - 1];
    return { mod: parts.includes("mod") || parts.includes("cmd") || parts.includes("ctrl"), key };
}

export function formatHotkey(hotkey: string | false): string {
    if (hotkey === false) return "";
    const isMac =
        typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform ?? "");
    const spec = parseHotkey(hotkey);
    const key = spec.key.toUpperCase();
    return spec.mod ? (isMac ? `⌘${key}` : `Ctrl+${key}`) : key;
}
