import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { fmt } from "../src/lib/format";
import { tokenizeQuery } from "../src/lib/highlight";
import { formatHotkey, parseHotkey } from "../src/lib/hotkey";
import {
    SearchStorage,
    savePendingHighlight,
    takePendingHighlight,
} from "../src/lib/storage";

describe("fmt", () => {
    it("uses spaces as thousands separators", () => {
        expect(fmt(2006)).toBe("2 006");
        expect(fmt(1234567)).toBe("1 234 567");
        expect(fmt(42)).toBe("42");
    });
});

describe("parseHotkey / formatHotkey", () => {
    it("understands mod/cmd/ctrl prefixes and bare keys", () => {
        expect(parseHotkey("mod+k")).toEqual({ mod: true, key: "k" });
        expect(parseHotkey("Ctrl+P")).toEqual({ mod: true, key: "p" });
        expect(parseHotkey("/")).toEqual({ mod: false, key: "/" });
    });

    it("formats per platform and returns empty for disabled hotkeys", () => {
        expect(formatHotkey(false)).toBe("");
        const platform = Object.getOwnPropertyDescriptor(Navigator.prototype, "platform");
        Object.defineProperty(window.navigator, "platform", {
            value: "MacIntel",
            configurable: true,
        });
        expect(formatHotkey("mod+k")).toBe("⌘K");
        Object.defineProperty(window.navigator, "platform", {
            value: "Win32",
            configurable: true,
        });
        expect(formatHotkey("mod+k")).toBe("Ctrl+K");
        expect(formatHotkey("k")).toBe("K");
        if (platform) Object.defineProperty(Navigator.prototype, "platform", platform);
    });
});

describe("tokenizeQuery", () => {
    it("lowercases, splits on non-word chars and dedupes", () => {
        expect(tokenizeQuery("Vacuum  REFRESH, vacuum!")).toEqual(["vacuum", "refresh"]);
    });

    it("keeps underscores (code identifiers) together", () => {
        expect(tokenizeQuery("ts_starts_with(prefix)")).toEqual(["ts_starts_with", "prefix"]);
    });

    it("drops single characters and caps at 8 terms", () => {
        expect(tokenizeQuery("a b c ok")).toEqual(["ok"]);
        expect(tokenizeQuery("t1 t2 t3 t4 t5 t6 t7 t8 t9 t10")).toHaveLength(8);
    });
});

describe("SearchStorage", () => {
    beforeEach(() => localStorage.clear());
    afterEach(() => localStorage.clear());

    it("namespaces keys so two widgets don't collide", () => {
        const a = new SearchStorage("ns-a");
        const b = new SearchStorage("ns-b");
        a.pushRecent("vacuum");
        expect(a.getRecent()).toEqual(["vacuum"]);
        expect(b.getRecent()).toEqual([]);
    });

    it("moves repeated queries to the front and keeps at most 5", () => {
        const s = new SearchStorage("ns");
        for (const q of ["one", "two", "three", "four", "five", "six"]) s.pushRecent(q);
        expect(s.getRecent()).toEqual(["six", "five", "four", "three", "two"]);
        s.pushRecent("three");
        expect(s.getRecent()).toEqual(["three", "six", "five", "four", "two"]);
    });

    it("ignores blank queries and removes the key when the list empties", () => {
        const s = new SearchStorage("ns");
        s.pushRecent("   ");
        expect(s.getRecent()).toEqual([]);
        s.pushRecent("solo");
        s.removeRecent("solo");
        expect(localStorage.getItem("ns:recent")).toBeNull();
    });

    it("round-trips the saved connection and wizard draft", () => {
        const s = new SearchStorage("ns");
        s.saveConnection({ backendUrl: "http://api:7700", token: "sk-t" });
        expect(s.getConnection()).toEqual({ backendUrl: "http://api:7700", token: "sk-t" });
        s.clearConnection();
        expect(s.getConnection()).toBeNull();
    });

    it("survives corrupted json", () => {
        localStorage.setItem("ns:recent", "{nope");
        localStorage.setItem("ns:connection", "[broken");
        const s = new SearchStorage("ns");
        expect(s.getRecent()).toEqual([]);
        expect(s.getConnection()).toBeNull();
    });
});

describe("pending highlight hand-off", () => {
    it("is taken exactly once (MPA navigation)", () => {
        savePendingHighlight({ url: "/docs/x", anchor: "usage", terms: ["vacuum"] });
        expect(takePendingHighlight()).toEqual({
            url: "/docs/x",
            anchor: "usage",
            terms: ["vacuum"],
        });
        expect(takePendingHighlight()).toBeNull();
    });
});
