import type { SereneSearchConfig } from "@serenedb/docs-search-core";

export interface SavedConnection {
    backendUrl: string;
    token?: string;
}

export interface WizardDraft {
    config: SereneSearchConfig;
    step: number;
    token: string;
}

const safe = {
    get(key: string): string | null {
        try {
            return window.localStorage.getItem(key);
        } catch {
            return null;
        }
    },
    set(key: string, value: string): void {
        try {
            window.localStorage.setItem(key, value);
        } catch {
            /* private mode */
        }
    },
    remove(key: string): void {
        try {
            window.localStorage.removeItem(key);
        } catch {
            /* private mode */
        }
    },
};

export class SearchStorage {
    constructor(private ns: string) {}

    private key(k: string): string {
        return `${this.ns}:${k}`;
    }

    getConnection(): SavedConnection | null {
        return this.getJson<SavedConnection>("connection");
    }
    saveConnection(conn: SavedConnection): void {
        safe.set(this.key("connection"), JSON.stringify(conn));
    }
    clearConnection(): void {
        safe.remove(this.key("connection"));
    }

    getDraft(): WizardDraft | null {
        return this.getJson<WizardDraft>("wizard-draft");
    }
    saveDraft(draft: WizardDraft): void {
        safe.set(this.key("wizard-draft"), JSON.stringify(draft));
    }
    clearDraft(): void {
        safe.remove(this.key("wizard-draft"));
    }

    getRecent(): string[] {
        return this.getJson<string[]>("recent") ?? [];
    }
    pushRecent(q: string): void {
        const cleaned = q.trim();
        if (!cleaned) return;
        const list = [cleaned, ...this.getRecent().filter((r) => r !== cleaned)].slice(0, 5);
        safe.set(this.key("recent"), JSON.stringify(list));
    }
    removeRecent(q: string): void {
        const list = this.getRecent().filter((r) => r !== q);
        if (list.length === 0) safe.remove(this.key("recent"));
        else safe.set(this.key("recent"), JSON.stringify(list));
    }
    clearRecent(): void {
        safe.remove(this.key("recent"));
    }

    private getJson<T>(k: string): T | null {
        const raw = safe.get(this.key(k));
        if (!raw) return null;
        try {
            return JSON.parse(raw) as T;
        } catch {
            return null;
        }
    }
}

/** sessionStorage hand-off for post-navigation highlighting (MPA case). */
export const PENDING_HIGHLIGHT_KEY = "sds:pending-highlight";

export interface PendingHighlight {
    url: string;
    anchor?: string;
    terms: string[];
}

export function savePendingHighlight(p: PendingHighlight): void {
    try {
        window.sessionStorage.setItem(PENDING_HIGHLIGHT_KEY, JSON.stringify(p));
    } catch {
        /* private mode */
    }
}

export function takePendingHighlight(): PendingHighlight | null {
    try {
        const raw = window.sessionStorage.getItem(PENDING_HIGHLIGHT_KEY);
        if (!raw) return null;
        window.sessionStorage.removeItem(PENDING_HIGHLIGHT_KEY);
        return JSON.parse(raw) as PendingHighlight;
    } catch {
        return null;
    }
}
