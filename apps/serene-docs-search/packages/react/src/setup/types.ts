import type { SereneSearchConfig } from "@serenedb/docs-search-core";

/** Shared props for the wizard's step editors. */
export interface StepProps {
    config: SereneSearchConfig;
    update: (mutate: (c: SereneSearchConfig) => void) => void;
}

export type ConnState =
    | { kind: "idle" }
    | { kind: "testing" }
    | { kind: "ok"; version?: string; sections: number }
    | { kind: "fail"; message: string };
