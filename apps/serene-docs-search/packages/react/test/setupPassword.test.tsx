import { act, cleanup, render, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { defaultConfig } from "@serenedb/docs-search-core";
import type { SereneDocsSearch } from "../src/hooks/useSereneDocsSearch";
import { SearchStorage } from "../src/lib/storage";
import { Deploy } from "../src/setup/Deploy";
import { useSetupFlow } from "../src/setup/useSetupFlow";

const HEX32 = /^[0-9a-f]{32}$/;

const fakeSearch = (storage: SearchStorage) =>
    ({
        open: true,
        status: "unconfigured",
        storage,
        connect: vi.fn(),
        disconnect: vi.fn(),
    }) as unknown as SereneDocsSearch;

const mountFlow = (storage: SearchStorage) =>
    renderHook(() =>
        useSetupFlow({ search: fakeSearch(storage), setup: "auto", pinnedBackend: false, toast: () => {} }),
    );

afterEach(() => {
    cleanup();
    window.localStorage.clear();
});

describe("setup wizard database password", () => {
    it("is generated once and kept in the draft", () => {
        const storage = new SearchStorage("t1");
        const first = mountFlow(storage);
        const password = first.result.current.dbPassword;
        expect(password).toMatch(HEX32);

        act(() => first.result.current.goStep(2));
        expect(storage.getDraft()?.dbPassword).toBe(password);

        act(() =>
            first.result.current.update((c) => {
                c.project = "Acme";
            }),
        );
        expect(storage.getDraft()?.dbPassword).toBe(password);
        first.unmount();

        expect(mountFlow(storage).result.current.dbPassword).toBe(password);
    });

    it("is generated for a draft saved without one", () => {
        const storage = new SearchStorage("t2");
        storage.saveDraft({ config: defaultConfig({ type: "folder", path: "./docs" }), step: 2, token: "sk-local-x" });

        const { result } = mountFlow(storage);
        expect(result.current.token).toBe("sk-local-x");
        expect(result.current.dbPassword).toMatch(HEX32);
    });

    it("is drawn anew when setup starts over", () => {
        const { result } = mountFlow(new SearchStorage("t3"));
        const before = result.current.dbPassword;

        act(() => result.current.resetSetup());
        expect(result.current.dbPassword).toMatch(HEX32);
        expect(result.current.dbPassword).not.toBe(before);
    });
});

describe("deploy screen", () => {
    it("shows a compose that gives both services the wizard's password", () => {
        const { container } = render(
            <Deploy
                config={defaultConfig({ type: "folder", path: "./docs" })}
                token="sk-local-t"
                dbPassword="0123abcd"
                backendUrl="http://localhost:7700"
                setBackendUrl={() => {}}
                tokenInput="sk-local-t"
                setTokenInput={() => {}}
                conn={{ kind: "idle" }}
                setConn={() => {}}
                toast={() => {}}
            />,
        );
        const text = container.textContent ?? "";
        expect(text).toContain('POSTGRES_PASSWORD: "0123abcd"');
        expect(text).toContain('SERENEDB_PASSWORD: "0123abcd"');
    });
});
