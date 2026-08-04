import {
    cleanup,
    fireEvent,
    render,
    screen,
    waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SereneDocsSearch, createMcpSetupInstructions } from "../src";

const health = new Response(
    JSON.stringify({
        ok: true,
        version: "0.9.1",
        serenedb: { connected: true },
        index: { ready: true, building: false, sections: 12, documents: 4 },
        features: { ai: true, hybrid: false },
        searchType: "fulltext",
    }),
    { status: 200, headers: { "Content-Type": "application/json" } },
);

describe("MCP setup tab", () => {
    const writeText = vi.fn(async () => {});

    beforeEach(() => {
        localStorage.clear();
        vi.stubGlobal(
            "fetch",
            vi.fn(async () => health.clone()),
        );
        Object.defineProperty(navigator, "clipboard", {
            configurable: true,
            value: { writeText },
        });
        writeText.mockClear();
    });

    afterEach(() => {
        cleanup();
        vi.unstubAllGlobals();
    });

    it("shows one selected client command at a time and copies it", async () => {
        render(
            <SereneDocsSearch
                backendUrl="https://search.example.com"
                mcp={{
                    endpoint: "https://mcp.example.com/mcp",
                    serverName: "product-docs",
                }}
                open
                onOpenChange={() => {}}
                trigger={false}
            />,
        );

        await waitFor(() =>
            expect(screen.getByRole("tab", { name: "ASK AI" })).toBeTruthy(),
        );
        expect(screen.getByRole("tab", { name: "SEARCH" })).toBeTruthy();
        fireEvent.click(screen.getByRole("tab", { name: "MCP" }));

        const modeTabs = screen.getByRole("tablist", { name: "Search modes" });
        const clientTabs = screen.getByRole("group", { name: "MCP client" });
        expect(clientTabs.parentElement).toBe(modeTabs.parentElement);
        expect(
            clientTabs.parentElement?.classList.contains("sds-tabs-row"),
        ).toBe(true);
        expect(
            screen
                .getByRole("button", { name: /^Codex$/i })
                .classList.contains("sds-tab"),
        ).toBe(true);
        expect(screen.queryByText(/MODEL CONTEXT PROTOCOL/)).toBeNull();
        expect(
            screen.getByRole("heading", {
                name: "Use this documentation in your coding agent.",
            }),
        ).toBeTruthy();

        const codexCommand =
            "codex mcp add product-docs --url https://mcp.example.com/mcp";
        const claudeCommand =
            "claude mcp add --transport http product-docs https://mcp.example.com/mcp";

        const prompt = screen.getByText(
            "Choose your client, copy the command.",
        );
        const codexRegion = screen.getByRole("region", {
            name: "Codex command",
        });
        expect(codexRegion.previousElementSibling).toBe(prompt);
        expect(codexRegion.textContent).toContain(codexCommand);
        expect(
            screen.queryByRole("region", { name: "Claude command" }),
        ).toBeNull();
        expect(screen.queryByText("~/.codex/config.toml")).toBeNull();
        expect(screen.queryByText(".mcp.json")).toBeNull();
        expect(screen.queryByText("STDIO WRAPPER")).toBeNull();
        expect(screen.queryByText(/command or config/i)).toBeNull();
        expect(
            screen.queryByText(/select a client, then copy its command/i),
        ).toBeNull();

        fireEvent.click(
            screen.getByRole("button", { name: "Copy Codex command" }),
        );

        await waitFor(() => {
            expect(writeText).toHaveBeenCalledWith(codexCommand);
        });

        writeText.mockClear();
        fireEvent.click(screen.getByRole("button", { name: /^Claude$/i }));
        expect(
            screen.getByRole("region", { name: "Claude command" }).textContent,
        ).toContain(claudeCommand);
        expect(
            screen.queryByRole("region", { name: "Codex command" }),
        ).toBeNull();
        fireEvent.click(
            screen.getByRole("button", { name: "Copy Claude command" }),
        );
        await waitFor(() =>
            expect(writeText).toHaveBeenCalledWith(claudeCommand),
        );
    });

    it("keeps the explicit endpoint command patterns", () => {
        const setup = createMcpSetupInstructions({
            endpoint: "https://mcp.example.com/mcp",
            serverName: "docs",
        });

        expect(setup?.connection).toBe("endpoint");
        expect(setup?.codex.command).toBe(
            "codex mcp add docs --url https://mcp.example.com/mcp",
        );
        expect(setup?.claude.command).toBe(
            "claude mcp add --transport http docs https://mcp.example.com/mcp",
        );

        const sereneDbSetup = createMcpSetupInstructions({
            endpoint: "https://api.serenedb.com/mcp",
            serverName: "serenedb-docs",
        });
        expect(sereneDbSetup?.codex.command).toBe(
            "codex mcp add serenedb-docs --url https://api.serenedb.com/mcp",
        );
    });

    it("requires an explicit remote endpoint and never creates a package command", async () => {
        expect(createMcpSetupInstructions()).toBeNull();

        render(
            <SereneDocsSearch
                backendUrl="https://search.example.com"
                open
                onOpenChange={() => {}}
                trigger={false}
            />,
        );

        await waitFor(() =>
            expect(screen.getByRole("tab", { name: "MCP" })).toBeTruthy(),
        );
        fireEvent.click(screen.getByRole("tab", { name: "MCP" }));
        expect(
            screen.getByRole("heading", { name: "Configure an MCP endpoint" }),
        ).toBeTruthy();
        expect(screen.getByText(/mcp\.endpoint/)).toBeTruthy();
        expect(screen.queryByText(/npx/i)).toBeNull();
    });

    it("allows hosts to hide the MCP tab", async () => {
        render(
            <SereneDocsSearch
                backendUrl="https://search.example.com"
                mcp={false}
                open
                onOpenChange={() => {}}
                trigger={false}
            />,
        );
        await waitFor(() =>
            expect(screen.getByRole("tab", { name: "ASK AI" })).toBeTruthy(),
        );
        expect(screen.queryByRole("tab", { name: "MCP" })).toBeNull();
    });
});
