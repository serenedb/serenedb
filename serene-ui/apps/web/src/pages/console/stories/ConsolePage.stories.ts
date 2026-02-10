import type { Meta, StoryObj } from "@storybook/react-vite";
import { ConsolePage } from "../ui/ConsolePage";
import { userEvent, within } from "storybook/test";
import type { MatchImageSnapshotOptions } from "jest-image-snapshot";
import type { ScreenshotOptions } from "vitest/browser";
import { expect } from "vitest";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const meta = {
    component: ConsolePage,
    parameters: {
        route: "/console",
    },
    tags: ["test"],
} satisfies Meta<typeof ConsolePage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {
    tags: ["test"],
    play: async () => {
        const { page } = await import("vitest/browser");
        await sleep(1000);
        const screenshot = await page.screenshot({
            fullPage: true,
        } as ScreenshotOptions);
        expect(screenshot).toMatchImageSnapshot({
            maxDiffPercentage: 1.0,
        } as MatchImageSnapshotOptions);
    },
};

export const SearchModal: Story = {
    tags: ["test"],
    play: async ({ canvasElement }) => {
        const { page } = await import("vitest/browser");
        const { expect } = await import("vitest");
        const canvas = within(canvasElement);
        const button = await canvas.findByTitle("search");
        await userEvent.click(button);
        await sleep(1500);
        const screenshot = await page.screenshot({
            fullPage: true,
        } as ScreenshotOptions);
        expect(screenshot).toMatchImageSnapshot({
            maxDiffPercentage: 1.0,
        } as MatchImageSnapshotOptions);
    },
};

export const SupportModal: Story = {
    tags: ["test"],
    play: async ({ canvasElement }) => {
        const { page } = await import("vitest/browser");
        const { expect } = await import("vitest");
        const canvas = within(canvasElement);
        const button = await canvas.findByTitle("support");
        await userEvent.click(button);
        await sleep(1500);
        const screenshot = await page.screenshot({
            fullPage: true,
        } as ScreenshotOptions);
        expect(screenshot).toMatchImageSnapshot({
            maxDiffPercentage: 1.0,
        } as MatchImageSnapshotOptions);
    },
};

export const AddConnection: Story = {
    tags: ["test"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(document.body);

        const editConnectionsButton = await canvas.findByRole("button", {
            name: /add server/i,
        });
        await userEvent.click(editConnectionsButton);

        const connectionNameInput =
            await body.findByLabelText(/connection name/i);
        await userEvent.clear(connectionNameInput);
        await userEvent.type(connectionNameInput, "test");

        const connectButton = await body.findByRole("button", {
            name: /connect & add/i,
        });
        await userEvent.click(connectButton);

        await body.findByText(
            "Connection successfully added!",
            {},
            { timeout: 6000 },
        );
        await sleep(5000);
        expect(body.queryByText("Connection successfully added!")).toBeNull();
    },
};

export const TestAddConnectionsErrors: Story = {
    tags: ["test"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(document.body);

        const editConnectionsButton = await canvas.findByRole("button", {
            name: /edit connections/i,
        });
        await userEvent.click(editConnectionsButton);

        const connectionNameInput =
            await body.findByLabelText(/connection name/i);
        await userEvent.clear(connectionNameInput);
        await userEvent.type(connectionNameInput, "error-cases");

        const databaseInput = await body.findByLabelText(/default database/i);
        await userEvent.clear(databaseInput);
        await userEvent.type(databaseInput, "postgres");

        const hostInput = await body.findByLabelText(/host/i);
        const portInput = await body.findByLabelText(/port/i);

        const connectButton = await body.findByRole("button", {
            name: /connect & add/i,
        });

        await userEvent.clear(hostInput);
        await userEvent.type(hostInput, "127.0.0.1");
        await userEvent.clear(portInput);
        await userEvent.type(portInput, "1");

        await userEvent.click(connectButton);

        await body.findByText("Connection test failed", {}, { timeout: 8000 });
        await body.findByText(
            "Connection refused. Check the host, port, and server status. Details: connect ECONNREFUSED 127.0.0.1:1",
        );

        await userEvent.clear(hostInput);
        await userEvent.type(hostInput, "nonexistent.invalid");
        await userEvent.clear(portInput);
        await userEvent.type(portInput, "5432");

        await userEvent.click(connectButton);

        await body.findByText("Connection test failed", {}, { timeout: 8000 });

        body.queryAllByRole("button", { name: "Close" }).forEach((button) => {
            button.click();
        });
    },
};

export const VerifyTree: Story = {
    tags: ["test"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(document.body);

        const explorerHeading = await canvas.findByText("Explorer");
        const explorerWrapper = explorerHeading.parentElement?.parentElement;
        if (!explorerWrapper) {
            throw new Error("Explorer container not found");
        }

        const explorer = within(explorerWrapper);
        const testEntity = await explorer.findByRole("button", {
            name: "test",
        });
        await userEvent.click(testEntity);

        await sleep(1000);

        const postgresEntity = await explorer.findByRole("button", {
            name: "postgres",
        });
        await userEvent.click(postgresEntity);

        await sleep(1000);

        const catalogsEntity = await explorer.findByRole("button", {
            name: "Catalogs",
        });
        await userEvent.click(catalogsEntity);

        await sleep(1000);

        const pgCatalogEntity = await explorer.findByRole("button", {
            name: "pg_catalog",
        });
        await userEvent.click(pgCatalogEntity);

        await sleep(1000);

        const tablesEntity = await explorer.findByRole("button", {
            name: "Tables",
        });
        await userEvent.click(tablesEntity);

        await sleep(1000);

        const pgAuthidEntity = await explorer.findByRole("button", {
            name: "pg_authid",
        });
        await userEvent.click(pgAuthidEntity);

        await sleep(1000);

        const columnsEntity = await explorer.findByRole("button", {
            name: "Columns",
        });
        await userEvent.click(columnsEntity);

        await sleep(1000);

        const indexesEntity = await explorer.findByRole("button", {
            name: "Indexes",
        });
        await userEvent.click(indexesEntity);

        await sleep(1000);
    },
};

export const ExecuteCorrectQuery: Story = {
    tags: ["test"],
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(document.body);

        const connectionCombobox = await canvas.findByRole(
            "combobox",
            { name: /select connection/i },
            { timeout: 6000 },
        );
        await userEvent.click(connectionCombobox);
        await body.findByPlaceholderText("Search connections");
        const connectionOption = await body.findByRole("option", {
            name: "test",
        });
        await userEvent.click(connectionOption);

        const databaseCombobox = await canvas.findByRole("combobox", {
            name: /select database/i,
        });
        await userEvent.click(databaseCombobox);
        await body.findByPlaceholderText("Search databases");
        const databaseOption = await body.findByRole("option", {
            name: "postgres",
        });
        await userEvent.click(databaseOption);

        localStorage.setItem(
            "storybook:console:prefillQuery",
            "SELECT * FROM pg_tables",
        );

        await sleep(250);

        const executeButton = await canvas.findByRole("button", {
            name: "Execute",
        });
        await userEvent.click(executeButton);

        await body.findByText("schemaname", {}, { timeout: 15000 });
    },
};
