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
