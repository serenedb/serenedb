import type {
    CompareOptions,
    CompareResult,
} from "../commands/compareScreenshot";

declare module "vitest/browser" {
    interface BrowserCommands {
        compareScreenshot(
            screenshotPath: string,
            options: CompareOptions,
        ): Promise<CompareResult>;
    }

    interface BrowserPage {
        screenshot(options?: {
            fullPage?: boolean;
            clip?: { x: number; y: number; width: number; height: number };
            path?: string;
            type?: "png" | "jpeg";
            quality?: number;
        }): Promise<string>;
    }
}

export {};
