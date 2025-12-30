import { expect } from "vitest";
import type { CompareOptions } from "../commands/compareScreenshot";

export interface MatchImageSnapshotOptions {
    testName?: string;
    baselineDir?: string;
    customSnapshotsDir?: string;
    diffDir?: string;
    customDiffDir?: string;
    currentDir?: string;
    threshold?: number;
    maxDiffPercentage?: number;
    failureThreshold?: number;
    failureThresholdType?: "pixel" | "percent";
    updateBaseline?: boolean;
}

declare global {
    namespace jest {
        interface Matchers<R, T = {}> {
            toMatchImageSnapshot(options?: MatchImageSnapshotOptions): R;
        }
    }
}

declare module "vitest" {
    interface Assertion<T = any> {
        toMatchImageSnapshot(
            options?: MatchImageSnapshotOptions,
        ): Promise<void>;
    }
    interface AsymmetricMatchersContaining {
        toMatchImageSnapshot(options?: MatchImageSnapshotOptions): any;
    }
}

expect.extend({
    async toMatchImageSnapshot(
        received: string | { path: string },
        options: MatchImageSnapshotOptions = {},
    ) {
        const testPath = this.testPath?.split("/") || [];
        const testName = testPath.at(-1) || "unknown-test";

        let screenshotPath: string;
        if (typeof received === "string") {
            screenshotPath = received;
        } else if (received && typeof received === "object" && received.path) {
            screenshotPath = received.path;
        } else {
            throw new Error(
                "Expected a screenshot path string or screenshot result object",
            );
        }

        const compareOptions: Partial<CompareOptions> = {
            testName,
            baselineDir: options.customSnapshotsDir || options.baselineDir,
            diffDir: options.customDiffDir || options.diffDir,
            currentDir: options.currentDir,
            threshold: options.threshold,
            updateBaseline: options.updateBaseline,
        };

        if (options.maxDiffPercentage !== undefined) {
            compareOptions.maxDiffPercentage = options.maxDiffPercentage;
        } else if (
            options.failureThreshold !== undefined &&
            options.failureThresholdType === "percent"
        ) {
            compareOptions.maxDiffPercentage = options.failureThreshold;
        } else if (options.failureThreshold !== undefined) {
            compareOptions.maxDiffPercentage = 1.0;
        }

        // Dynamically import commands from vitest/browser to ensure it's only used in browser mode
        const { commands } = await import("vitest/browser");

        const result = await (commands as any).compareScreenshot(
            screenshotPath,
            compareOptions as CompareOptions,
        );

        return {
            pass: result.matches,
            message: () => result.message,
        };
    },
});
