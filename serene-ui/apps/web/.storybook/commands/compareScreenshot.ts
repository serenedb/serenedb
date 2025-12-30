import { promises as fs } from "node:fs";
import path from "node:path";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

export interface CompareOptions {
    testName: string;
    baselineDir?: string;
    diffDir?: string;
    currentDir?: string;
    threshold?: number;
    maxDiffPercentage?: number;
    updateBaseline?: boolean;
}

export interface CompareResult {
    matches: boolean;
    diffPercentage?: number;
    message: string;
}

export const compareScreenshot = async (
    context: { testPath?: string },
    screenshotPath: string,
    options: CompareOptions,
): Promise<CompareResult> => {
    const { testPath } = context;
    if (!testPath) {
        throw new Error("testPath is not available in the test context");
    }

    const projectRoot = process.cwd();
    const {
        testName,
        baselineDir = `${projectRoot}/screenshots/snapshots`,
        diffDir = `${projectRoot}/screenshots/diffs`,
        currentDir = `${projectRoot}/screenshots/current`,
        threshold = 0.1,
        maxDiffPercentage = 1.0,
        updateBaseline = process.env.UPDATE_SNAPSHOTS === "true",
    } = options;

    const testBaselineDir = path.join(baselineDir, testName);
    const testDiffDir = path.join(diffDir, testName);
    const testCurrentDir = path.join(currentDir, testName);

    const filename = path.basename(screenshotPath);
    const baselinePath = path.join(testBaselineDir, filename);
    const diffPath = path.join(testDiffDir, `diff-${filename}`);
    const currentPath = path.join(testCurrentDir, filename);

    await fs.mkdir(testBaselineDir, { recursive: true });
    await fs.mkdir(testDiffDir, { recursive: true });
    await fs.mkdir(testCurrentDir, { recursive: true });

    await fs.copyFile(screenshotPath, currentPath);

    if (updateBaseline) {
        await fs.copyFile(screenshotPath, baselinePath);
        return {
            matches: true,
            message: `Updated baseline image: ${baselinePath}`,
        };
    }

    try {
        await fs.access(baselinePath);

        const [img1Data, img2Data] = await Promise.all([
            fs.readFile(baselinePath),
            fs.readFile(screenshotPath),
        ]);

        const img1 = PNG.sync.read(img1Data);
        const img2 = PNG.sync.read(img2Data);

        if (img1.width !== img2.width || img1.height !== img2.height) {
            return {
                matches: false,
                message: `Image dimensions don't match: ${img1.width}x${img1.height} vs ${img2.width}x${img2.height}`,
            };
        }

        const { width, height } = img1;
        const diff = new PNG({ width, height });

        const numDiffPixels = pixelmatch(
            img1.data,
            img2.data,
            diff.data,
            width,
            height,
            { threshold },
        );

        await fs.writeFile(diffPath, PNG.sync.write(diff));

        const diffPercentage = (numDiffPixels / (width * height)) * 100;
        const matches = diffPercentage <= maxDiffPercentage;

        return {
            matches,
            diffPercentage,
            message: matches
                ? `Image matches baseline (diff: ${diffPercentage.toFixed(2)}%)`
                : `Image differs from baseline by ${diffPercentage.toFixed(
                      2,
                  )}% (threshold: ${maxDiffPercentage}%). See diff: ${diffPath}`,
        };
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
            await fs.copyFile(screenshotPath, baselinePath);
            return {
                matches: true,
                message: `Created new baseline image: ${baselinePath}`,
            };
        }
        throw error;
    }
};
