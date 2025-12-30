import fsSync from "fs";

/**
 * Checks if the current process is running inside a Docker container.
 * @returns {boolean} - True if running inside a Docker container, false otherwise.
 */
export function isDocker(): boolean {
    try {
        if (fsSync.existsSync("/.dockerenv")) return true;
        const cgroup = fsSync.readFileSync("/proc/1/cgroup", "utf8");
        return cgroup.includes("docker");
    } catch {
        return false;
    }
}
