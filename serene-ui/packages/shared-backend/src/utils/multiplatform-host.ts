import fs from "fs/promises";
import os from "os";
import { Socket } from "net";
import http from "http";
import { isDocker } from "./isDocker";

interface DockerInfo {
    OperatingSystem: string;
    KernelVersion: string;
    ServerVersion: string;
}

async function fetchDockerInfo(): Promise<DockerInfo> {
    return new Promise((resolve, reject) => {
        const options = {
            method: "GET",
            path: "/info",
            createConnection: () =>
                new Socket().connect("/var/run/docker.sock"),
        };

        const req = http.request(options, (res) => {
            let data = "";
            res.on("data", (chunk) => (data += chunk));
            res.on("end", () => {
                try {
                    const parsed = JSON.parse(data) as DockerInfo;
                    resolve(parsed);
                } catch (err) {
                    reject(err);
                }
            });
        });

        req.on("error", reject);
        req.end();
    });
}

async function getHostOS(): Promise<
    | { os: string; kernel: string; dockerVersion: string }
    | { info: string; version: string }
    | { error: string }
> {
    try {
        const info = await fetchDockerInfo();
        return {
            os: info.OperatingSystem,
            kernel: info.KernelVersion,
            dockerVersion: info.ServerVersion,
        };
    } catch {
        try {
            const version = await fs.readFile("/proc/version", "utf8");
            return { info: "Fallback", version: version.trim() };
        } catch {
            return { error: "Cannot determine host OS" };
        }
    }
}

/**
 * Returns the host for the current platform. Used for correctly identifying the host when running in a Docker container.
 * @param host - hostname or IP to normalize
 * @returns resolved host string
 */
export async function getMultiPlatformHost(host: string): Promise<string> {
    if (!isDocker()) return host;

    if (
        host !== "localhost" &&
        host !== "127.0.0.1" &&
        host !== "host.docker.internal"
    ) {
        return host;
    }

    await getHostOS();

    try {
        const dns = await import("dns/promises");
        await dns.lookup("host.docker.internal");
        return "host.docker.internal";
    } catch {}

    const iface = os.networkInterfaces();
    const dockerIface = iface["docker0"] || iface["eth0"];
    if (dockerIface?.[0]?.address) return dockerIface[0].address;

    return "127.0.0.1";
}
