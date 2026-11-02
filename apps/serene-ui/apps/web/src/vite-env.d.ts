/// <reference types="vite/client" />
/// <reference types="vite-plugin-svgr/client" />

interface ElectronAPI {
    invokeApi<T = any>(
        method: "GET" | "POST" | "PATCH" | "DELETE",
        url: string,
        data?: unknown,
    ): Promise<{ statusCode: number; data: T }>;
}

declare global {
    interface Window {
        electronAPI: ElectronAPI;
    }
}
