export {};

declare global {
    interface Window {
        electronAPI: {
            invokeApi<T = any>(
                method: "GET" | "POST" | "PATCH" | "DELETE",
                url: string,
                data?: unknown,
            ): Promise<{ statusCode: number; data: T }>;
        };
    }
}
