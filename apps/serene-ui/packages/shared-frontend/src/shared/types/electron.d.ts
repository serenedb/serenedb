export {};

declare global {
    interface Window {
        sereneTheme?: {
            preference: "dark" | "light" | "system";
            setPreference: (theme: "dark" | "light" | "system") => void;
        };
        electronAPI: {
            invokeApi<T = any>(
                method: "GET" | "POST" | "PATCH" | "DELETE",
                url: string,
                data?: unknown,
            ): Promise<{ statusCode: number; data: T }>;
        };
    }
}
