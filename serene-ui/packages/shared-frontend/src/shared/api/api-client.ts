/// <reference types="vite/client" />

export interface BaseApiResponse<T> {
    success: boolean;
    message: string;
    data?: T;
}

export interface BaseApiError {
    success: boolean;
    message: string;
    data?: undefined;
}

interface ApiClient {
    request<T>(
        method: "GET" | "POST" | "PATCH" | "DELETE",
        url: string,
        options?: { params?: Record<string, string>; body?: unknown },
    ): Promise<BaseApiResponse<T>>;

    get<T>(
        url: string,
        params?: Record<string, string>,
    ): Promise<BaseApiResponse<T>>;
    post<T>(url: string, body?: unknown): Promise<BaseApiResponse<T>>;
    patch<T>(url: string, body?: unknown): Promise<BaseApiResponse<T>>;
    delete<T>(url: string, body?: unknown): Promise<BaseApiResponse<T>>;
}

const { MODE } = import.meta.env;
const BASE_API_URL =
    MODE === "test" ? "http://127.0.0.1:3636/api" : "http://127.0.0.1:3000/api";

class FetchApiClient implements ApiClient {
    async request<T>(
        method: "GET" | "POST" | "PATCH" | "DELETE",
        url: string,
        options: { params?: Record<string, string>; body?: unknown } = {},
    ): Promise<BaseApiResponse<T>> {
        const { params, body } = options;
        const query = params
            ? "?" + new URLSearchParams(params).toString()
            : "";
        let res;
        try {
            res = await fetch(BASE_API_URL + url + query, {
                method,
                headers: { "Content-Type": "application/json" },
                body: body ? JSON.stringify(body) : undefined,
            });
        } catch (error) {
            return error as BaseApiError;
        }

        let payload: unknown;
        try {
            payload = await res?.json();
        } catch {
            payload = await res?.text();
        }

        if (res && !res?.ok) {
            return payload as BaseApiError;
        }

        return payload as BaseApiResponse<T>;
    }

    async get<T>(url: string, params?: Record<string, string>) {
        return this.request<T>("GET", url, { params });
    }
    async post<T>(url: string, body?: unknown) {
        return this.request<T>("POST", url, { body });
    }
    async patch<T>(url: string, body?: unknown) {
        return this.request<T>("PATCH", url, { body });
    }
    async delete<T>(url: string, body?: unknown) {
        return this.request<T>("DELETE", url, { body });
    }
}

class ElectronApiClient implements ApiClient {
    async request<T>(
        method: "GET" | "POST" | "PATCH" | "DELETE",
        url: string,
        options: { params?: Record<string, string>; body?: unknown } = {},
    ): Promise<BaseApiResponse<T>> {
        const result = await window.electronAPI.invokeApi<BaseApiResponse<T>>(
            method,
            url,
            options.params ?? options.body,
        );

        return result.data;
    }

    get<T>(url: string, params?: Record<string, string>) {
        return this.request<T>("GET", url, { params });
    }
    post<T>(url: string, body?: unknown) {
        return this.request<T>("POST", url, { body });
    }
    patch<T>(url: string, body?: unknown) {
        return this.request<T>("PATCH", url, { body });
    }
    delete<T>(url: string, body?: unknown) {
        return this.request<T>("DELETE", url, { body });
    }
}

function createApiClient(): ApiClient {
    const mode = import.meta.env.MODE as Mode;

    if (mode && mode.includes("electron")) {
        return new ElectronApiClient();
    }
    return new FetchApiClient();
}

export const apiClient = createApiClient();
