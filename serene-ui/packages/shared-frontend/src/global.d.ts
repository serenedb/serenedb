declare module "*.svg?react" {
    import * as React from "react";
    const ReactComponent: React.FunctionComponent<
        React.SVGProps<SVGSVGElement> & { title?: string }
    >;
    export default ReactComponent;
}

type Mode =
    | "dev-docker"
    | "dev-electron"
    | "prod-docker"
    | "prod-electron"
    | "test-docker";

interface ImportMetaEnv {
    MODE: string;
}

interface ImportMeta {
    readonly env: ImportMetaEnv;
}

interface Window {
    electronAPI: {
        invokeApi<T>(
            method: "GET" | "POST" | "PATCH" | "DELETE",
            url: string,
            options: Record<string, string> | unknown,
        ): Promise<{ data: T }>;
    };
}
