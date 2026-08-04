import React from "react";
import { createRoot, type Root } from "react-dom/client";
import {
    SereneDocsSearch,
    SereneDocsSearchButton,
    type SereneDocsSearchProps,
} from "@serenedb/docs-search-react";

export interface InitOptions
    extends Omit<SereneDocsSearchProps, "open" | "onOpenChange" | "container"> {
    /** Where to render the trigger button. Element or selector. Omit for modal-only. */
    container?: HTMLElement | string;
}

export interface Instance {
    open: () => void;
    close: () => void;
    destroy: () => void;
}

/**
 * Script-tag entry point:
 *
 *   <link rel="stylesheet" href=".../serene-docs-search.css">
 *   <script src=".../serene-docs-search.js"></script>
 *   <script>
 *     SereneDocsSearch.init({ container: '#search', backendUrl: 'http://localhost:7700' })
 *   </script>
 */
export function init(options: InitOptions = {}): Instance {
    const { container, ...props } = options;

    let mount: HTMLElement | null = null;
    if (typeof container === "string") {
        mount = document.querySelector<HTMLElement>(container);
        if (!mount) console.warn(`SereneDocsSearch: container "${container}" not found`);
    } else if (container instanceof HTMLElement) {
        mount = container;
    }

    const host = document.createElement("div");
    (mount ?? document.body).appendChild(host);
    const root: Root = createRoot(host);

    let setOpenExternal: ((open: boolean) => void) | null = null;

    function Wrapper(): React.ReactElement {
        const [open, setOpen] = React.useState(false);
        setOpenExternal = setOpen;
        return (
            <SereneDocsSearch
                {...props}
                trigger={props.trigger ?? Boolean(mount)}
                open={open}
                onOpenChange={setOpen}
            />
        );
    }

    root.render(<Wrapper />);

    return {
        open: () => setOpenExternal?.(true),
        close: () => setOpenExternal?.(false),
        destroy: () => {
            root.unmount();
            host.remove();
        },
    };
}

export { SereneDocsSearch, SereneDocsSearchButton };

/* Auto-init when the script tag carries data attributes:
   <script src="serene-docs-search.js"
           data-backend-url="http://localhost:7700"
           data-container="#search"></script> */
if (typeof document !== "undefined") {
    const script = document.currentScript as HTMLScriptElement | null;
    if (script?.dataset.backendUrl || script?.dataset.container) {
        const boot = () =>
            init({
                backendUrl: script.dataset.backendUrl,
                token: script.dataset.token,
                container: script.dataset.container,
                theme: (script.dataset.theme as "light" | "dark" | "auto") ?? "auto",
                placeholder: script.dataset.placeholder,
            });
        if (document.readyState === "loading") {
            document.addEventListener("DOMContentLoaded", boot, { once: true });
        } else {
            boot();
        }
    }
}
