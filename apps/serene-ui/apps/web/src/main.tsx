import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { App } from "@/app";

// Suppress benign ResizeObserver errors in Electron
// This error occurs when ResizeObserver callbacks trigger layout changes
// that cause more resize observations, which is common with resizable panels
if (typeof window !== "undefined") {
    const resizeObserverErrHandler = (e: ErrorEvent) => {
        if (
            e.message ===
                "ResizeObserver loop completed with undelivered notifications." ||
            e.message === "ResizeObserver loop limit exceeded"
        ) {
            e.stopImmediatePropagation();
            return;
        }
    };
    window.addEventListener("error", resizeObserverErrHandler);
}

createRoot(document.getElementById("root")!).render(
    <StrictMode>
        <App />
    </StrictMode>,
);
