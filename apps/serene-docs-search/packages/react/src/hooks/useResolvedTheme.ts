import { useCallback, useEffect, useState } from "react";

/** Follow the host page's theme: docusaurus [data-theme], .dark class, or media query. */
export function useResolvedTheme(theme: "light" | "dark" | "auto"): "light" | "dark" {
    const detect = useCallback((): "light" | "dark" => {
        if (theme !== "auto") return theme;
        if (typeof document !== "undefined") {
            const html = document.documentElement;
            const attr = html.getAttribute("data-theme");
            if (attr === "dark" || attr === "light") return attr;
            if (html.classList.contains("dark")) return "dark";
        }
        if (typeof window !== "undefined" && window.matchMedia?.("(prefers-color-scheme: dark)").matches) {
            return "dark";
        }
        return "light";
    }, [theme]);

    const [resolved, setResolved] = useState<"light" | "dark">(detect);

    useEffect(() => {
        setResolved(detect());
        if (theme !== "auto" || typeof window === "undefined") return;
        const observer = new MutationObserver(() => setResolved(detect()));
        observer.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ["data-theme", "class"],
        });
        const mq = window.matchMedia?.("(prefers-color-scheme: dark)");
        const onMq = () => setResolved(detect());
        mq?.addEventListener?.("change", onMq);
        return () => {
            observer.disconnect();
            mq?.removeEventListener?.("change", onMq);
        };
    }, [theme, detect]);

    return resolved;
}
