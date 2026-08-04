import React, { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useResolvedTheme } from "./hooks/useResolvedTheme";
import {
    useSereneDocsSearch,
    type UseSereneDocsSearchOptions,
} from "./hooks/useSereneDocsSearch";
import { applyPendingHighlight } from "./lib/highlight";
import { SearchScreen } from "./search/SearchScreen";
import type { SereneDocsSearchMcpOptions } from "./search/McpSetup";
import { SetupScreen } from "./setup/SetupScreen";
import { useSetupFlow } from "./setup/useSetupFlow";

export interface SereneDocsSearchProps extends UseSereneDocsSearchOptions {
    /** "light" | "dark" | "auto" (default: auto — follows the host page). */
    theme?: "light" | "dark" | "auto";
    placeholder?: string;
    /** Render the built-in trigger button (default true). */
    trigger?: boolean;
    triggerLabel?: string;
    /** First-run setup wizard: "auto" shows it when no backend is known. */
    setup?: "auto" | "never";
    /** Configure the MCP setup tab, or pass false to hide it. */
    mcp?: false | SereneDocsSearchMcpOptions;
    /** Portal target (default document.body). */
    container?: HTMLElement;
    zIndex?: number;
}

export function SereneDocsSearch(props: SereneDocsSearchProps): React.ReactElement | null {
    const {
        theme = "auto",
        placeholder = "Search docs or ask a question…",
        trigger = true,
        triggerLabel = "Search docs…",
        setup = "auto",
        mcp = {},
        container,
        zIndex,
        ...hookOptions
    } = props;

    const search = useSereneDocsSearch(hookOptions);
    const resolvedTheme = useResolvedTheme(theme);

    const [toast, setToast] = useState("");
    const toastTimer = useRef<number | undefined>(undefined);
    const showToast = useCallback((msg: string) => {
        window.clearTimeout(toastTimer.current);
        setToast(msg);
        toastTimer.current = window.setTimeout(() => setToast(""), 2600);
    }, []);

    const flow = useSetupFlow({
        search,
        setup,
        pinnedBackend: Boolean(hookOptions.backendUrl),
        toast: showToast,
    });

    useEffect(() => {
        applyPendingHighlight();
    }, []);

    // lock host scroll while the modal is open
    useEffect(() => {
        if (!search.open) return;
        const prev = document.documentElement.style.overflow;
        document.documentElement.style.overflow = "hidden";
        return () => {
            document.documentElement.style.overflow = prev;
        };
    }, [search.open]);

    const rootStyle = zIndex != null ? ({ "--sds-z": String(zIndex) } as React.CSSProperties) : undefined;
    const portalTarget = container ?? (typeof document !== "undefined" ? document.body : null);

    const overlay =
        search.open && portalTarget
            ? createPortal(
                  <div className="sds-root" data-sds-theme={resolvedTheme} style={rootStyle}>
                      {toast && <div className="sds-toast">{toast}</div>}
                      <div
                          className="sds-overlay"
                          onMouseDown={(e) => {
                              if (e.target === e.currentTarget) search.setOpen(false);
                          }}
                      >
                          <div className="sds-modal-frame" role="dialog" aria-modal="true">
                              <div className="sds-modal">
                                  {flow.phase === "search" ? (
                                      <SearchScreen
                                          search={search}
                                          placeholder={placeholder}
                                          onResetSetup={flow.canReset ? flow.resetSetup : undefined}
                                          mcp={mcp}
                                      />
                                  ) : (
                                      <SetupScreen
                                          flow={flow}
                                          toast={showToast}
                                          onClose={() => search.setOpen(false)}
                                      />
                                  )}
                              </div>
                          </div>
                      </div>
                  </div>,
                  portalTarget,
              )
            : null;

    return (
        <>
            {trigger && (
                <SereneDocsSearchButton
                    label={triggerLabel}
                    hotkeyLabel={search.hotkeyLabel}
                    theme={resolvedTheme}
                    onClick={() => search.setOpen(true)}
                />
            )}
            {overlay}
        </>
    );
}

export function SereneDocsSearchButton({
    label = "Search docs…",
    hotkeyLabel,
    theme,
    onClick,
    className,
    frame = true,
}: {
    label?: string;
    hotkeyLabel?: string;
    theme?: "light" | "dark";
    onClick?: () => void;
    className?: string;
    /** Render the ASCII drop shadow around the button (default true). */
    frame?: boolean;
}): React.ReactElement {
    const button = (
        <button type="button" className="sds-trigger" onClick={onClick}>
            <span className="sds-trigger-gt">&gt;</span>
            <span className="sds-trigger-label">{label}</span>
            {hotkeyLabel && <span className="sds-kbd">{hotkeyLabel}</span>}
        </button>
    );
    return (
        <span className={`sds-root ${className ?? ""}`} data-sds-theme={theme ?? "light"}>
            {frame ? <span className="sds-trigger-frame">{button}</span> : button}
        </span>
    );
}
