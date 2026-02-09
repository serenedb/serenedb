import React, { useEffect, useRef } from "react";

import * as monaco from "monaco-editor";
// Vite-specific worker imports to enable Monaco language features (IntelliSense)
// These are required so Monaco can spawn the appropriate web workers per language.
// They are safe to import multiple times;

// @ts-ignore - Vite query is valid at build time
import EditorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
// @ts-ignore - Vite query is valid at build time
import JsonWorker from "monaco-editor/esm/vs/language/json/json.worker?worker";
// @ts-ignore - Vite query is valid at build time
import CssWorker from "monaco-editor/esm/vs/language/css/css.worker?worker";
// @ts-ignore - Vite query is valid at build time
import HtmlWorker from "monaco-editor/esm/vs/language/html/html.worker?worker";
// @ts-ignore - Vite query is valid at build time
import TsWorker from "monaco-editor/esm/vs/language/typescript/ts.worker?worker";
import { useResizeObserver } from "../hooks";
import { useChangeTheme } from "@serene-ui/shared-frontend/features";

const _self = self as any;
if (!_self.MonacoEnvironment || !_self.MonacoEnvironment.getWorker) {
    _self.MonacoEnvironment = {
        getWorker(_: string, label: string) {
            switch (label) {
                case "json":
                    return new JsonWorker();
                case "css":
                    return new CssWorker();
                case "html":
                    return new HtmlWorker();
                case "typescript":
                case "javascript":
                    return new TsWorker();
                default:
                    return new EditorWorker();
            }
        },
    };
}

type MonacoEditorProps = {
    value?: string;
    language?: string;
    theme?: string;
    options?: monaco.editor.IStandaloneEditorConstructionOptions;
    beforeMount?: (monaco: typeof import("monaco-editor")) => void;
    onMount?: (
        editor: monaco.editor.IStandaloneCodeEditor,
        monaco: typeof import("monaco-editor"),
    ) => void;
    onChange?: (value: string) => void;
    onExecute?: () => void;
    onExecuteInNewTab?: () => void;
};

export const MonacoEditor = React.forwardRef<HTMLElement, MonacoEditorProps>(
    (
        {
            value = "",
            language = "pgsql",
            theme = "myTheme",
            options = {},
            beforeMount,
            onMount,
            onChange,
            onExecute,
            onExecuteInNewTab,
        },
        forwardedRef,
    ) => {
        const { theme: globalTheme } = useChangeTheme();
        const { ref: resizeRef } = useResizeObserver();
        const containerRef = useRef<HTMLDivElement | null>(null);
        const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(
            null,
        );
        const isUpdatingFromPropRef = useRef(false);
        const onExecuteRef = useRef(onExecute);
        const onExecuteInNewTabRef = useRef(onExecuteInNewTab);

        useEffect(() => {
            onExecuteRef.current = onExecute;
            onExecuteInNewTabRef.current = onExecuteInNewTab;
        }, [onExecute, onExecuteInNewTab]);

        useEffect(() => {
            if (!containerRef.current) return;

            beforeMount?.(monaco);

            monaco.editor.defineTheme("myTheme", {
                base: "vs-dark",
                inherit: true,
                rules: [
                    {
                        token: "comment",
                        foreground: "#ffa500",
                        fontStyle: "medium",
                    },
                    { token: "keyword", foreground: "#895AF8" },
                    { token: "type", foreground: "#4EC9B0" },
                    { token: "string", foreground: "#CE9178" },
                    { token: "number", foreground: "#80BEFF" },
                ],
                colors: {
                    "editor.background": "#1E1E1E",
                    "editor.foreground":
                        globalTheme === "dark" ? "#B6B7B9" : "#506182",
                    "editorLineNumber.foreground": "#57575D",
                    "editorLineNumber.activeForeground": "#D4D4D4",
                    "editorCursor.foreground": "#A6ACCD",
                },
            });

            const editor = monaco.editor.create(containerRef.current, {
                value,
                language,
                theme: "myTheme",
                fontFamily: "monospace",
                automaticLayout: true,
                fontSize: 14,
                fontLigatures: true,
                lineNumbers: "on",
                lineNumbersMinChars: 3,
                lineDecorationsWidth: 0,
                ...options,
            });

            editorRef.current = editor;

            if (typeof forwardedRef === "function") {
            } else if (forwardedRef && forwardedRef.current) {
                (forwardedRef.current as any).__monacoEditor = editor;
            }

            onMount?.(editor, monaco);

            if (onExecute) {
                editor.addAction({
                    id: "execute-query",
                    label: "Execute Query",
                    keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
                    run: () => {
                        onExecuteRef.current?.();
                    },
                });
            }

            if (onExecuteInNewTab) {
                editor.addAction({
                    id: "execute-query-new-tab",
                    label: "Execute Query in New Tab",
                    keybindings: [
                        monaco.KeyMod.CtrlCmd |
                            monaco.KeyMod.Shift |
                            monaco.KeyCode.Enter,
                    ],
                    run: () => {
                        onExecuteInNewTabRef.current?.();
                    },
                });
            }
            const domNode = editor.getDomNode();
            const handleKeyDown = (e: KeyboardEvent) => {
                const isCtrlCmd = e.ctrlKey || e.metaKey;
                if (
                    isCtrlCmd &&
                    (e.key === "l" ||
                        e.key === "e" ||
                        e.key === "j" ||
                        e.key === "t" ||
                        e.key === "w" ||
                        e.key === "e")
                ) {
                    e.stopImmediatePropagation();
                    const newEvent = new KeyboardEvent("keydown", {
                        key: e.key,
                        code: e.code,
                        ctrlKey: e.ctrlKey,
                        metaKey: e.metaKey,
                        shiftKey: e.shiftKey,
                        altKey: e.altKey,
                        bubbles: true,
                        cancelable: true,
                    });
                    domNode?.parentElement?.dispatchEvent(newEvent);
                }
            };
            domNode?.addEventListener("keydown", handleKeyDown, true);

            const sub = editor.onDidChangeModelContent(() => {
                if (!isUpdatingFromPropRef.current) {
                    onChange?.(editor.getValue());
                }
            });

            return () => {
                domNode?.removeEventListener("keydown", handleKeyDown, true);
                sub.dispose();
                editor.dispose();
            };
        }, []);

        useEffect(() => {
            const editor = editorRef.current;
            if (editor && value !== undefined && value !== editor.getValue()) {
                const model = editor.getModel();
                if (model) {
                    isUpdatingFromPropRef.current = true;
                    editor.pushUndoStop();
                    model.pushEditOperations(
                        [],
                        [
                            {
                                range: model.getFullModelRange(),
                                text: value,
                            },
                        ],
                        () => null,
                    );
                    editor.pushUndoStop();
                    isUpdatingFromPropRef.current = false;
                }
            }
        }, [value]);

        useEffect(() => {
            const editor = editorRef.current;
            if (editor) {
                const model = editor.getModel();
                if (model) {
                    monaco.editor.setModelLanguage(model, language);
                }
            }
        }, [language]);

        useEffect(() => {
            monaco.editor.setTheme(theme);
        }, [theme]);

        useEffect(() => {
            console.log(globalTheme);
            const isDark = globalTheme === "dark";
            monaco.editor.defineTheme("myTheme", {
                base: isDark ? "vs-dark" : "vs",
                inherit: true,
                rules: [
                    {
                        token: "comment",
                        foreground: "#ffa500",
                        fontStyle: "medium",
                    },
                    { token: "keyword", foreground: "#895AF8" },
                    { token: "type", foreground: "#4EC9B0" },
                    { token: "string", foreground: "#CE9178" },
                    { token: "number", foreground: "#80BEFF" },
                ],
                colors: {
                    "editor.background": isDark ? "#1E1E1E" : "#FFFFFF",
                    "editor.foreground": isDark ? "#B6B7B9" : "#506182",
                    "editorLineNumber.foreground": isDark
                        ? "#57575D"
                        : "#A0A0A0",
                    "editorLineNumber.activeForeground": isDark
                        ? "#D4D4D4"
                        : "#333333",
                    "editorCursor.foreground": isDark ? "#A6ACCD" : "#506182",
                },
            });
            monaco.editor.setTheme("myTheme");
        }, [globalTheme]);

        useEffect(() => {
            const editor = editorRef.current;
            if (editor) {
                editor.updateOptions(options);
            }
        }, [options]);

        return (
            <div
                className="w-full h-full flex-1"
                ref={(node) => {
                    resizeRef(node);
                    if (typeof forwardedRef === "function") {
                        forwardedRef(node);
                    } else if (forwardedRef) {
                        forwardedRef.current = node;
                    }
                }}>
                <div ref={containerRef} className="w-full h-full" />
            </div>
        );
    },
);
