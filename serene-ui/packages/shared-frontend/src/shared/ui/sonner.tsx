"use client";

import { useTheme } from "next-themes";
import { Toaster as Sonner, type ToasterProps } from "sonner";

const Toaster = ({ ...props }: ToasterProps) => {
    const { theme = "system" } = useTheme();

    return (
        <Sonner
            theme={theme as ToasterProps["theme"]}
            className="toaster group"
            style={
                {
                    "--normal-bg": "var(--popover)",
                    "--normal-text": "var(--popover-foreground)",
                    "--normal-border": "var(--border)",
                    "--z-index": 51,
                    "--pointer-event": "auto",
                    "--error-bg": "#5e1113",
                    "--error-border": "#662528",
                    "--error-text": "#dbbabb",
                    "--success-bg": "var(--color-green-950)",
                    "--success-text": "#b5beb9",
                } as React.CSSProperties
            }
            toastOptions={{
                style: {
                    pointerEvents: "auto",
                    height: "auto",
                    padding: "12px 16px",
                },
                classNames: {
                    content: "text-xs fade-in",
                    title: "font-medium",
                    description: "font-light opacity-70",
                },
            }}
            {...props}
        />
    );
};

export { Toaster };
