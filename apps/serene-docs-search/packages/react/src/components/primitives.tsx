import React from "react";

/** SereneDB mini logo, inlined so the package has no asset dependencies. */
export function Logo({ className }: { className?: string }): React.ReactElement {
    return (
        <svg
            className={className}
            width="16"
            height="16"
            viewBox="0 0 28 28"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
            aria-hidden="true"
        >
            <path
                d="M28 14C28 6.26801 21.732 0 14 0C6.26801 0 0 6.26801 0 14C0 21.732 6.26801 28 14 28C21.732 28 28 21.732 28 14Z"
                fill="#895AF8"
            />
            <path
                d="M9.34524 18.6549C13.9013 23.211 20.7606 23.7386 24.6659 19.8334C28.5711 15.9281 28.0435 9.06884 23.4874 4.51273C18.9313 -0.0433921 12.072 -0.571025 8.16674 3.33422C4.2615 7.23947 4.78913 14.0988 9.34524 18.6549Z"
                fill="#80BEFF"
            />
            <path
                d="M15.6488 12.3503C17.9268 14.6283 21.3248 14.9238 23.2384 13.0102C25.1519 11.0967 24.8565 7.69869 22.5784 5.42063C20.3004 3.14257 16.9024 2.8471 14.9888 4.76067C13.0752 6.67424 13.3707 10.0722 15.6488 12.3503Z"
                fill="white"
            />
        </svg>
    );
}

/** lucide circle-user, inlined — marks the user's messages in the AI chat. */
export function UserIcon({ className }: { className?: string }): React.ReactElement {
    return (
        <svg
            className={className}
            xmlns="http://www.w3.org/2000/svg"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
        >
            <circle cx="12" cy="12" r="10" />
            <circle cx="12" cy="10" r="3" />
            <path d="M7 20.662V19a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v1.662" />
        </svg>
    );
}

export function Button({
    variant = "default",
    size,
    className,
    ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
    variant?: "default" | "ghost" | "secondary";
    size?: "sm";
}): React.ReactElement {
    const cls = [
        "sds-btn",
        `sds-btn-${variant}`,
        size === "sm" ? "sds-btn-sm" : "",
        className ?? "",
    ]
        .filter(Boolean)
        .join(" ");
    return <button type="button" className={cls} {...rest} />;
}

export function Field({
    label,
    className,
    children,
}: {
    label: string;
    className?: string;
    children: React.ReactNode;
}): React.ReactElement {
    return (
        <div className={className}>
            <div className="sds-field-label">{label}</div>
            {children}
        </div>
    );
}

export function CheckLine({
    on,
    onToggle,
    children,
}: {
    on: boolean;
    onToggle: () => void;
    children: React.ReactNode;
}): React.ReactElement {
    return (
        <button type="button" className="sds-check" onClick={onToggle}>
            <span className="sds-check-glyph">{on ? "[x]" : "[ ]"}</span>
            <span>{children}</span>
        </button>
    );
}

/** Colorized YAML/JSON terminal preview (keys purple, values blue, comments dim). */
export function TerminalCode({ text }: { text: string }): React.ReactElement {
    return (
        <pre className="sds-terminal-pre">
            {text.split("\n").map((line, i) => (
                <React.Fragment key={i}>
                    {colorizeLine(line)}
                    {"\n"}
                </React.Fragment>
            ))}
        </pre>
    );
}

function colorizeLine(line: string): React.ReactNode {
    const comment = line.indexOf("#");
    let main = comment >= 0 ? line.slice(0, comment) : line;
    const trail = comment >= 0 ? <span className="c">{line.slice(comment)}</span> : null;
    const m = /^(\s*(?:- )?)("?[\w."-]+"?)(\s*:)(.*)$/.exec(main);
    if (m) {
        return (
            <>
                {m[1]}
                <span className="k">{m[2]}</span>
                {m[3]}
                <span className="v">{m[4]}</span>
                {trail}
            </>
        );
    }
    return (
        <>
            <span className="v">{main}</span>
            {trail}
        </>
    );
}
