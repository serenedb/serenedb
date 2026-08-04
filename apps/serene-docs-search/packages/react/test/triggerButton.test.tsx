import { cleanup, fireEvent, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SereneDocsSearchButton } from "../src/SereneDocsSearch";

afterEach(cleanup);

describe("SereneDocsSearchButton", () => {
    it("renders the framed (ASCII shadow) variant by default", () => {
        const { container } = render(<SereneDocsSearchButton hotkeyLabel="⌘K" />);
        expect(container.querySelector(".sds-trigger-frame .sds-trigger")).not.toBeNull();
        expect(container.textContent).toContain("Search docs…");
        expect(container.textContent).toContain("⌘K");
    });

    it("frame={false} drops the shadow wrapper but keeps the button", () => {
        const onClick = vi.fn();
        const { container } = render(
            <SereneDocsSearchButton frame={false} theme="dark" onClick={onClick} />,
        );
        expect(container.querySelector(".sds-trigger-frame")).toBeNull();
        const btn = container.querySelector<HTMLButtonElement>(".sds-trigger")!;
        expect(btn).not.toBeNull();
        expect(container.querySelector(".sds-root")?.getAttribute("data-sds-theme")).toBe("dark");
        fireEvent.click(btn);
        expect(onClick).toHaveBeenCalled();
    });
});
