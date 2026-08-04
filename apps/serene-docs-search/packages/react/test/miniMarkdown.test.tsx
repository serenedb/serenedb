import { cleanup, fireEvent, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MiniMarkdown } from "../src/components/MiniMarkdown";

afterEach(cleanup);

describe("MiniMarkdown", () => {
    it("renders paragraphs with bold, inline code and links", () => {
        const { container } = render(
            <MiniMarkdown text={"Use **VACUUM** with `REFRESH`.\n\nSee [docs](/docs/vacuum)."} />,
        );
        expect(container.querySelectorAll("p")).toHaveLength(2);
        expect(container.querySelector("strong")?.textContent).toBe("VACUUM");
        expect(container.querySelector("code")?.textContent).toBe("REFRESH");
        const a = container.querySelector("a")!;
        expect(a.getAttribute("href")).toBe("/docs/vacuum");
        expect(a.textContent).toBe("docs");
    });

    it("renders fenced code blocks verbatim, without inline parsing", () => {
        const { container } = render(
            <MiniMarkdown text={"Intro\n\n```sql\nSELECT **not bold** FROM t;\n```"} />,
        );
        const pre = container.querySelector("pre code")!;
        expect(pre.textContent).toBe("SELECT **not bold** FROM t;");
        expect(pre.querySelector("strong")).toBeNull();
    });

    it("groups consecutive list items into one list", () => {
        const { container } = render(
            <MiniMarkdown text={"- first\n- second\n1. third"} />,
        );
        const items = [...container.querySelectorAll("ul li")].map((li) => li.textContent);
        expect(items).toEqual(["first", "second", "third"]);
    });

    it("turns [n] into citation buttons that call onCitation", () => {
        const onCitation = vi.fn();
        const { getAllByRole } = render(
            <MiniMarkdown text={"VACUUM REFRESH rebuilds the index [2]."} onCitation={onCitation} />,
        );
        const btn = getAllByRole("button").find((b) => b.textContent === "[2]")!;
        fireEvent.click(btn);
        expect(onCitation).toHaveBeenCalledWith(2);
    });

    it("keeps rendering while a stream is mid-sentence", () => {
        const { container } = render(<MiniMarkdown text={"Partial answer without trailing"} />);
        expect(container.querySelector("p")?.textContent).toBe("Partial answer without trailing");
    });

    it("renders ### headings as heading blocks, not literal hashes", () => {
        const { container } = render(
            <MiniMarkdown text={"### How to setup hybrid search\nYou need an index.\n\n## Steps"} />,
        );
        const hs = [...container.querySelectorAll(".sds-md-h")];
        expect(hs.map((h) => [h.textContent, h.getAttribute("data-level")])).toEqual([
            ["How to setup hybrid search", "3"],
            ["Steps", "2"],
        ]);
        expect(container.textContent).not.toContain("###");
        expect(container.querySelector("p")?.textContent).toBe("You need an index.");
    });

    it("syntax-highlights fenced code: keywords, strings, numbers, comments", () => {
        const { container } = render(
            <MiniMarkdown
                text={"```sql\nSELECT id FROM t WHERE a = 'x' LIMIT 5; -- top rows\n```"}
            />,
        );
        const code = container.querySelector("pre code")!;
        const byClass = (cls: string) =>
            [...code.querySelectorAll(`.${cls}`)].map((el) => el.textContent);
        expect(byClass("sds-tok-k")).toContain("SELECT");
        expect(byClass("sds-tok-k")).toContain("LIMIT");
        expect(byClass("sds-tok-s")).toEqual(["'x'"]);
        expect(byClass("sds-tok-n")).toEqual(["5"]);
        expect(byClass("sds-tok-c")).toEqual(["-- top rows"]);
        expect(code.textContent).toBe("SELECT id FROM t WHERE a = 'x' LIMIT 5; -- top rows");
    });
});
