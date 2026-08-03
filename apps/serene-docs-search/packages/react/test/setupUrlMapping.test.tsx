import React, { useState } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
    defaultConfig,
    type SereneSearchConfig,
} from "@serenedb/docs-search-core";
import { StepContent } from "../src/setup/steps/StepContent";
import { stepValid } from "../src/setup/validation";

function Harness(): React.ReactElement {
    const [config, setConfig] = useState<SereneSearchConfig>(() =>
        defaultConfig({ type: "folder", path: "/data/docs" }),
    );
    const update = (mutate: (next: SereneSearchConfig) => void) =>
        setConfig((current) => {
            const next = JSON.parse(JSON.stringify(current)) as SereneSearchConfig;
            mutate(next);
            return next;
        });

    return (
        <>
            <StepContent config={config} update={update} />
            <output data-testid="config">{JSON.stringify(config)}</output>
            <output data-testid="valid">{String(stepValid(config, 2))}</output>
        </>
    );
}

describe("setup URL mapping", () => {
    it("configures ordered path-to-domain rules for a multi-site corpus", () => {
        render(<Harness />);

        fireEvent.click(screen.getByRole("button", { name: "+ add mapping" }));
        expect(screen.getByTestId("valid").textContent).toBe("false");
        fireEvent.change(screen.getByPlaceholderText("blog/**"), {
            target: { value: "blog/**" },
        });
        fireEvent.change(screen.getByPlaceholderText("https://blog.example.com"), {
            target: { value: "https://blog.example.com" },
        });
        fireEvent.change(screen.getByPlaceholderText("blog/"), {
            target: { value: "blog/" },
        });

        fireEvent.click(screen.getByRole("button", { name: "+ add mapping" }));
        fireEvent.change(screen.getByPlaceholderText("**"), {
            target: { value: "**" },
        });
        fireEvent.change(screen.getByPlaceholderText("https://docs.example.com"), {
            target: { value: "https://docs.example.com" },
        });

        const config = JSON.parse(screen.getByTestId("config").textContent || "{}") as SereneSearchConfig;
        expect(config.content.urlMapping?.rules).toEqual([
            {
                match: "blog/**",
                baseUrl: "https://blog.example.com",
                stripPrefix: "blog/",
            },
            {
                match: "**",
                baseUrl: "https://docs.example.com",
            },
        ]);
        expect(screen.getByTestId("valid").textContent).toBe("true");
    });
});
