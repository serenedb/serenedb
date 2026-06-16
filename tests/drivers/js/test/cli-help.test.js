import { describe, expect, test } from "vitest";
import {
  DEFAULT_CLI_HELP_REFERENCE_PATH,
  buildCliHelpReference,
  compareCliHelpReference,
} from "../cli_help.js";

describe("serened CLI help reference", () => {
  test("matches the generated CLI help reference", () => {
    const actual = buildCliHelpReference();
    const result = compareCliHelpReference({ actual });

    if (!result.matches) {
      console.error(
        [
          "serened CLI help reference is out of date.",
          "",
          result.diff,
          "",
          `If this change is expected, run: npm run generate:cli-test`,
          `Reference file: ${DEFAULT_CLI_HELP_REFERENCE_PATH}`,
        ].join("\n"),
      );
    }

    expect(result.matches).toBe(true);
  });
});
