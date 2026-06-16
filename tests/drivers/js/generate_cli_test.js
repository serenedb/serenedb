#!/usr/bin/env node
import path from "node:path";
import {
  DEFAULT_CLI_HELP_REFERENCE_PATH,
  writeCliHelpReference,
} from "./cli_help.js";

function usage() {
  console.error(
    [
      "Usage: node generate_cli_test.js [--out PATH] [--bin PATH] [--source PATH] [--require-binary]",
      "",
      "Updates the SereneDB CLI help reference used by the JS driver test and docs site.",
    ].join("\n"),
  );
}

const args = process.argv.slice(2);
let out = process.env.SDB_CLI_HELP_OUTPUT || DEFAULT_CLI_HELP_REFERENCE_PATH;
let binary = process.env.SDB_CLI_HELP_BIN || "serened";
let repoRoot = process.env.SDB_CLI_HELP_SOURCE || path.resolve("..", "..", "..");
let requireBinary = process.env.SDB_CLI_HELP_REQUIRE_BINARY === "1";

for (let i = 0; i < args.length; i += 1) {
  const arg = args[i];
  if (arg === "--out") {
    out = args[++i];
  } else if (arg === "--bin") {
    binary = args[++i];
  } else if (arg === "--source") {
    repoRoot = args[++i];
  } else if (arg === "--require-binary") {
    requireBinary = true;
  } else if (arg === "-h" || arg === "--help") {
    usage();
    process.exit(0);
  } else {
    console.error(`Unknown argument: ${arg}`);
    usage();
    process.exit(2);
  }
}

const data = writeCliHelpReference(path.resolve(out), {
  binary,
  repoRoot: path.resolve(repoRoot),
  requireBinary,
});

const flagCount = data.flagGroups.reduce(
  (sum, group) => sum + group.flags.length,
  0,
);
console.log(
  `Generated ${path.relative(process.cwd(), out)} from ${data.generatedFrom}: ${data.commands.length} commands, ${flagCount} flags`,
);
