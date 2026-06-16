import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_REPO_ROOT = path.resolve(HERE, "../../..");
export const DEFAULT_CLI_HELP_REFERENCE_PATH = path.join(
  HERE,
  "fixtures",
  "serened_cli_help.json",
);

const CATEGORY_RULES = [
  ["server/rest_server/database_path_feature.cpp", "server"],
  ["server/rest_server/endpoint_feature.cpp", "server"],
  ["server/general_server/scheduler_feature.cpp", "server"],
  ["server/storage_engine/search_engine.cpp", "server"],
  ["server/general_server/general_server_feature.cpp", "http"],
  ["server/general_server/ssl_server_feature.cpp", "ssl"],
  ["libs/basics/log_flags.cpp", "log"],
];

const CATEGORY_LABELS = {
  server: "Server",
  http: "HTTP",
  ssl: "TLS / SSL",
  log: "Logging",
};

const CATEGORY_ORDER = ["server", "http", "ssl", "log"];

const SOURCE_FILES = CATEGORY_RULES.map(([file]) => file);

function readFile(root, relative) {
  return fs.readFileSync(path.join(root, relative), "utf8");
}

function findBalancedCall(text, marker, startIndex = 0) {
  const markerIndex = text.indexOf(marker, startIndex);
  if (markerIndex < 0) {
    return null;
  }
  const open = text.indexOf("(", markerIndex + marker.length);
  if (open < 0) {
    return null;
  }

  let depth = 0;
  let quote = null;
  let escaped = false;

  for (let i = open; i < text.length; i += 1) {
    const ch = text[i];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === quote) {
        quote = null;
      }
      continue;
    }

    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (ch === "(") {
      depth += 1;
    } else if (ch === ")") {
      depth -= 1;
      if (depth === 0) {
        return {
          body: text.slice(open + 1, i),
          end: i + 1,
        };
      }
    }
  }

  return null;
}

function splitTopLevelCommas(text) {
  const parts = [];
  let start = 0;
  let depth = 0;
  let angleDepth = 0;
  let quote = null;
  let escaped = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === quote) {
        quote = null;
      }
      continue;
    }

    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (ch === "(" || ch === "{" || ch === "[") {
      depth += 1;
      continue;
    }
    if (ch === ")" || ch === "}" || ch === "]") {
      depth -= 1;
      continue;
    }
    if (ch === "<") {
      angleDepth += 1;
      continue;
    }
    if (ch === ">") {
      angleDepth = Math.max(0, angleDepth - 1);
      continue;
    }
    if (ch === "," && depth === 0 && angleDepth === 0) {
      parts.push(text.slice(start, i).trim());
      start = i + 1;
    }
  }

  parts.push(text.slice(start).trim());
  return parts;
}

function decodeCppString(value) {
  return value
    .replace(/\\n/g, "\n")
    .replace(/\\t/g, "\t")
    .replace(/\\"/g, '"')
    .replace(/\\\\/g, "\\");
}

function cppStringLiterals(text) {
  const parts = [];
  const re = /"((?:\\.|[^"\\])*)"/g;
  let match;
  while ((match = re.exec(text))) {
    parts.push(decodeCppString(match[1]));
  }
  return parts;
}

function normalizeType(rawType) {
  return rawType
    .replace(/\s+/g, " ")
    .replace(/^std::/, "")
    .replace(/std::/g, "")
    .trim();
}

function normalizeDefault(rawDefault) {
  const value = rawDefault.trim();
  const strings = cppStringLiterals(value);
  if (strings.length > 0) {
    return strings.join("");
  }
  if (value === "{}") {
    return "[]";
  }
  return value;
}

function normalizeDescription(text) {
  return text.replace(/\s+/g, " ").trim();
}

function categoryForSource(relativePath) {
  for (const [suffix, category] of CATEGORY_RULES) {
    if (relativePath.endsWith(suffix)) {
      return category;
    }
  }
  return "other";
}

function parseAbslFlagsFromSource(repoRoot = DEFAULT_REPO_ROOT) {
  const flags = [];

  for (const relative of SOURCE_FILES) {
    const source = readFile(repoRoot, relative);
    let cursor = 0;
    while (true) {
      const call = findBalancedCall(source, "ABSL_FLAG", cursor);
      if (!call) {
        break;
      }
      cursor = call.end;
      const parts = splitTopLevelCommas(call.body);
      if (parts.length < 4) {
        continue;
      }

      const [rawType, rawName, rawDefault, ...rawDescription] = parts;
      const category = categoryForSource(relative);
      flags.push({
        name: rawName.trim(),
        flag: `--${rawName.trim()}`,
        type: normalizeType(rawType),
        defaultValue: normalizeDefault(rawDefault),
        description: normalizeDescription(
          cppStringLiterals(rawDescription.join(",")).join(" "),
        ),
        source: relative,
        category,
        categoryLabel: CATEGORY_LABELS[category] || category,
      });
    }
  }

  return flags.sort((a, b) => {
    const categoryDiff =
      CATEGORY_ORDER.indexOf(a.category) - CATEGORY_ORDER.indexOf(b.category);
    return categoryDiff || a.name.localeCompare(b.name);
  });
}

function parseListSection(lines, heading) {
  const start = lines.findIndex((line) => line.trim().startsWith(heading));
  if (start < 0) {
    return [];
  }

  const items = [];
  let current = null;
  for (const line of lines.slice(start + 1)) {
    if (!line.trim()) {
      break;
    }
    const match = line.match(/^\s*(--\S+)\s+(.*)$/);
    if (match) {
      current = { flag: match[1], description: match[2].trim() };
      items.push(current);
    } else if (current) {
      current.description = `${current.description} ${line.trim()}`.trim();
    }
  }
  return items;
}

function commandDescription(command) {
  if (command.includes(" shell ")) {
    return "Open the embedded DuckDB shell without starting the SereneDB server.";
  }
  if (command.includes(" psql ")) {
    return "Open the psql-compatible shell subcommand without starting the SereneDB server.";
  }
  return "Start the SereneDB pg-wire server, using the optional data directory and flags.";
}

function parseUsageSections(usageMessage) {
  const lines = usageMessage.split(/\r?\n/);
  const title = lines.find((line) => line.trim())?.trim() || "serened";
  const usageStart = lines.findIndex((line) => line.trim() === "Usage:");
  const usage = [];
  if (usageStart >= 0) {
    for (const line of lines.slice(usageStart + 1)) {
      if (!line.trim()) {
        break;
      }
      usage.push(line.trim());
    }
  }

  return {
    title,
    usage,
    commands: usage.map((command) => ({
      command,
      description: commandDescription(command),
    })),
    flagSources: parseListSection(lines, "Flag sources"),
    helpVariants: parseListSection(lines, "Help variants"),
  };
}

function readUsageMessageFromSource(repoRoot = DEFAULT_REPO_ROOT) {
  const source = readFile(repoRoot, "libs/app/app_server.cpp");
  const call = findBalancedCall(source, "absl::SetProgramUsageMessage");
  if (!call) {
    throw new Error("Could not find absl::SetProgramUsageMessage");
  }
  return cppStringLiterals(call.body).join("");
}

function parseFlagsFromHelpOutput(helpText) {
  const flags = [];
  let currentGroup = "other";
  let current = null;

  for (const line of helpText.split(/\r?\n/)) {
    const groupMatch = line.match(/^Flags from (.*):\s*$/);
    if (groupMatch) {
      currentGroup = groupMatch[1].trim();
      current = null;
      continue;
    }

    const flagMatch = line.match(/^\s+--?([A-Za-z0-9_]+)(?:\s+\((.*))?$/);
    if (flagMatch) {
      current = {
        name: flagMatch[1],
        flag: `--${flagMatch[1]}`,
        type: "",
        defaultValue: "",
        description: (flagMatch[2] || "").replace(/\)?;?$/, "").trim(),
        source: currentGroup,
        category: currentGroup,
        categoryLabel: CATEGORY_LABELS[currentGroup] || currentGroup,
      };
      flags.push(current);
      continue;
    }

    if (!current) {
      continue;
    }

    const defaultMatch = line.match(/^\s+default:\s*(.*)$/);
    if (defaultMatch) {
      current.defaultValue = defaultMatch[1].replace(/;$/, "").trim();
    } else if (line.trim()) {
      current.description = `${current.description} ${line.trim()}`
        .replace(/\s+/g, " ")
        .trim();
    }
  }

  return flags;
}

export function parseSerenedHelpOutput(helpText) {
  const sections = parseUsageSections(helpText);
  return {
    generatedFrom: "serened --help",
    ...sections,
    flagGroups: groupFlags(parseFlagsFromHelpOutput(helpText)),
    rawHelp: helpText,
  };
}

function runHelp(binary) {
  const result = spawnSync(binary, ["--help"], {
    encoding: "utf8",
    maxBuffer: 1024 * 1024 * 8,
  });

  if (result.error) {
    if (result.error.code === "ENOENT") {
      return null;
    }
    throw result.error;
  }

  const output = [result.stdout, result.stderr].filter(Boolean).join("\n");
  if (!output.includes("Usage:")) {
    throw new Error(`Help output from ${binary} did not contain a Usage section`);
  }
  return output;
}

function groupFlags(flags) {
  const groups = new Map();
  for (const flag of flags) {
    const key = flag.category || "other";
    if (!groups.has(key)) {
      groups.set(key, {
        id: key,
        label: flag.categoryLabel || CATEGORY_LABELS[key] || key,
        flags: [],
      });
    }
    groups.get(key).flags.push(flag);
  }

  return [...groups.values()].sort((a, b) => {
    const ai = CATEGORY_ORDER.indexOf(a.id);
    const bi = CATEGORY_ORDER.indexOf(b.id);
    if (ai !== -1 || bi !== -1) {
      return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
    }
    return a.label.localeCompare(b.label);
  });
}

export function buildCliHelpReference({
  repoRoot = process.env.SDB_CLI_HELP_SOURCE || DEFAULT_REPO_ROOT,
  binary = process.env.SDB_CLI_HELP_BIN || "serened",
  requireBinary = process.env.SDB_CLI_HELP_REQUIRE_BINARY === "1",
} = {}) {
  const helpText = runHelp(binary);
  if (helpText) {
    const parsed = parseSerenedHelpOutput(helpText);
    if (parsed.flagGroups.some((group) => group.flags.length > 0)) {
      return parsed;
    }
  } else if (requireBinary) {
    throw new Error(
      `Could not run ${binary}. Set SDB_CLI_HELP_BIN to a serened binary.`,
    );
  }

  const usageMessage = readUsageMessageFromSource(repoRoot);
  return {
    generatedFrom: "serened source fallback",
    ...parseUsageSections(usageMessage),
    flagGroups: groupFlags(parseAbslFlagsFromSource(repoRoot)),
    rawHelp: usageMessage,
  };
}

export function writeCliHelpReference(outPath, options = {}) {
  const data = buildCliHelpReference(options);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(data, null, 2)}\n`);
  return data;
}

export function readCliHelpReference(
  referencePath = DEFAULT_CLI_HELP_REFERENCE_PATH,
) {
  return JSON.parse(fs.readFileSync(referencePath, "utf8"));
}

function stringifyReference(data) {
  return `${JSON.stringify(data, null, 2)}\n`;
}

function diffLines(expectedLines, actualLines) {
  const rows = expectedLines.length + 1;
  const cols = actualLines.length + 1;
  const lengths = Array.from({ length: rows }, () =>
    new Uint32Array(cols),
  );

  for (let i = expectedLines.length - 1; i >= 0; i -= 1) {
    for (let j = actualLines.length - 1; j >= 0; j -= 1) {
      lengths[i][j] =
        expectedLines[i] === actualLines[j]
          ? lengths[i + 1][j + 1] + 1
          : Math.max(lengths[i + 1][j], lengths[i][j + 1]);
    }
  }

  const ops = [];
  let i = 0;
  let j = 0;
  while (i < expectedLines.length && j < actualLines.length) {
    if (expectedLines[i] === actualLines[j]) {
      ops.push({ type: "equal", line: expectedLines[i] });
      i += 1;
      j += 1;
    } else if (lengths[i + 1][j] >= lengths[i][j + 1]) {
      ops.push({ type: "delete", line: expectedLines[i] });
      i += 1;
    } else {
      ops.push({ type: "insert", line: actualLines[j] });
      j += 1;
    }
  }
  while (i < expectedLines.length) {
    ops.push({ type: "delete", line: expectedLines[i] });
    i += 1;
  }
  while (j < actualLines.length) {
    ops.push({ type: "insert", line: actualLines[j] });
    j += 1;
  }

  return ops;
}

export function formatCliHelpReferenceDiff(expected, actual, maxLines = 260) {
  const expectedLines = stringifyReference(expected).split("\n");
  const actualLines = stringifyReference(actual).split("\n");
  const ops = diffLines(expectedLines, actualLines);
  const output = [
    "--- reference: fixtures/serened_cli_help.json",
    "+++ current: serened --help",
  ];
  let equalRun = [];

  function flushEquals() {
    if (equalRun.length === 0) {
      return;
    }
    if (equalRun.length <= 6) {
      output.push(...equalRun.map((line) => ` ${line}`));
    } else {
      output.push(...equalRun.slice(0, 3).map((line) => ` ${line}`));
      output.push(`... ${equalRun.length - 6} unchanged lines ...`);
      output.push(...equalRun.slice(-3).map((line) => ` ${line}`));
    }
    equalRun = [];
  }

  for (const op of ops) {
    if (op.type === "equal") {
      equalRun.push(op.line);
      continue;
    }

    flushEquals();
    output.push(`${op.type === "delete" ? "-" : "+"}${op.line}`);

    if (output.length >= maxLines) {
      output.push(`... diff truncated after ${maxLines} lines ...`);
      break;
    }
  }

  return output.join("\n");
}

export function compareCliHelpReference({
  referencePath = DEFAULT_CLI_HELP_REFERENCE_PATH,
  actual = buildCliHelpReference(),
} = {}) {
  const expected = readCliHelpReference(referencePath);
  const expectedText = stringifyReference(expected);
  const actualText = stringifyReference(actual);

  if (expectedText === actualText) {
    return { matches: true, expected, actual, diff: "" };
  }

  return {
    matches: false,
    expected,
    actual,
    diff: formatCliHelpReferenceDiff(expected, actual),
  };
}
