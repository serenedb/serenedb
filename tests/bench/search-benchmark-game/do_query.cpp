////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/strings/ascii.h>
#include <absl/strings/str_format.h>
#include <fast_float/fast_float.h>

#include <cstdio>
#include <iostream>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/utils/levenshtein_default_pdp.hpp>
#include <optional>
#include <string>
#include <system_error>

#include "basics/duckdb_engine.h"
#include "executor.h"

namespace {

// What a line asks of its query, spelled as `count`, `docs`, `scored` or
// `top_<N>`. A top-k reads `_count` as "do not prune, take the exact total",
// and anything that is not a bare count may end in `_hash` for a checksum
// over what it found and `_print` for all of it -- either, both, in either
// order.
enum class Kind : uint8_t {
  Count,
  Docs,
  Scored,
  TopK,
};

struct Command {
  Kind kind;
  bench::Report report;
  size_t k = 0;        // top-k only
  bool exact = false;  // top-k only: `_count`
};

constexpr std::string_view kUnsupported = "UNSUPPORTED";

bool StripSuffix(std::string_view& name, std::string_view suffix) {
  if (!name.ends_with(suffix)) {
    return false;
  }
  name.remove_suffix(suffix.size());
  return true;
}

std::optional<Command> ParseCommand(std::string_view name) {
  Command cmd{.kind = Kind::Count};

  for (;;) {
    if (!cmd.report.print && StripSuffix(name, "_print")) {
      cmd.report.print = true;
      continue;
    }
    if (!cmd.report.hash && StripSuffix(name, "_hash")) {
      cmd.report.hash = true;
      continue;
    }
    break;
  }

  if (name == "count") {
    // A count is a number and nothing else: there are no documents in hand to
    // checksum or to print.
    return cmd.report.hash || cmd.report.print ? std::nullopt
                                               : std::optional{cmd};
  }
  if (name == "docs") {
    cmd.kind = Kind::Docs;
    return cmd;
  }
  if (name == "scored") {
    cmd.kind = Kind::Scored;
    return cmd;
  }

  cmd.exact = StripSuffix(name, "_count");
  if (!name.starts_with("top_")) {
    return std::nullopt;
  }
  name.remove_prefix(4);

  const auto* const end = name.data() + name.size();
  const auto [stop, ec] = fast_float::from_chars(name.data(), end, cmd.k);
  if (ec != std::errc{} || stop != end || cmd.k == 0) {
    return std::nullopt;
  }
  cmd.kind = Kind::TopK;
  return cmd;
}

size_t ExecuteCommand(bench::Executor& executor, const Command& cmd,
                      std::string_view query) {
  switch (cmd.kind) {
    case Kind::Count:
      return executor.ExecuteCount(query);
    case Kind::Docs: {
      const auto result = executor.ExecuteEmitDocs(query, cmd.report);
      return cmd.report.hash ? result.hash : result.count;
    }
    case Kind::Scored: {
      const auto result = executor.ExecuteEmitScoredDocs(query, cmd.report);
      return cmd.report.hash ? result.hash : result.count;
    }
    case Kind::TopK: {
      const auto count = cmd.exact ? executor.ExecuteTopKWithCount(cmd.k, query)
                                   : executor.ExecuteTopK(cmd.k, query);
      if (cmd.report.print) {
        executor.PrintResults();
      }
      return cmd.report.hash ? executor.HashResults() : count;
    }
  }
  return 0;
}

}  // namespace

int main(int argc, const char* argv[]) {
  // DuckDBEngine owns the process-wide DuckDB the cs codec / reader use.
  // Bracket the executor lifetime so the DuckDB instance is destroyed
  // BEFORE static dtors fire (see build_index.cpp main() for the
  // BlockAllocator/thread_local UAF rationale).
  sdb::DuckDBEngine::Instance().Initialize();
  int exit_code = 0;
  try {
    irs::formats::Init();
    irs::InitOptimizeRules();
    irs::DefaultPDP(1, false);
    irs::DefaultPDP(1, true);
    irs::DefaultPDP(2, false);
    irs::DefaultPDP(2, true);

    bench::Executor executor{argv[1]};

    std::string data;
    while (std::getline(std::cin, data)) {
      size_t count = 0;
      const std::string_view line{data};
      const auto tab = line.find('\t');
      const auto cmd =
        tab == std::string_view::npos
          ? std::nullopt
          : ParseCommand(absl::AsciiStrToLower(line.substr(0, tab)));
      if (!cmd) {
        absl::FPrintF(stderr, "unknown command: %s\n", line);
      } else {
        try {
          count = ExecuteCommand(executor, *cmd, line.substr(tab + 1));
        } catch (const std::exception& ex) {
          absl::FPrintF(stderr, "unsupported: %s\n", ex.what());
        }
      }
      if (!count) {
        absl::PrintF("%s\n", kUnsupported);
      } else {
        absl::PrintF("%d\n", count);
      }
      // The driver writes one query and waits for its line, and nothing ties
      // this output to the input any more.
      std::fflush(stdout);
    }
  } catch (const std::exception& ex) {
    absl::FPrintF(stderr, "fatal: %s\n", ex.what());
    exit_code = 1;
  }
  sdb::DuckDBEngine::Instance().Shutdown();
  return exit_code;
}
