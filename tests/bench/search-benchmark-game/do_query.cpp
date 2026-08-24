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

#include <cstdio>
#include <iostream>  // std::cin
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/utils/levenshtein_default_pdp.hpp>
#include <string>

#include "basics/duckdb_engine.h"
#include "executor.h"

namespace {

size_t ExecuteCommand(bench::Executor& executor, const bench::Command& cmd,
                      std::string_view query) {
  switch (cmd.kind) {
    case bench::Kind::Unsupported:
      break;
    case bench::Kind::Count:
      return executor.ExecuteCount(query);
    case bench::Kind::Docs: {
      const auto result = executor.ExecuteEmitDocs(query, cmd.report);
      return cmd.report.hash ? result.hash : result.count;
    }
    case bench::Kind::Scored: {
      const auto result = executor.ExecuteEmitScoredDocs(query, cmd.report);
      return cmd.report.hash ? result.hash : result.count;
    }
    case bench::Kind::TopK: {
      const auto count = cmd.prune
                           ? executor.ExecuteTopK(cmd.k, query)
                           : executor.ExecuteTopKWithCount(cmd.k, query);
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
          ? bench::Command{}
          : bench::ParseCommand(absl::AsciiStrToLower(line.substr(0, tab)));
      if (cmd.kind == bench::Kind::Unsupported) {
        absl::FPrintF(stderr, "unknown command: %s\n", line);
      } else {
        try {
          count = ExecuteCommand(executor, cmd, line.substr(tab + 1));
        } catch (const std::exception& ex) {
          absl::FPrintF(stderr, "unsupported: %s\n", ex.what());
        }
      }
      if (!count) {
        absl::PrintF("UNSUPPORTED\n");
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
