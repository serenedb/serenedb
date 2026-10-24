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
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/levenshtein_default_pdp.hpp>
#include <iresearch/utils/remap_executable.hpp>
#include <string>
#include <vector>

#include "executor.h"
#include "line_source.h"

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
    case bench::Kind::Hits: {
      const auto result = executor.ExecuteEmitHits(query, cmd.report);
      return cmd.report.hash ? result.hash : result.count;
    }
    case bench::Kind::Top: {
      const auto count = cmd.prune
                           ? executor.ExecuteTopK(cmd.k, query)
                           : executor.ExecuteTopKWithCount(cmd.k, query);
      if (cmd.report.print) {
        executor.PrintResults();
      }
      if (!cmd.report.hash) {
        return count;
      }
      return cmd.prune ? executor.HashResults()
                       : executor.HashResultsWithCount(count);
    }
  }
  return 0;
}

int64_t MedianRunNs(bench::Executor& executor, const bench::Command& cmd,
                    std::string_view query, size_t repeat,
                    std::vector<int64_t>& samples) {
  samples.clear();
  for (size_t i = 0; i != repeat; ++i) {
    const auto start = std::chrono::steady_clock::now();
    ExecuteCommand(executor, cmd, query);
    samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count());
  }
  const auto middle = samples.begin() + samples.size() / 2;
  std::nth_element(samples.begin(), middle, samples.end());
  return *middle;
}

// The index at `path`, or a copy of it in memory: in cache blocks, or with
// every file compacted into one span.
irs::Directory OpenIndex(std::string_view path, std::string_view memory) {
  auto& db = irs::DuckDBEngine::Instance().instance();
  irs::Directory local{db, path};
  if (memory.empty()) {
    return local;
  }
  irs::Directory copy{db, irs::MemoryIndexRoot()};
  std::vector<std::string> names;
  local.Visit([&](std::string_view name) { names.emplace_back(name); });
  std::vector<irs::byte_type> buffer(1 << 20);
  for (const auto& name : names) {
    auto in = local.Open(name, duckdb::MappedFileAccess::SEQUENTIAL);
    auto out = copy.Create(name);
    for (auto left = in->Length(); left != 0;) {
      const auto chunk = std::min<uint64_t>(left, buffer.size());
      in->ReadData(buffer.data(), chunk);
      out->WriteData(buffer.data(), chunk);
      left -= chunk;
    }
    out->Close();
    if (memory == "compacted") {
      copy.Compact(name);
    }
  }
  return copy;
}

}  // namespace

int main(int argc, const char* argv[]) {
  size_t repeat = 0;
  std::string_view memory;
  bool usage = argc < 2;
  for (int i = 2; i < argc && !usage; i += 2) {
    const std::string_view flag{argv[i]};
    if (i + 1 == argc) {
      usage = true;
    } else if (flag == "--repeat") {
      usage = !absl::SimpleAtoi(argv[i + 1], &repeat) || repeat == 0;
    } else if (flag == "--memory") {
      memory = argv[i + 1];
      usage = memory != "blocks" && memory != "compacted";
    } else {
      usage = true;
    }
  }
  if (usage) {
    absl::FPrintF(stderr,
                  "usage: %s <index dir> [--memory blocks|compacted] "
                  "[--repeat N] < queries\n",
                  argv[0]);
    return 1;
  }
  irs::RemapExecutable(true);
  // DuckDBEngine owns the process-wide DuckDB the cs codec / reader use.
  // Bracket the executor lifetime so the DuckDB instance is destroyed
  // BEFORE static dtors fire (see build_index.cpp main() for the
  // BlockAllocator/thread_local UAF rationale).
  irs::DuckDBEngine::Instance().Initialize();
  int exit_code = 0;
  try {
    irs::InitOptimizeRules();
    irs::DefaultPDP(1, false);
    irs::DefaultPDP(1, true);
    irs::DefaultPDP(2, false);
    irs::DefaultPDP(2, true);

    bench::Executor executor{OpenIndex(argv[1], memory)};

    bench::LineSource input{STDIN_FILENO};
    std::string_view line;
    std::vector<int64_t> samples;
    while (input.Next(line)) {
      size_t count = 0;
      int64_t median_ns = 0;
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
          if (count && repeat) {
            median_ns =
              MedianRunNs(executor, cmd, line.substr(tab + 1), repeat, samples);
          }
        } catch (const std::exception& ex) {
          absl::FPrintF(stderr, "unsupported: %s\n", ex.what());
          count = 0;
        }
      }
      if (!count) {
        absl::PrintF("UNSUPPORTED\n");
      } else if (repeat) {
        absl::PrintF("%d\t%d\n", count, median_ns);
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
  irs::DuckDBEngine::Instance().Shutdown();
  return exit_code;
}
