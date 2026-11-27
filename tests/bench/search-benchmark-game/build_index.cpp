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

#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <absl/time/clock.h>
#include <unistd.h>

#include <cstdio>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/misc.hpp>
#include <iresearch/utils/remap_executable.hpp>
#include <thread>

#include "executor.h"
#include "index_builder.h"
#include "insert_field.hpp"

int main(int argc, const char* argv[]) {
  size_t threads = 1;
  if (argc > 1 && (!absl::SimpleAtoi(argv[1], &threads) || threads == 0)) {
    absl::FPrintF(stderr, "usage: %s [indexer threads] < corpus\n", argv[0]);
    return 1;
  }
  irs::RemapExecutable(SDB_REMAP_EXECUTABLE,
                       std::thread::hardware_concurrency());
  // DuckDBEngine owns the process-wide DuckDB the cs codec / writer use.
  // Bring it up before the first iresearch construction and tear it down
  // before main returns -- duckdb's BlockAllocator dtor reads a
  // thread_local cache libc destroys *before* static dtors, so any
  // DuckDB instance whose lifetime extends into the static-dtor phase
  // trips heap-use-after-free.
  irs::DuckDBEngine::Instance().Initialize();
  int exit_code = 0;
  try {
    const auto start = absl::Now();
    irs::Finally output_time = [start] noexcept {
      absl::PrintF("Total indexing time: %d us\n",
                   absl::ToInt64Microseconds(absl::Now() - start));
    };

    struct IndexText : bench::IBatchHandler {
      bench::Document doc;
      void operator()(const bench::Batch& batch,
                      irs::IndexWriter::Transaction& ctx) override {
        for (const auto& item : batch.Docs()) {
          doc.Fill(item);
          auto trx = ctx.Insert();
          tests::InsertField(trx, doc.fields[1]);
        }
      }
    };

    bench::BenchConfig config;
    bench::IndexBuilderOptions builder_options{
      .batch_size = 100000,
      .indexer_threads = threads,
      .compaction_interval_ms = 5000,
      .compaction_threads = 0,
      .compact_all = true,
      .row_group_size = 10'000'000,
    };

    bench::IndexBuilder builder{"idx", builder_options, config};
    bench::LineSource input{STDIN_FILENO};
    builder.IndexFrom(input, [] -> std::unique_ptr<bench::IBatchHandler> {
      return std::make_unique<IndexText>();
    });

    absl::PrintF("Number of documents: %d\n", builder.GetReader().docs_count());
  } catch (const std::exception& ex) {
    absl::FPrintF(stderr, "fatal: %s\n", ex.what());
    exit_code = 1;
  }
  // MUST run before main() returns -- see comment at top of main().
  irs::DuckDBEngine::Instance().Shutdown();
  return exit_code;
}
