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

#include <absl/strings/str_format.h>

#include <cstdio>
#include <iostream>  // std::cin
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/timer_utils.hpp>

#include "basics/duckdb_engine.h"
#include "executor.h"
#include "index_builder.h"
#include "insert_field.hpp"

int main(int argc, const char* argv[]) {
  // DuckDBEngine owns the process-wide DuckDB the cs codec / writer use.
  // Bring it up before the first iresearch construction and tear it down
  // before main returns -- duckdb's BlockAllocator dtor reads a
  // thread_local cache libc destroys *before* static dtors, so any
  // DuckDB instance whose lifetime extends into the static-dtor phase
  // trips heap-use-after-free.
  sdb::DuckDBEngine::Instance().Initialize();
  int exit_code = 0;
  try {
    irs::timer_utils::InitStats(true);
    irs::Finally output_stats = [] noexcept {
      std::vector<std::tuple<std::string, size_t, size_t>> output;
      irs::timer_utils::Visit(
        [&](const std::string& key, size_t count, size_t time_us) -> bool {
          if (count == 0) {
            return true;
          }
          output.emplace_back(key, count, time_us);
          return true;
        });
      std::sort(output.begin(), output.end());
      for (auto& [key, count, time_us] : output) {
        absl::PrintF("%s calls:%d, time: %d us, avg call: %g us\n", key, count,
                     time_us,
                     static_cast<double>(time_us) / static_cast<double>(count));
      }
    };

    SCOPED_TIMER("Total indexing time");

    irs::formats::Init();

    struct IndexAllFields : bench::IBatchHandler {
      bench::Document doc;
      void operator()(std::vector<std::string>& buf,
                      irs::IndexWriter::Transaction& ctx) override {
        for (auto& line : buf) {
          doc.Fill(line);
          auto trx = ctx.Insert();
          tests::InsertFields(trx, doc.fields.begin(), doc.fields.end());
        }
      }
    };

    bench::BenchConfig config;
    bench::IndexBuilderOptions builder_options{
      .batch_size = 100000,
      .indexer_threads = 1,
      .refresh_interval_ms = 0,
      .compaction_interval_ms = 5000,
      .compaction_threads = 0,
      .compact_all = true,
      .row_group_size = 10'000'000,
    };

    bench::IndexBuilder builder{"idx", builder_options, config};
    builder.IndexFromStream(std::cin,
                            [] -> std::unique_ptr<bench::IBatchHandler> {
                              return std::make_unique<IndexAllFields>();
                            });

    absl::PrintF("Number of documents: %d\n", builder.GetReader().docs_count());
  } catch (const std::exception& ex) {
    absl::FPrintF(stderr, "fatal: %s\n", ex.what());
    exit_code = 1;
  }
  // MUST run before main() returns -- see comment at top of main().
  sdb::DuckDBEngine::Instance().Shutdown();
  return exit_code;
}
