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

#include "index_builder.h"

#include <atomic>
#include <duckdb/main/database.hpp>
#include <iostream>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <memory>

namespace bench {

duckdb::DatabaseInstance& CsDb() {
  static std::unique_ptr<duckdb::DuckDB> kDb = [] {
    duckdb::DBConfig cfg;
    cfg.options.access_mode = duckdb::AccessMode::AUTOMATIC;
    return std::make_unique<duckdb::DuckDB>(":memory:", &cfg);
  }();
  return *kDb->instance;
}

static irs::IndexWriterOptions MakeWriterOptions(irs::ScorerPtr scorer_ptr,
                                                 size_t segment_pool_size,
                                                 size_t segment_mem_max,
                                                 uint32_t row_group_size,
                                                 uint32_t norm_row_group_size) {
  irs::IndexWriterOptions writer_opts;
  writer_opts.reader_options.scorer = scorer_ptr;
  writer_opts.segment_pool_size = segment_pool_size;
  writer_opts.segment_memory_max = segment_mem_max;
  writer_opts.db = &CsDb();
  writer_opts.reader_options.db = &CsDb();
  writer_opts.column_options =
    [row_group_size](irs::field_id id) -> irs::ColumnOptions {
    return {.row_group_size = row_group_size};
  };
  writer_opts.norm_column_options =
    [norm_row_group_size, next = std::make_shared<std::atomic<irs::field_id>>(
                            0)](std::string_view) -> irs::NormColumnOptions {
    return {
      .id = next->fetch_add(1, std::memory_order_relaxed),
      .row_group_size = norm_row_group_size,
    };
  };
  return writer_opts;
}

IndexBuilder::IndexBuilder(std::string_view path,
                           const IndexBuilderOptions& opts,
                           const BenchConfig& config)
  : _opts{opts},
    _scorer{irs::scorers::Get(config.scorer,
                              irs::Type<irs::text_format::Json>::get(),
                              config.scorer_options)},
    _dir{path},
    _format{irs::formats::Get(config.format_name)},
    _writer{irs::IndexWriter::Make(
      _dir, _format, irs::kOmCreate,
      MakeWriterOptions(_scorer_ptr, opts.indexer_threads,
                        config.segment_mem_max, opts.row_group_size,
                        opts.norm_row_group_size))} {}

void IndexBuilder::IndexFromStream(std::istream& input,
                                   BatchHandlerFactory factory) {
  irs::async_utils::ThreadPool<> thread_pool{_opts.indexer_threads +
                                             _opts.compaction_threads + 1};

  struct {
    absl::CondVar cond;
    std::atomic<bool> done{false};
    bool eof{false};
    absl::Mutex mutex;
    std::vector<std::string> buf;

    bool Swap(std::vector<std::string>& buf) {
      absl::MutexLock lock{&mutex};
      for (;;) {
        this->buf.swap(buf);
        this->buf.resize(0);
        cond.notify_all();

        if (!buf.empty()) {
          return true;
        }

        if (eof) {
          done.store(true);
          return false;
        }

        if (!eof) {
          cond.Wait(&mutex);
        }
      }
    }
  } batch_provider;

  // stream reader thread
  thread_pool.run([&batch_provider, &input, batch_size = _opts.batch_size] {
    irs::SetThreadName(IR_NATIVE_STRING("reader"));

    absl::MutexLock lock{&batch_provider.mutex};

    for (;;) {
      batch_provider.buf.resize(batch_provider.buf.size() + 1);
      batch_provider.cond.notify_all();

      auto& line = batch_provider.buf.back();

      if (std::getline(input, line).eof()) {
        batch_provider.buf.pop_back();
        break;
      }

      if (batch_size && batch_provider.buf.size() >= batch_size) {
        batch_provider.cond.Wait(&batch_provider.mutex);
      }
    }

    batch_provider.eof = true;
  });

  absl::Mutex compaction_mutex;
  absl::CondVar compaction_cv;

  // commiter thread
  if (_opts.refresh_interval_ms) {
    thread_pool.run([&compaction_cv, &compaction_mutex, &batch_provider, this] {
      irs::SetThreadName(IR_NATIVE_STRING("committer"));

      while (!batch_provider.done.load()) {
        {
          std::cout << "[COMMIT]" << std::endl;
          _writer->RefreshCommit();
        }

        // notify compaction threads
        if (_opts.compaction_threads) {
          absl::MutexLock lock{&compaction_mutex};
          compaction_cv.notify_all();
        }

        std::this_thread::sleep_for(
          std::chrono::milliseconds(_opts.refresh_interval_ms));
      }
    });
  }

  // compaction threads
  const irs::index_utils::CompactionTier compaction_options;
  auto policy = irs::index_utils::MakePolicy(compaction_options);

  for (size_t i = _opts.compaction_threads; i; --i) {
    thread_pool.run([&] {
      irs::SetThreadName(IR_NATIVE_STRING("compactr"));

      while (!batch_provider.done.load()) {
        {
          absl::MutexLock lock{&compaction_mutex};
          if (compaction_cv.WaitWithTimeout(
                &compaction_mutex,
                absl::Milliseconds(_opts.compaction_interval_ms))) {
            continue;
          }
        }

        {
          std::cout << "[COMPACT]" << std::flush;
          _writer->Compact(policy);
        }

        irs::directory_utils::RemoveAllUnreferenced(_dir);
      }
    });
  }

  // indexer threads
  for (size_t i = _opts.indexer_threads; i; --i) {
    thread_pool.run([&, factory] {
      irs::SetThreadName(IR_NATIVE_STRING("indexer"));

      SDB_ASSERT(factory, "BatchHandlerFactory must not be null");
      auto handler = factory();
      std::vector<std::string> buf;

      while (batch_provider.Swap(buf)) {
        auto ctx = _writer->GetBatch();
        (*handler)(buf, ctx);
        std::cout << "." << std::flush;
      }
    });
  }

  thread_pool.stop();

  std::cout << "[COMMIT]" << std::endl;
  _writer->RefreshCommit();

  if (_opts.compact_all) {
    std::cout << "Compacting all segments:" << std::endl;
    CompactAll();
  } else if (_opts.compaction_threads) {
    irs::directory_utils::RemoveAllUnreferenced(_dir);
  }
}

void IndexBuilder::CompactAll() {
  _writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount()));
  _writer->RefreshCommit();
  irs::directory_utils::RemoveAllUnreferenced(_dir);
}

void Document::Fill(std::string_view line) {
  simdjson::padded_string padded_input{line};
  const auto error = parser.iterate(padded_input).get(json_doc);
  SDB_ASSERT(error == simdjson::SUCCESS, "Failed to parse JSON document", line);
  fields[0].text = json_doc["id"];
  fields[1].text = json_doc["text"];
}

bool TextField::Write(irs::DataOutput& out) const {
  irs::WriteStr(out, text);
  return true;
}

}  // namespace bench
