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

#include <absl/strings/str_format.h>
#include <simdjson.h>

#include <atomic>
#include <cstdio>
#include <duckdb/main/database.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <memory>
#include <optional>

namespace bench {

static irs::IndexWriterOptions MakeWriterOptions(irs::ScorerPtr scorer_ptr,
                                                 size_t segment_pool_size,
                                                 size_t segment_mem_max,
                                                 uint32_t row_group_size) {
  auto* db = &::irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions writer_opts;
  writer_opts.reader_options.scorer = scorer_ptr;
  writer_opts.segment_pool_size = segment_pool_size;
  writer_opts.segment_memory_max = segment_mem_max;
  writer_opts.db = db;
  writer_opts.reader_options.db = db;
  writer_opts.row_group_size = row_group_size;
  writer_opts.norm_column_id =
    [next = std::make_shared<std::atomic<irs::field_id>>(0)](
      irs::field_id) -> irs::field_id {
    return next->fetch_add(1, std::memory_order_relaxed);
  };
  return writer_opts;
}

IndexBuilder::IndexBuilder(std::string_view path,
                           const IndexBuilderOptions& opts,
                           const BenchConfig& config)
  : _opts{opts},
    _scorer{irs::BM25::Make(irs::BM25::Options{})},
    _dir{path},
    _writer{irs::IndexWriter::Make(
      _dir, irs::kOmCreate,
      MakeWriterOptions(_scorer_ptr, opts.indexer_threads,
                        config.segment_mem_max, opts.row_group_size))} {}

void Batch::Add(std::string_view id, std::string_view text) {
  _spans.push_back(
    {.offset = _arena.size(), .id = id.size(), .text = text.size()});
  _arena.append(id);
  _arena.append(text);
}

void Batch::Seal() {
  _docs.clear();
  _docs.reserve(_spans.size());
  const auto* base = _arena.data();
  for (const auto& span : _spans) {
    const auto* id = base + span.offset;
    _docs.push_back({.id = {id, span.id}, .text = {id + span.id, span.text}});
  }
}

void IndexBuilder::IndexFrom(LineSource& source, BatchHandlerFactory factory) {
  irs::async_utils::ThreadPool<> thread_pool{_opts.indexer_threads +
                                             _opts.compaction_threads + 1};

  struct {
    absl::CondVar cond;
    std::atomic<bool> done{false};
    bool eof{false};
    absl::Mutex mutex;
    std::optional<Batch> ready;

    void Put(Batch&& batch) {
      absl::MutexLock lock{&mutex};
      while (ready) {
        cond.Wait(&mutex);
      }
      ready = std::move(batch);
      cond.notify_all();
    }

    void Close() {
      absl::MutexLock lock{&mutex};
      eof = true;
      cond.notify_all();
    }

    bool Take(Batch& batch) {
      {
        absl::MutexLock lock{&mutex};
        while (!ready && !eof) {
          cond.Wait(&mutex);
        }
        if (!ready) {
          done.store(true);
          return false;
        }
        batch = std::move(*ready);
        ready.reset();
        cond.notify_all();
      }
      batch.Seal();
      return true;
    }
  } batch_provider;

  thread_pool.run([&batch_provider, &source, batch_size = _opts.batch_size] {
    simdjson::ondemand::parser parser;
    std::string padded;
    Batch batch;
    std::string_view line;
    while (source.Next(line)) {
      if (line.empty()) {
        continue;
      }
      const auto* data = line.data();
      const auto capacity = line.size() + simdjson::SIMDJSON_PADDING;
      if (static_cast<size_t>(source.End() - data) < capacity) {
        padded.assign(line);
        padded.resize(capacity);
        data = padded.data();
      }
      simdjson::ondemand::document json;
      const auto error = parser.iterate(data, line.size(), capacity).get(json);
      SDB_ASSERT(error == simdjson::SUCCESS, "Failed to parse JSON document",
                 line);
      const std::string_view id = json["id"];
      const std::string_view text = json["text"];
      batch.Add(id, text);
      if (batch_size != 0 && batch.Size() >= batch_size) {
        batch_provider.Put(std::move(batch));
        batch = {};
      }
    }
    if (batch.Size() != 0) {
      batch_provider.Put(std::move(batch));
    }
    batch_provider.Close();
  });

  absl::Mutex compaction_mutex;
  absl::CondVar compaction_cv;

  // commiter thread
  if (_opts.refresh_interval_ms) {
    thread_pool.run([&compaction_cv, &compaction_mutex, &batch_provider, this] {
      while (!batch_provider.done.load()) {
        {
          absl::PrintF("[COMMIT]\n");
          std::fflush(stdout);
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
          absl::PrintF("[COMPACT]");
          std::fflush(stdout);
          _writer->Compact(policy);
        }

        irs::directory_utils::RemoveAllUnreferenced(_dir);
      }
    });
  }

  // indexer threads
  for (size_t i = _opts.indexer_threads; i; --i) {
    thread_pool.run([&, factory] {
      SDB_ASSERT(factory, "BatchHandlerFactory must not be null");
      auto handler = factory();
      Batch batch;

      while (batch_provider.Take(batch)) {
        auto ctx = _writer->GetBatch();
        (*handler)(batch, ctx);
        ctx.Commit();
        absl::PrintF(".");
        std::fflush(stdout);
      }
    });
  }

  thread_pool.stop();

  absl::PrintF("[COMMIT]\n");
  std::fflush(stdout);
  _writer->RefreshCommit();

  if (_opts.compact_all) {
    absl::PrintF("Compacting all segments:\n");
    std::fflush(stdout);
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

bool TextField::Write(irs::DataOutput& out) const {
  irs::WriteStr(out, text);
  return true;
}

}  // namespace bench
