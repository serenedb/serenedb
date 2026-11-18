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

#include <absl/algorithm/container.h>
#include <absl/strings/str_format.h>
#include <simdjson.h>

#include <atomic>
#include <cstdio>
#include <duckdb/main/database.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/segment_list.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/directory_cleaner.hpp>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace bench {

static irs::IndexWriterOptions MakeWriterOptions(irs::ScorerPtr scorer_ptr,
                                                 size_t segment_pool_size,
                                                 size_t segment_mem_max,
                                                 uint32_t row_group_size,
                                                 uint32_t segment_docs_max) {
  auto* db = &::irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions writer_opts;
  writer_opts.reader_options.scorer = scorer_ptr;
  writer_opts.segment_pool_size = segment_pool_size;
  writer_opts.segment_memory_max = segment_mem_max;
  writer_opts.segment_docs_max = segment_docs_max;
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
  : _opts{opts}, _scorer{irs::BM25::Make(irs::BM25::Options{})}, _dir{path} {
  auto options =
    MakeWriterOptions(_scorer_ptr, opts.indexer_threads, config.segment_mem_max,
                      opts.row_group_size, opts.segment_docs_max);
  _reader_options = options.reader_options;
  _writer = irs::IndexWriter::Make(_dir, std::move(options));
}

irs::DirectoryReader IndexBuilder::GetReader() {
  std::vector<irs::SegmentReader> readers;
  absl::MutexLock lock{&_mutex};
  readers.reserve(_segments.size());
  for (const auto& meta : _segments) {
    readers.emplace_back(
      irs::SegmentReaderImpl::Open(_dir, meta, _reader_options));
  }
  return {_dir, _reader_options, std::move(readers), /*stats=*/true};
}

void IndexBuilder::Remove(const irs::DocRemoval& removal) {
  absl::MutexLock lock{&_mutex};
  for (const auto& group : removal.groups) {
    const auto name = irs::FileName(group.segment);
    const auto it = absl::c_find_if(
      _segments, [&](const auto& meta) { return meta.name == name; });
    if (it == _segments.end()) {
      continue;
    }
    irs::DocumentMaskBuilder docs{group.docs};
    if (it->docs_mask) {
      docs.Merge(*it->docs_mask);
    }
    it->docs_mask =
      std::make_shared<const irs::DocumentMask>(std::move(docs).Finish());
    it->live_docs_count = it->docs_count - irs::RemovalCount(*it);
  }
}

void IndexBuilder::Add(
  std::span<const irs::IndexWriter::FlushedSegment> flushed) {
  absl::MutexLock lock{&_mutex};
  for (const auto& segment : flushed) {
    SDB_ASSERT(segment.docs_mask.Empty());
    _segments.push_back(segment.meta);
  }
  Repin();
}

void IndexBuilder::Repin() {
  irs::FileRefs pins;
  auto& refs = _dir.attributes().refs();
  for (const auto& meta : _segments) {
    for (const auto& file : irs::SegmentFiles(meta.name, meta.kinds)) {
      pins.emplace_back(refs.add(file));
    }
  }
  _pins = std::move(pins);
}

void IndexBuilder::Compact(const irs::CompactionPolicy& policy) {
  {
    auto merged = irs::GetBlocking(
      _writer->MergeAsync(policy, GetReader(), nullptr, {}, nullptr));
    if (!merged.reader) {
      return;
    }
    absl::MutexLock lock{&_mutex};
    for (const auto* candidate : merged.claim.candidates) {
      const auto it = absl::c_find_if(_segments, [&](const auto& meta) {
        return meta.name == candidate->Meta().name;
      });
      if (it == _segments.end() ||
          it->docs_mask.get() != candidate->docs_mask()) {
        return;
      }
    }
    for (const auto* candidate : merged.claim.candidates) {
      std::erase_if(_segments, [&](const auto& meta) {
        return meta.name == candidate->Meta().name;
      });
    }
    _segments.push_back(merged.meta);
    Repin();
  }
  irs::DirectoryCleaner::clean(_dir);
}

void IndexBuilder::WriteSegmentList() {
  std::string text;
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& meta : _segments) {
      SDB_ASSERT(!meta.docs_mask);
      uint64_t id = 0;
      irs::ParseSegmentId(meta.name, id);
      irs::AppendSegmentList(text, id, meta.kinds, meta.docs_count);
    }
  }
  auto out = _dir.create(kSegmentListFile);
  out->WriteData(reinterpret_cast<const irs::byte_type*>(text.data()),
                 text.size());
  out.reset();
  const std::string_view name = kSegmentListFile;
  _dir.sync({&name, 1});
}

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
  std::vector<std::jthread> threads;

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

  threads.emplace_back([&batch_provider, &source,
                        batch_size = _opts.batch_size] {
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

  // compaction threads
  const irs::index_utils::CompactionTier compaction_options;
  auto policy = irs::index_utils::MakePolicy(compaction_options);

  for (size_t i = _opts.compaction_threads; i; --i) {
    threads.emplace_back([&] {
      while (!batch_provider.done.load()) {
        {
          absl::MutexLock lock{&compaction_mutex};
          if (compaction_cv.WaitWithTimeout(
                &compaction_mutex,
                absl::Milliseconds(_opts.compaction_interval_ms))) {
            continue;
          }
        }

        absl::PrintF("[COMPACT]");
        std::fflush(stdout);
        Compact(policy);
      }
    });
  }

  // indexer threads
  for (size_t i = _opts.indexer_threads; i; --i) {
    threads.emplace_back([&, factory] {
      SDB_ASSERT(factory, "BatchHandlerFactory must not be null");
      auto handler = factory();
      Batch batch;

      while (batch_provider.Take(batch)) {
        auto ctx = _writer->GetBatch();
        (*handler)(batch, ctx);
        Add(ctx.FlushSegments(true));
        ctx.Abort();
        if (_opts.compaction_threads) {
          absl::MutexLock lock{&compaction_mutex};
          compaction_cv.notify_all();
        }
        absl::PrintF(".");
        std::fflush(stdout);
      }
    });
  }

  threads.clear();

  if (_opts.compact_all) {
    absl::PrintF("Compacting all segments:\n");
    std::fflush(stdout);
    Compact(irs::index_utils::MakePolicy(irs::index_utils::CompactionCount()));
  }
  WriteSegmentList();
}

bool TextField::Write(irs::DataOutput& out) const {
  irs::WriteStr(out, text);
  return true;
}

}  // namespace bench
