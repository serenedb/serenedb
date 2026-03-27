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

#include "index_builder.hpp"

#include <iostream>
#include <iresearch/utils/index_utils.hpp>

namespace bench {

static irs::IndexWriterOptions MakeWriterOptions(irs::Scorer** scorer_ptr,
                                                 size_t segment_pool_size,
                                                 size_t segment_mem_max) {
  irs::IndexWriterOptions writer_opts;
  writer_opts.reader_options.scorers = {scorer_ptr, 1};
  writer_opts.segment_pool_size = segment_pool_size;
  writer_opts.segment_memory_max = segment_mem_max;
  writer_opts.features = [](irs::IndexFeatures id) {
    const irs::ColumnInfo info{
      irs::Type<irs::compression::None>::get(), {}, false};
    if (irs::IndexFeatures::Norm == id) {
      return std::make_pair(info, &irs::Norm::MakeWriter);
    }
    return std::make_pair(info, irs::FeatureWriterFactory{});
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
      MakeWriterOptions(&_scorer_ptr, opts.indexer_threads,
                        config.segment_mem_max))} {}

void IndexBuilder::IndexFromStream(std::istream& input,
                                   BatchHandlerFactory factory) {
  irs::async_utils::ThreadPool<> thread_pool{_opts.indexer_threads +
                                             _opts.consolidation_threads + 1};

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

  absl::Mutex consolidation_mutex;
  absl::CondVar consolidation_cv;

  // commiter thread
  if (_opts.commit_interval_ms) {
    thread_pool.run(
      [&consolidation_cv, &consolidation_mutex, &batch_provider, this] {
        irs::SetThreadName(IR_NATIVE_STRING("committer"));

        while (!batch_provider.done.load()) {
          {
            std::cout << "[COMMIT]" << std::endl;
            _writer->Commit();
          }

          // notify consolidation threads
          if (_opts.consolidation_threads) {
            absl::MutexLock lock{&consolidation_mutex};
            consolidation_cv.notify_all();
          }

          std::this_thread::sleep_for(
            std::chrono::milliseconds(_opts.commit_interval_ms));
        }
      });
  }

  // consolidation threads
  const irs::index_utils::ConsolidateTier consolidation_options;
  auto policy = irs::index_utils::MakePolicy(consolidation_options);

  for (size_t i = _opts.consolidation_threads; i; --i) {
    thread_pool.run([&] {
      irs::SetThreadName(IR_NATIVE_STRING("consolidater"));

      while (!batch_provider.done.load()) {
        {
          absl::MutexLock lock{&consolidation_mutex};
          if (consolidation_cv.WaitWithTimeout(
                &consolidation_mutex,
                absl::Milliseconds(_opts.consolidation_interval_ms))) {
            continue;
          }
        }

        {
          std::cout << "[CONSOLIDATE]" << std::flush;
          _writer->Consolidate(policy);
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
  _writer->Commit();

  if (_opts.consolidate_all) {
    std::cout << "Consolidating all segments:" << std::endl;
    ConsolidateAll();
  } else if (_opts.consolidation_threads) {
    irs::directory_utils::RemoveAllUnreferenced(_dir);
  }
}

void IndexBuilder::ConsolidateAll() {
  _writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount()));
  _writer->Commit();
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
