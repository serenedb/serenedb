////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "catalog/entry/tokenizer.h"
#include "connector/column_id.h"
#include "connector/search_sink_writer.hpp"
#include "engine_context.h"

namespace sdb::test {

inline constexpr connector::ColumnId kValueColumn{1};

inline duckdb::DatabaseInstance& EngineDb() {
  return irs::DuckDBEngine::Instance().instance();
}

inline catalog::ColumnTokenizer KeywordTokenizer(irs::field_id) {
  static auto gTokenizer = std::make_shared<catalog::Tokenizer>(
    search::Features{},
    irs::analysis::TokenizerConfig{.config = irs::KeywordTokenizer::Options{}});
  return {.analyzer = gTokenizer->Acquire(EngineContext()),
          .features = irs::IndexFeatures::None};
}

inline void WriteValues(irs::IndexWriter::Transaction& trx,
                        std::span<const int64_t> values,
                        const connector::PkChunk& pk = {},
                        connector::PkPolicy policy = {
                          .index_term = false,
                          .column = catalog::PkColumnKind::None}) {
  connector::DuckDBSearchSinkInsertWriter sink{
    trx, KeywordTokenizer, std::array<connector::ColumnId, 1>{kValueColumn},
    connector::NoEntryInfoProvider(), policy};
  duckdb::Vector column{duckdb::LogicalType::BIGINT, values.size()};
  std::ranges::copy(values,
                    duckdb::FlatVector::GetDataMutable<int64_t>(column));
  sink.Init(values.size(), pk);
  sink.SwitchColumn(
    connector::ColumnDescriptor{kValueColumn, duckdb::LogicalType::BIGINT},
    column, values.size());
  sink.Finish();
}

inline uint64_t IdOf(const irs::SubReader& segment) {
  uint64_t id = 0;
  EXPECT_TRUE(irs::ParseSegmentId(segment.Meta().name, id));
  return id;
}

inline uint32_t IndexOf(const irs::DirectoryReader& snapshot, uint64_t id) {
  for (uint32_t index = 0; index != snapshot.size(); ++index) {
    if (IdOf(snapshot[index]) == id) {
      return index;
    }
  }
  ADD_FAILURE() << "no segment " << id;
  return 0;
}

inline std::vector<std::pair<uint64_t, irs::doc_id_t>> RemovedDocs(
  const irs::DocRemoval& removal) {
  std::vector<std::pair<uint64_t, irs::doc_id_t>> docs;
  for (const auto& group : removal.groups) {
    group.docs.ForEachRun(nullptr,
                          [&](irs::doc_id_t first, irs::doc_id_t last) {
                            for (auto doc = first; doc < last; ++doc) {
                              docs.emplace_back(group.segment, doc);
                            }
                            return true;
                          });
  }
  return docs;
}

inline const irs::DirectoryReader& OneRowSegments() {
  static irs::MemoryDirectory dir;
  static const irs::DirectoryReader reader = [] {
    irs::IndexWriterOptions options;
    options.db = &EngineDb();
    options.reader_options.db = &EngineDb();
    auto writer =
      irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(options));
    for (int64_t value = 0; value != 8; ++value) {
      auto trx = writer->GetBatch(/*exclusive_segment=*/true);
      WriteValues(trx, std::array{value});
      EXPECT_TRUE(trx.Commit());
      writer->RefreshCommit();
    }
    return writer->GetSnapshot();
  }();
  return reader;
}

}  // namespace sdb::test
