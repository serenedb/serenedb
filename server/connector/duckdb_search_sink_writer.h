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

#include <duckdb.hpp>
#include <iresearch/index/index_writer.hpp>

#include "catalog/inverted_index.h"
#include "catalog/search_analyzer_impl.h"
#include "connector/duckdb_sink_writer_base.h"
#include "connector/search_sink_writer.hpp"  // reuse TokenizerProvider, Field, etc.
#include "search/inverted_index_shard.h"

namespace sdb::connector {

class DuckDBSearchSinkInsertWriter final : public DuckDBSinkIndexWriter,
                                           public SearchSinkInsertBaseImpl {
 public:
  DuckDBSearchSinkInsertWriter(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    std::span<const catalog::Column::Id> columns,
    JsonPathsProvider&& json_paths_provider = NoJsonPaths(),
    StoreValuesProvider&& store_values_provider = NoStoreValues(),
    IsTextIndexedProvider&& is_text_indexed_provider = AllTextIndexed(),
    HNSWInfoProvider&& hnsw_info_provider = NoHNSW())
    : SearchSinkInsertBaseImpl{trx,
                               std::move(tokenizer_provider),
                               std::move(json_paths_provider),
                               std::move(store_values_provider),
                               std::move(is_text_indexed_provider),
                               std::move(hnsw_info_provider),
                               columns} {}

  void Init(duckdb::idx_t batch_size, const duckdb::DataChunk&) final {
    InitImpl(batch_size);
  }

  bool SwitchColumn(const ColumnDescriptor& col, const duckdb::Vector& vec,
                    duckdb::idx_t count) final;

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    WriteImpl(cell_slices, full_key);
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }
};

// DuckDB-native search index delete writer.
class DuckDBSearchSinkDeleteWriter final : public DuckDBSinkIndexWriter,
                                           public SearchSinkDeleteBaseImpl {
 public:
  DuckDBSearchSinkDeleteWriter(irs::IndexWriter::Transaction& trx)
    : SearchSinkDeleteBaseImpl{trx} {}

  void Init(duckdb::idx_t batch_size, const duckdb::DataChunk&) final {
    InitImpl(batch_size);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteRowImpl(encoded_pk);
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }
};

// DuckDB-native search index update writer (insert + delete).
class DuckDBSearchSinkUpdateWriter final : public DuckDBSinkIndexWriter,
                                           public SearchSinkInsertBaseImpl,
                                           public SearchSinkDeleteBaseImpl {
 public:
  DuckDBSearchSinkUpdateWriter(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    std::span<const catalog::Column::Id> columns,
    JsonPathsProvider&& json_paths_provider = NoJsonPaths(),
    StoreValuesProvider&& store_values_provider = NoStoreValues(),
    IsTextIndexedProvider&& is_text_indexed_provider = AllTextIndexed(),
    HNSWInfoProvider&& hnsw_info_provider = NoHNSW())
    : SearchSinkInsertBaseImpl{trx,
                               std::move(tokenizer_provider),
                               std::move(json_paths_provider),
                               std::move(store_values_provider),
                               std::move(is_text_indexed_provider),
                               std::move(hnsw_info_provider),
                               columns},
      SearchSinkDeleteBaseImpl{trx} {}

  void Init(duckdb::idx_t batch_size, const duckdb::DataChunk&) final {
    SearchSinkInsertBaseImpl::InitImpl(batch_size);
    SearchSinkDeleteBaseImpl::InitImpl(batch_size);
  }

  bool SwitchColumn(const ColumnDescriptor& col, const duckdb::Vector& vec,
                    duckdb::idx_t count) final;

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    WriteImpl(cell_slices, full_key);
  }

  void Finish() final {
    SearchSinkDeleteBaseImpl::FinishImpl();
    SearchSinkInsertBaseImpl::FinishImpl();
  }

  void Abort() final {
    SearchSinkInsertBaseImpl::AbortImpl();
    SearchSinkDeleteBaseImpl::AbortImpl();
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteRowImpl(encoded_pk);
  }
};

}  // namespace sdb::connector
