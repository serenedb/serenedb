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

#pragma once
#include <velox/vector/ComplexVector.h>

#include <iresearch/index/index_writer.hpp>

#include "catalog/search_analyzer_impl.h"
#include "primary_key.hpp"
#include "sink_writer_base.hpp"
#include "search_remove_filter.hpp"

namespace sdb::connector::search {

class SearchRemoveFilterBase; 

class SearchSinkWriter final : public SinkWriterBase {
 public:
  SearchSinkWriter(irs::IndexWriter::Transaction& trx, velox::memory::MemoryPool* removes_pool);

  void Init(size_t batch_size) override;

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) override;

  void SwitchColumn(velox::TypeKind kind, bool have_nulls,  sdb::catalog::Column::Id column_id) override;
  void Finish() override;

  void DeleteRow(std::string_view row_key) override;
  void Delete(std::string_view) override
  {
    VELOX_UNSUPPORTED("SearchSinkWriter does not support Delete operation");
  }

 private:
  struct Field {
    std::string_view Name() const noexcept {
      SDB_ASSERT(!irs::IsNull(name));
      return name;
    }

    irs::IndexFeatures GetIndexFeatures() const noexcept {
      return index_features;
    }

    irs::Tokenizer& GetTokens() const noexcept {
      SDB_ASSERT(analyzer);
      return *analyzer;
    }

    bool Write(irs::DataOutput& out) const {
      if (!irs::IsNull(value)) {
        out.WriteBytes(value.data(), value.size());
      }

      return true;
    }
    void PrepareForStringValue();
    void SetStringValue(std::string_view value);

    void PrepareForNumericValue();
    template<typename T>
    void SetNumericValue(T value);

    void PrepareForBooleanValue();
    void SetBooleanValue(bool value);

    void PrepareForNullValue();
    void SetNullValue();

    sdb::search::AnalyzerImpl::CacheType::ptr analyzer;
    std::string_view name;
    irs::bytes_view value;
    irs::IndexFeatures index_features;
  };

  using Writer = std::function<Field&(std::string_view full_key,
                                     std::span<const rocksdb::Slice> cell_slices,
                                     Field& field, Field& null_field)>;
  template<bool have_nulls>
  static Field& WriteStringValue(std::string_view full_key,
                               std::span<const rocksdb::Slice> cell_slices,
                               Field& field, Field& null_field);
  template<bool have_nulls>
  static Field& WriteBooleanValue(std::string_view full_key,
                                std::span<const rocksdb::Slice> cell_slices,
                                Field& field, Field& null_field);

  static Field& WriteNullValue(std::string_view full_key,
                             std::span<const rocksdb::Slice> cell_slices,
                             Field& field, Field& null_field);

  template<typename T, bool have_nulls>
  static Field& WriteNumericValue(std::string_view full_key,
                                std::span<const rocksdb::Slice> cell_slices,
                                Field& field, Field& null_field);

  template<velox::TypeKind Kind>
  void SetupColumnWriter(sdb::catalog::Column::Id column_id, bool have_nulls);

  Field _field;
  Field _pk_field;
  Field _null_field;
  std::string _name_buffer;
  std::string _null_name_buffer;
  irs::IndexWriter::Transaction& _trx;
  std::unique_ptr<irs::IndexWriter::Document> _document;
  Writer _current_writer;
  bool _emit_pk{true};
  velox::memory::MemoryPool* _removes_pool;
  std::shared_ptr<SearchRemoveFilterBase> _remove_filter;
};



}  // namespace sdb::connector::search
