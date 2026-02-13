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
#include "search_remove_filter.hpp"
#include "sink_writer_base.hpp"

namespace sdb::connector::search {

class SearchRemoveFilterBase;

// BaseImpl (WriteImpl, finish impl)

class SearchSinkInsertBaseImpl {
 public:
  SearchSinkInsertBaseImpl(irs::IndexWriter::Transaction& trx,
                           std::span<const catalog::Column::Id> columns);

  void InitImpl(size_t batch_size);

  void WriteImpl(std::span<const rocksdb::Slice> cell_slices,
                 std::string_view full_key);

  bool SwitchColumnImpl(const velox::Type& type, bool have_nulls,
                        catalog::Column::Id column_id);
  void FinishImpl();

  void AbortImpl() {
    // We don't own the transaction so Abort should be called outside.
    _document.reset();
  }

 protected:
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

  using Writer = std::function<void(
    std::string_view full_key, std::span<const rocksdb::Slice> cell_slices)>;

  // Write executors. For INDEX, INDEX and STORE, Sort etc.
  // Could be more than one when we have index meta and different indexing
  // options.
  template<typename WriteFunc>
  Writer MakeIndexWriter(WriteFunc&& write_func);

  // Actual value processors. It is set to write executor (see MakeIndexWriter)
  // as a template. This methods are responsible for extracting value from
  // rocksdb slices and setting it to Field structure accordingly.
  static Field& WriteStringValue(std::string_view full_key,
                                 std::span<const rocksdb::Slice> cell_slices,
                                 Field& field);
  static Field& WriteBooleanValue(std::string_view full_key,
                                  std::span<const rocksdb::Slice> cell_slices,
                                  Field& field);

  template<typename T>
  static Field& WriteNumericValue(std::string_view full_key,
                                  std::span<const rocksdb::Slice> cell_slices,
                                  Field& field);

  // Setup column writer according to type kind.
  // Builds actual executor to avoid switch/case on each row whenever possible.
  template<velox::TypeKind Kind>
  void SetupColumnWriter(catalog::Column::Id column_id, bool have_nulls);

  Field _field;
  Field _pk_field;
  Field _null_field;
  std::string _name_buffer;
  std::string _null_name_buffer;
  irs::IndexWriter::Transaction& _trx;
  std::optional<irs::IndexWriter::Document> _document;
  containers::FlatHashSet<catalog::Column::Id> _columns;
  Writer _current_writer;
  bool _emit_pk{true};
};

class SearchSinkDeleteBaseImpl {
 public:
  SearchSinkDeleteBaseImpl(irs::IndexWriter::Transaction& trx);

  void InitImpl(size_t batch_size);

  void FinishImpl();

  void DeleteRowImpl(std::string_view row_key);

  void AbortImpl() { _remove_filter.reset(); }

 private:
  irs::IndexWriter::Transaction& _trx;
  std::shared_ptr<SearchRemoveFilterBase> _remove_filter;
};

class SearchSinkInsertWriter final : public SinkInsertWriter,
                                     public SearchSinkInsertBaseImpl {
 public:
  SearchSinkInsertWriter(irs::IndexWriter::Transaction& trx,
                         std::span<const catalog::Column::Id> columns)
    : SearchSinkInsertBaseImpl(trx, columns) {}

  void Init(size_t batch_size) final { InitImpl(batch_size); }

  bool SwitchColumn(const velox::Type& type, bool have_nulls,
                    catalog::Column::Id column_id) final {
    return SwitchColumnImpl(type, have_nulls, column_id);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    WriteImpl(cell_slices, full_key);
  }

  void Finish() final { FinishImpl(); }

  void Abort() final { AbortImpl(); }
};

class SearchSinkDeleteWriter final : public SinkDeleteWriter,
                                     public SearchSinkDeleteBaseImpl {
 public:
  SearchSinkDeleteWriter(irs::IndexWriter::Transaction& trx)
    : SearchSinkDeleteBaseImpl(trx) {}

  void Init(size_t batch_size) final { InitImpl(batch_size); }

  void DeleteRow(std::string_view row_key) final { DeleteRowImpl(row_key); }

  void Finish() final { FinishImpl(); }

  void Abort() final { AbortImpl(); }
};

class SearchSinkUpdateWriter final : public SinkUpdateWriter,
                                     public SearchSinkInsertBaseImpl,
                                     public SearchSinkDeleteBaseImpl {
 public:
  SearchSinkUpdateWriter(irs::IndexWriter::Transaction& trx,
                         std::span<const catalog::Column::Id> columns)
    : SearchSinkInsertBaseImpl(trx, columns), SearchSinkDeleteBaseImpl(trx) {}

  void Init(size_t batch_size) final {
    SearchSinkInsertBaseImpl::InitImpl(batch_size);
    SearchSinkDeleteBaseImpl::InitImpl(batch_size);
  }

  bool SwitchColumn(const velox::Type& type, bool have_nulls,
                    catalog::Column::Id column_id) final {
    return SwitchColumnImpl(type, have_nulls, column_id);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    WriteImpl(cell_slices, full_key);
  }

  void Finish() final {
    // Deletes should go first to not affect inserts (that are our updated
    // values)
    SearchSinkDeleteBaseImpl::FinishImpl();
    SearchSinkInsertBaseImpl::FinishImpl();
  }

  void Abort() final {
    SearchSinkInsertBaseImpl::AbortImpl();
    SearchSinkDeleteBaseImpl::AbortImpl();
  }

  void DeleteRow(std::string_view row_key) final { DeleteRowImpl(row_key); }

  bool IsIndexed(catalog::Column::Id column_id) const noexcept final {
    // TODO(Dronplane): implement proper check when we have metadata
    return true;
  }
};

}  // namespace sdb::connector::search
