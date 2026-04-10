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

#include "search_sink_writer.hpp"

#include <velox/type/Type.h>
#include <velox/vector/FlatVector.h>

#include <iresearch/analysis/tokenizers.hpp>

#include "basics/assert.h"
#include "basics/endian.h"
#include "basics/fwd.h"
#include "catalog/mangling.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/key_utils.hpp"
#include "search_remove_filter.hpp"

namespace sdb::connector {
namespace {

constexpr size_t kDefaultPoolSize = 8;  // arbitrary value
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gStringStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gNumberStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gBoolStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gNullStreamPool(
  kDefaultPoolSize);

void SetNameToBuffer(std::string& name_buffer, catalog::Column::Id column_id) {
  SDB_ASSERT(name_buffer.size() >= sizeof(column_id));
  absl::big_endian::Store(name_buffer.data(), column_id);
}

std::string_view DecodeString(std::span<const rocksdb::Slice> cell_slices) {
  // if string is prefixed during Insert - two slices will be present
  // one is prefix, second is actual string data
  // But if we are re-indexing from existing data (Update operation) - only one
  // slice will be present
  SDB_ASSERT(cell_slices.size() <= 2);
  if (!cell_slices.front().starts_with(kStringPrefix)) {
    SDB_ASSERT(cell_slices.size() == 1);
    return {cell_slices.front().data(), cell_slices.front().size()};
  } else {
    if (cell_slices.size() == 1) {
      // re-indexing case
      return {cell_slices.front().data() + 1, cell_slices.front().size() - 1};
    } else {
      return {cell_slices[1].data(), cell_slices[1].size()};
    }
  }
}

}  // namespace

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx, AnalyzerProvider&& analyzer_provider,
  std::span<const catalog::Column::Id> columns)
  : ColumnSinkWriterImplBase{columns},
    _analyzer_provider{std::move(analyzer_provider)},
    _trx{trx} {
  _pk_field.PrepareForVerbatimStringValue();
  _pk_field.name = kPkFieldName;
}

bool SearchSinkInsertBaseImpl::SwitchColumnImpl(const velox::Type& type,
                                                bool have_nulls,
                                                catalog::Column::Id column_id) {
  if (!IsIndexed(column_id)) {
#ifdef SDB_DEV
    _current_writer = nullptr;
#endif
    return false;
  }
  // For now we do not support types that are not default comparable as our
  // ranges depend on that.
  SDB_ASSERT(!type.providesCustomComparison());
  if (type.kind() == facebook::velox::TypeKind::UNKNOWN) {
    // for UNKNOWN type we always have nulls so no need of separate nulls
    // handling
    SetupColumnWriter<velox::TypeKind::UNKNOWN>(column_id, false);
  } else {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(SetupColumnWriter, type.kind(),
                                       column_id, have_nulls);
  }
  SDB_ASSERT(_document.has_value());
  _document->NextFieldBatch();
  return true;
}

void SearchSinkInsertBaseImpl::WriteImpl(
  std::span<const rocksdb::Slice> cell_slices, std::string_view full_key) {
  SDB_ASSERT(_current_writer);
  SDB_ASSERT(_document.has_value());
  _current_writer(full_key, cell_slices);
  _document->NextDocument();
}

void SearchSinkInsertBaseImpl::FinishImpl() { _document.reset(); }

template<velox::TypeKind Kind>
void SearchSinkInsertBaseImpl::SetupColumnWriter(catalog::Column::Id column_id,
                                                 bool have_nulls) {
  basics::StrResize(_name_buffer, sizeof(column_id));
  SetNameToBuffer(_name_buffer, column_id);
  using T = typename velox::TypeTraits<Kind>::NativeType;

  if (have_nulls || Kind == velox::TypeKind::UNKNOWN) {
    basics::StrResize(_null_name_buffer, sizeof(column_id));
    SetNameToBuffer(_null_name_buffer, column_id);
    search::mangling::MangleNull(_null_name_buffer);
    _null_field.name = _null_name_buffer;
    if (!_null_field.analyzer) {
      _null_field.PrepareForNullValue();
    }
  }

  // Generic wrapper for handling nulls in column.
  auto make_nullable_writer_func =
    [&]<typename WriteFunc>(WriteFunc&& write_func) {
      return
        [&, write_func = std::forward<WriteFunc>(write_func)](
          std::string_view full_key,
          std::span<const rocksdb::Slice> cell_slices, Field& field) -> Field& {
          if (cell_slices.size() == 1 && cell_slices.front().empty()) {
            _null_field.SetNullValue();
            return _null_field;
          }
          return write_func(full_key, cell_slices, field);
        };
    };

  if constexpr (Kind == velox::TypeKind::UNKNOWN) {
    _current_writer = MakeIndexWriter<false>(
      [&](std::string_view full_key,
          std::span<const rocksdb::Slice> cell_slices, Field&) -> Field& {
        SDB_ASSERT(cell_slices.size() == 1);
        SDB_ASSERT(cell_slices.front().empty());
        _null_field.SetNullValue();
        return _null_field;
      });
  } else if constexpr (Kind == velox::TypeKind::VARCHAR ||
                       Kind == velox::TypeKind::VARBINARY) {
    search::mangling::MangleString(_name_buffer);
    auto column_analyzer = _analyzer_provider(column_id);
    const auto* store_attr =
      irs::get<irs::StoreAttr>(*column_analyzer.analyzer);
    _field.PrepareForStringValue(std::move(column_analyzer));
    _field.store_attr = store_attr;
    irs::ResolveBool(store_attr != nullptr, [&]<bool HasStore> {
      _current_writer =
        have_nulls ? MakeIndexWriter<HasStore>(
                       make_nullable_writer_func(&WriteStringValue<HasStore>))
                   : MakeIndexWriter<HasStore>(&WriteStringValue<HasStore>);
    });
  } else if constexpr (std::is_same_v<T, bool>) {
    search::mangling::MangleBool(_name_buffer);
    _field.PrepareForBooleanValue();
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter<false>(make_nullable_writer_func(&WriteBooleanValue));
    } else {
      _current_writer = MakeIndexWriter<false>(&WriteBooleanValue);
    }
  } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
    search::mangling::MangleNumeric(_name_buffer);
    _field.PrepareForNumericValue();
    if (have_nulls) {
      _current_writer = MakeIndexWriter<false>(
        make_nullable_writer_func(&WriteNumericValue<T>));
    } else {
      _current_writer = MakeIndexWriter<false>(&WriteNumericValue<T>);
    }
  } else {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "TypeKind ",
              velox::TypeKindName::toName(Kind),
              " is not supported in search index");
  }
  _field.name = _name_buffer;

  if (_emit_pk) {
    // TODO(Dronplane): if pk contains only one column and that column is also
    // indexed we can avoid indexing this column twice. But then we need to get
    // here info about this case and also search source should be ready to
    // handle that.
    _current_writer = [&, data_writer = std::move(_current_writer)](
                        std::string_view full_key,
                        std::span<const rocksdb::Slice> cell_slices) {
      auto row_key = key_utils::ExtractRowKey(full_key);
      irs::StoreAttr pk_store;
      pk_store.value = irs::ViewCast<irs::byte_type>(row_key);
      _pk_field.store_attr = &pk_store;
      _pk_field.SetStringValue(row_key);
      // We need indexed PK for removes
      VELOX_CHECK(
        _document->template Insert<irs::Action::INDEX | irs::Action::STORE>(
          _pk_field),
        "Failed to insert PK field into IResearch document");
      data_writer(full_key, cell_slices);
    };
    _emit_pk = false;
  }
}

template<bool HasStore, typename WriteFunc>
SearchSinkInsertBaseImpl::Writer SearchSinkInsertBaseImpl::MakeIndexWriter(
  WriteFunc&& write_func) {
  return
    [&, func = std::forward<WriteFunc>(write_func)](
      std::string_view full_key, std::span<const rocksdb::Slice> cell_slices) {
      auto& field = func(full_key, cell_slices, _field);
      if constexpr (HasStore) {
        if (field.store_attr) {
          VELOX_CHECK(
            _document->template Insert<irs::Action::INDEX | irs::Action::STORE>(
              &field),
            "Failed to insert and store field into IResearch document");
          return;
        }
      }
      VELOX_CHECK(_document->template Insert<irs::Action::INDEX>(&field),
                  "Failed to insert field into IResearch document");
    };
}

void SearchSinkInsertBaseImpl::InitImpl(size_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  if (_document) {
    _document.reset();
  }
  _document.emplace(_trx.Insert(false, batch_size));
  _emit_pk = true;
}

template<bool HasStore>
SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteStringValue(
  std::string_view, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  SDB_ASSERT(!cell_slices.empty());
  auto decoded_string = DecodeString(cell_slices);
  field.SetStringValue(decoded_string);
  return field;
}

template<typename T>
SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteNumericValue(
  std::string_view, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  SDB_ASSERT(cell_slices.size() == 1);
  SDB_ASSERT(sizeof(T) == cell_slices[0].size());
  // this is true as long as we match machine ending with storage ending
  static_assert(basics::IsLittleEndian());
  field.SetNumericValue(absl::little_endian::Load<T>(cell_slices[0].data()));
  return field;
}

SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteBooleanValue(
  std::string_view, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  SDB_ASSERT(cell_slices.size() == 1);
  SDB_ASSERT(cell_slices[0].size() == 1);
  field.SetBooleanValue(cell_slices.front() == kTrueValue);
  return field;
}

void SearchSinkInsertBaseImpl::Field::PrepareForVerbatimStringValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gStringStreamPool.emplace(search::AnalyzerImpl::StringStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::PrepareForStringValue(
  catalog::ColumnAnalyzer&& column_analyzer) {
  index_features = column_analyzer.features;
  SDB_ASSERT(column_analyzer.analyzer);
  analyzer.reset();
  string_analyzer = std::move(column_analyzer.analyzer);
}

void SearchSinkInsertBaseImpl::Field::SetStringValue(std::string_view value) {
  SDB_ASSERT(analyzer || string_analyzer);
  SDB_ASSERT((analyzer == nullptr) || !string_analyzer.has_value());
  if (analyzer) {
    auto& sstream = basics::downCast<irs::StringTokenizer>(*analyzer);
    sstream.reset(value);
  } else {
    string_analyzer.value()->reset(value);
  }
}

void SearchSinkInsertBaseImpl::Field::PrepareForNumericValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gNumberStreamPool.emplace(search::AnalyzerImpl::NumberStreamTag{});
}

template<typename T>
void SearchSinkInsertBaseImpl::Field::SetNumericValue(T value) {
  auto& nstream = basics::downCast<irs::NumericTokenizer>(*analyzer);
  if constexpr (std::is_same_v<
                  T, velox::TypeTraits<velox::TypeKind::HUGEINT>::NativeType>) {
    // TODO(Dronplane): Native int128 support
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "HUGEINT kind is not supported");
  } else if constexpr (
    std::is_same_v<T,
                   velox::TypeTraits<velox::TypeKind::TINYINT>::NativeType> ||
    std::is_same_v<T,
                   velox::TypeTraits<velox::TypeKind::SMALLINT>::NativeType>) {
    // TODO(Dronplane): Native int 16/8 support
    nstream.reset(static_cast<int32_t>(value));
  } else {
    nstream.reset(value);
  }
}

void SearchSinkInsertBaseImpl::Field::PrepareForBooleanValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gBoolStreamPool.emplace(search::AnalyzerImpl::BoolStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetBooleanValue(bool value) {
  auto& bstream = basics::downCast<irs::BooleanTokenizer>(*analyzer);
  bstream.reset(value);
}

void SearchSinkInsertBaseImpl::Field::PrepareForNullValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gNullStreamPool.emplace(search::AnalyzerImpl::NullStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetNullValue() {
  auto& nstream = basics::downCast<irs::NullTokenizer>(*analyzer);
  nstream.reset();
}

SearchSinkDeleteBaseImpl::SearchSinkDeleteBaseImpl(
  irs::IndexWriter::Transaction& trx)
  : _trx{trx} {}

void SearchSinkDeleteBaseImpl::DeleteRowImpl(std::string_view row_key) {
  SDB_ASSERT(_remove_filter);
  _remove_filter->Add(row_key);
}

void SearchSinkDeleteBaseImpl::InitImpl(size_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  FinishImpl();
  SDB_ASSERT(!_remove_filter);
  _remove_filter = std::make_shared<SearchRemoveFilter>(batch_size);
}

void SearchSinkDeleteBaseImpl::FinishImpl() {
  if (_remove_filter && !_remove_filter->Empty()) {
    _trx.Remove(std::move(_remove_filter));
  }
  _remove_filter.reset();
}

}  // namespace sdb::connector
