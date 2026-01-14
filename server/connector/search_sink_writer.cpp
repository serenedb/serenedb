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
#include "velox/functions/prestosql/types/JsonType.h"

namespace {

using namespace sdb::search;
using namespace sdb::connector;
using namespace sdb::catalog;

constexpr size_t kDefaultPoolSize = 8;  // arbitrary value
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gStringStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gNumberStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gBoolStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gNullStreamPool(
  kDefaultPoolSize);

void SetNameToBuffer(std::string& name_buffer, Column::Id column_id) {
  SDB_ASSERT(name_buffer.size() >= sizeof(column_id));
  absl::big_endian::Store(name_buffer.data(), column_id);
}

}  // namespace

namespace sdb::connector::search {

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx)
  : _trx(trx) {
  _pk_field.PrepareForStringValue();
  _pk_field.name = kPkFieldName;
}

bool SearchSinkInsertBaseImpl::SwitchColumnImpl(
  velox::TypeKind kind, bool have_nulls, sdb::catalog::Column::Id column_id) {
  if (kind == facebook::velox::TypeKind::UNKNOWN) {
    // for UNKNOWN type we always have nulls so no need of separate nulls
    // handling
    SetupColumnWriter<velox::TypeKind::UNKNOWN>(column_id, false);
  } else {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(SetupColumnWriter, kind, column_id,
                                       have_nulls);
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
void SearchSinkInsertBaseImpl::SetupColumnWriter(
  sdb::catalog::Column::Id column_id, bool have_nulls) {
  basics::StrResize(_name_buffer, sizeof(column_id));
  SetNameToBuffer(_name_buffer, column_id);
  using T = typename velox::TypeTraits<Kind>::NativeType;

  if (have_nulls || Kind == velox::TypeKind::UNKNOWN) {
    basics::StrResize(_null_name_buffer, sizeof(column_id));
    SetNameToBuffer(_null_name_buffer, column_id);
    mangling::MangleNull(_null_name_buffer);
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
    _current_writer = MakeIndexWriter(
      [&](std::string_view full_key,
          std::span<const rocksdb::Slice> cell_slices, Field&) -> Field& {
        SDB_ASSERT(cell_slices.size() == 1);
        SDB_ASSERT(cell_slices.front().empty());
        _null_field.SetNullValue();
        return _null_field;
      });
  } else if constexpr (Kind == velox::TypeKind::VARCHAR ||
                       Kind == velox::TypeKind::VARBINARY) {
    mangling::MangleString(_name_buffer);
    _field.PrepareForStringValue();
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter(make_nullable_writer_func(&WriteStringValue));
    } else {
      _current_writer = MakeIndexWriter(&WriteStringValue);
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    mangling::MangleBool(_name_buffer);
    _field.PrepareForBooleanValue();
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter(make_nullable_writer_func(&WriteBooleanValue));
    } else {
      _current_writer = MakeIndexWriter(&WriteBooleanValue);
    }
  } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
    mangling::MangleNumeric(_name_buffer);
    _field.PrepareForNumericValue();
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter(make_nullable_writer_func(&WriteNumericValue<T>));
    } else {
      _current_writer = MakeIndexWriter(&WriteNumericValue<T>);
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
      _pk_field.value = irs::ViewCast<irs::byte_type>(row_key);
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

template<typename WriteFunc>
SearchSinkInsertBaseImpl::Writer SearchSinkInsertBaseImpl::MakeIndexWriter(
  WriteFunc&& write_func) {
  return
    [&, func = std::forward<WriteFunc>(write_func)](
      std::string_view full_key, std::span<const rocksdb::Slice> cell_slices) {
      VELOX_CHECK(_document->template Insert<irs::Action::INDEX>(
                    &func(full_key, cell_slices, _field)),
                  "Failed to insert field into IResearch document");
    };
}

void SearchSinkInsertBaseImpl::InitImpl(size_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  SDB_ASSERT(!_document.has_value());
  _document.emplace(_trx.Insert(false, batch_size));
  _emit_pk = true;
}

SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteStringValue(
  std::string_view, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  SDB_ASSERT(!cell_slices.empty());
  // if string is prefixed during Insert - two slices will be present
  // one is prefix, second is actual string data
  // But if we are re-indexing from existing data (Update operation) - only one
  // slice will be present
  SDB_ASSERT(cell_slices.size() <= 2);
  if (!cell_slices.front().starts_with(kStringPrefix)) {
    field.SetStringValue(
      {cell_slices.front().data(), cell_slices.front().size()});
  } else {
    if (cell_slices.size() == 1) {
      // re-indexing case
      field.SetStringValue(
        {cell_slices.front().data() + 1, cell_slices.front().size() - 1});
    } else {
      field.SetStringValue({cell_slices[1].data(), cell_slices[1].size()});
    }
  }
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

void SearchSinkInsertBaseImpl::Field::PrepareForStringValue() {
  index_features = irs::IndexFeatures::None;
  analyzer = gStringStreamPool.emplace(AnalyzerImpl::StringStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetStringValue(std::string_view value) {
  auto& sstream = sdb::basics::downCast<irs::StringTokenizer>(*analyzer);
  sstream.reset(value);
}

void SearchSinkInsertBaseImpl::Field::PrepareForNumericValue() {
  index_features = irs::IndexFeatures::None;
  analyzer = gNumberStreamPool.emplace(AnalyzerImpl::NumberStreamTag{});
}

template<typename T>
void SearchSinkInsertBaseImpl::Field::SetNumericValue(T value) {
  auto& nstream = sdb::basics::downCast<irs::NumericTokenizer>(*analyzer);
  if constexpr (std::is_same_v<
                  T, velox::TypeTraits<velox::TypeKind::HUGEINT>::NativeType>) {
    // TODO(Dronplane): Native int128 support
    nstream.reset(static_cast<double>(value));
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
  index_features = irs::IndexFeatures::None;
  analyzer = gBoolStreamPool.emplace(AnalyzerImpl::BoolStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetBooleanValue(bool value) {
  auto& bstream = sdb::basics::downCast<irs::BooleanTokenizer>(*analyzer);
  bstream.reset(value);
}

void SearchSinkInsertBaseImpl::Field::PrepareForNullValue() {
  index_features = irs::IndexFeatures::None;
  analyzer = gNullStreamPool.emplace(AnalyzerImpl::NullStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetNullValue() {
  auto& nstream = sdb::basics::downCast<irs::NullTokenizer>(*analyzer);
  nstream.reset();
}

SearchSinkDeleteBaseImpl::SearchSinkDeleteBaseImpl(
  irs::IndexWriter::Transaction& trx, velox::memory::MemoryPool& removes_pool)
  : _trx(trx), _removes_pool(removes_pool) {}

void SearchSinkDeleteBaseImpl::DeleteRowImpl(std::string_view row_key) {
  SDB_ASSERT(_remove_filter);
  _remove_filter->Add(row_key);
}

void SearchSinkDeleteBaseImpl::InitImpl(size_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  SDB_ASSERT(!_remove_filter);
  _remove_filter =
    std::make_shared<SearchRemoveFilter>(_removes_pool, batch_size);
}

void SearchSinkDeleteBaseImpl::FinishImpl() {
  if (_remove_filter && !_remove_filter->Empty()) {
    _trx.Remove(std::move(_remove_filter));
  }
  _remove_filter.reset();
}

}  // namespace sdb::connector::search
