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
#include "basics/fwd.h"
#include "catalog/mangling.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/key_utils.hpp"
#include "velox/functions/prestosql/types/JsonType.h"

namespace {

using namespace sdb::search;
using namespace sdb::connector;
using namespace sdb::catalog;

constexpr auto kPkFieldName = std::numeric_limits<Column::Id>::max();

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

struct Field {
  void SetPkValue(std::string_view pk) {
    SetStringValue(pk);
    // For PK we want to store the value so we could emit it back in search
    // results
    value = irs::bytes_view(reinterpret_cast<const irs::byte_type*>(pk.data()),
                            pk.size());
  }

  void SetStringValue(std::string_view value) {
    index_features = irs::IndexFeatures::None;
    analyzer = gStringStreamPool.emplace(AnalyzerImpl::StringStreamTag{});
    auto& sstream = sdb::basics::downCast<irs::StringTokenizer>(*analyzer);
    sstream.reset(value);
  }

  void SetNumericValue(double value) {
    index_features = irs::IndexFeatures::None;
    analyzer = gNumberStreamPool.emplace(AnalyzerImpl::NumberStreamTag{});
    auto& nstream = sdb::basics::downCast<irs::NumericTokenizer>(*analyzer);
    nstream.reset(value);
  }

  void SetBooleanValue(bool value) {
    index_features = irs::IndexFeatures::None;
    analyzer = gBoolStreamPool.emplace(AnalyzerImpl::BoolStreamTag{});
    auto& nstream = sdb::basics::downCast<irs::NumericTokenizer>(*analyzer);
    nstream.reset(value);
  }

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

  AnalyzerImpl::CacheType::ptr analyzer;
  std::string_view name;
  irs::bytes_view value;
  irs::IndexFeatures index_features;
};

}  // namespace

namespace sdb::connector {

SearchSinkWriter::SearchSinkWriter(irs::IndexWriter::Transaction& trx)
  : _trx(trx) {
  basics::StrResize(_pk_buffer, sizeof(Column::Id));
  SetNameToBuffer(_pk_buffer, kPkFieldName);
};

void SearchSinkWriter::SwitchColumn(velox::TypeKind kind,  sdb::catalog::Column::Id column_id) {
    if (_field.analyzer) {
      // not first column - PK already emitted. so drop the flag.
      _emit_pk = false;
      _field.value = {};
    }
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      SetupColumnWriter, kind, column_id);
    SDB_ASSERT(_document);
    _document->NextFieldBatch();
  }

void SearchSinkWriter::Delete(std::string_view full_key) {
  VELOX_UNSUPPORTED("Iresearch delete Not implemented");
}
void SearchSinkWriter::Write(std::span<const rocksdb::Slice> cell_slices,
                             std::string_view full_key) {
  SDB_ASSERT(_current_writer);
  SDB_ASSERT(_document);
  if (_emit_pk) {
    auto row_key = key_utils::ExtractRowKey(full_key);
    _field.value = irs::ViewCast<irs::byte_type>(row_key);
    _field.name = _pk_buffer;
    _field.SetStringValue(row_key);
    VELOX_CHECK(_document->template Insert<irs::Action::INDEX | irs::Action::STORE>(_field),
                "Failed to insert PK field into IResearch document");
    _field.name = _name_buffer;
  }
  _current_writer(cell_slices, _field);
  VELOX_CHECK(_document->template Insert<irs::Action::INDEX>(_field),
              "Failed to insert field into IResearch document");
  _document->NextDocument();
}

template<velox::TypeKind Kind>
void SearchSinkWriter::SetupColumnWriter(sdb::catalog::Column::Id column_id) {
  basics::StrResize(_name_buffer, sizeof(column_id));
  SetNameToBuffer(_name_buffer, column_id);
  using T = typename velox::TypeTraits<Kind>::NativeType;
  if constexpr (Kind == velox::TypeKind::VARCHAR ||
                Kind == velox::TypeKind::VARBINARY) {
    mangling::MangleString(_name_buffer);
    _current_writer = &WriteStringValue;
  } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
    mangling::MangleNumeric(_name_buffer);
    _current_writer = &WriteNumericValue<T>;
  } else if constexpr (std::is_same_v<T, bool>) {
    mangling::MangleBool(_name_buffer);
    _current_writer = &WriteBooleanValue;
  } else {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "TypeKind ",
              velox::TypeKindName::toName(Kind),
              " is not supported in search index");
  }
  _field.name = _name_buffer;
}

void SearchSinkWriter::Init(size_t batch_size) {
  _document = std::make_unique<irs::IndexWriter::Document>(
    _trx.Insert(false, batch_size));
  VELOX_CHECK(_document, "Failed to create IResearch document for insertion");
}

void SearchSinkWriter::WriteStringValue(
  std::span<const rocksdb::Slice> cell_slices, SearchSinkWriter::Field& field) {
  SDB_ASSERT(cell_slices.size() == 1);
  if (cell_slices.front().data()[0] != '\0') {
    field.SetStringValue(
      {cell_slices.front().data(), cell_slices.front().size()});
  } else {
    field.SetStringValue(
      {cell_slices.front().data() + 1, cell_slices.front().size() - 1});
  }
}

template<typename T>
void SearchSinkWriter::WriteNumericValue(
  std::span<const rocksdb::Slice> cell_slices, SearchSinkWriter::Field& field) {
  SDB_ASSERT(cell_slices.size() == 1);
  SDB_ASSERT(sizeof(T) == cell_slices[0].size());
  // this is true as long as we match machine ending with storage ending
  field.SetNumericValue(*reinterpret_cast<const T*>(cell_slices[0].data()));
};

void SearchSinkWriter::WriteBooleanValue(
  std::span<const rocksdb::Slice> cell_slices, SearchSinkWriter::Field& field) {
  SDB_ASSERT(cell_slices.size() == 1);
  SDB_ASSERT(cell_slices[0].size() == 1);
  field.SetBooleanValue(cell_slices.front() == kTrueValue);
};

void SearchSinkWriter::Field::SetStringValue(std::string_view value) {
  index_features = irs::IndexFeatures::None;
  analyzer = gStringStreamPool.emplace(AnalyzerImpl::StringStreamTag{});
  auto& sstream = sdb::basics::downCast<irs::StringTokenizer>(*analyzer);
  sstream.reset(value);
}

void SearchSinkWriter::Field::SetNumericValue(double value) {
  index_features = irs::IndexFeatures::None;
  analyzer = gNumberStreamPool.emplace(AnalyzerImpl::NumberStreamTag{});
  auto& nstream = sdb::basics::downCast<irs::NumericTokenizer>(*analyzer);
  nstream.reset(value);
}

void SearchSinkWriter::Field::SetBooleanValue(bool value) {
  index_features = irs::IndexFeatures::None;
  analyzer = gBoolStreamPool.emplace(AnalyzerImpl::BoolStreamTag{});
  auto& nstream = sdb::basics::downCast<irs::NumericTokenizer>(*analyzer);
  nstream.reset(value);
}

}  // namespace sdb::connector
