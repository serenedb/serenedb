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

#include <velox/functions/prestosql/types/JsonType.h>
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

  // JSON columns get special multi-field traversal.
  // Note: the Velox scan layer delivers VarcharType vectors even for JSON
  // columns -- the JsonType singleton is lost. We detect JSON columns by name.
  SDB_PRINT("SwitchColumn id=", column_id, " type.kind=", (int)type.kind(),
            " type.name=", type.name());
  if (type.kind() == facebook::velox::TypeKind::VARCHAR &&
      std::string_view{type.name()} == "JSON") {
    SDB_PRINT("-> SetupJsonColumnWriter");
    SetupJsonColumnWriter(column_id, have_nulls);
  } else if (type.kind() == facebook::velox::TypeKind::UNKNOWN) {
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
    search::mangling::MangleString(_name_buffer);
    _field.PrepareForStringValue(_analyzer_provider(column_id));
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter(make_nullable_writer_func(&WriteStringValue));
    } else {
      _current_writer = MakeIndexWriter(&WriteStringValue);
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    search::mangling::MangleBool(_name_buffer);
    _field.PrepareForBooleanValue();
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter(make_nullable_writer_func(&WriteBooleanValue));
    } else {
      _current_writer = MakeIndexWriter(&WriteBooleanValue);
    }
  } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
    search::mangling::MangleNumeric(_name_buffer);
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

// Questions:
// - Do we need special catalog::Column::ID for each column? Look like no, just
// mangling path for now?
// - Backfill wants to write varchar instead of json. Find out why

// Key:
// column_id.
void SearchSinkInsertBaseImpl::SetupJsonColumnWriter(
  catalog::Column::Id column_id, bool have_nulls) {
  // Build the column_id prefix once and store in _json_path_buffer.
  basics::StrResize(_json_path_buffer, sizeof(column_id));
  absl::big_endian::Store(_json_path_buffer.data(), column_id);
  _json_column_prefix_len = _json_path_buffer.size();

  // Prepare per-type leaf fields.
  // JSON string leaves use the verbatim identity tokenizer so that TERM_EQ and
  // TERM_LIKE can match exact values (e.g. "server-01").  A text analyzer
  // would split the value into sub-tokens, making exact-match queries fail.
  _json_str_field.PrepareForVerbatimStringValue();
  _json_num_field.PrepareForNumericValue();
  _json_bool_field.PrepareForBooleanValue();
  _json_null_field.PrepareForNullValue();

  // The null field name for null leaves is the column prefix + mangled-null.
  // We reuse _null_name_buffer to keep the mangled name stable.
  basics::StrResize(_null_name_buffer, sizeof(column_id));
  absl::big_endian::Store(_null_name_buffer.data(), column_id);
  search::mangling::MangleNull(_null_name_buffer);
  _json_null_field.name = _null_name_buffer;

  _current_writer = [&](std::string_view full_key,
                        std::span<const rocksdb::Slice> cell_slices) {
    (void)full_key;
    // Reconstruct the JSON string from RocksDB slices.
    // Same logic as WriteStringValue: may have a kStringPrefix byte.
    std::string_view json_str;
    SDB_ASSERT(1 <= cell_slices.size() && cell_slices.size() <= 2);
    if (!cell_slices.front().starts_with(kStringPrefix)) {
      json_str = {cell_slices.front().data(), cell_slices.front().size()};
    } else if (cell_slices.size() == 1) {
      json_str = {cell_slices.front().data() + 1,
                  cell_slices.front().size() - 1};
    } else {
      json_str = {cell_slices[1].data(), cell_slices[1].size()};
    }

    // If the stored value is a JSON-encoded string (outer quotes + escaping),
    // unescape it first to get the raw JSON, then re-parse.
    SDB_PRINT("json_str=", json_str);
    _json_parser.PrepareJson(json_str);
    {
      auto doc = _json_parser.GetJsonDocument();
      SDB_PRINT("zalupa=", doc.raw_json().value());
      SDB_PRINT("zalupa2=", doc.type().value());
      if (doc.type() == simdjson::ondemand::json_type::string) {
        std::string_view unescaped;
        auto error = doc.get_string().get(unescaped);
        if (error == simdjson::SUCCESS) {
          SDB_PRINT("unescaped json=", unescaped);
          _json_parser.PrepareJson(unescaped);
        } else {
          SDB_PRINT("zalupa =", error);
        }
      }
      // SDB_PRINT("root val type = ", root_val.type().value());
    }
    {
      auto doc = _json_parser.GetJsonDocument();
      simdjson::ondemand::value root_val;
      if (doc.get_value().get(root_val)) {
        return;
      }
      // Restore path buffer to just the column prefix before traversal.
      _json_path_buffer.resize(_json_column_prefix_len);
      TraverseJsonValue(root_val, 0);
    }
  };

  // Never emit PK a second time through the standard path for this column;
  // the pk emission is handled by the outer wrapper set in SetupColumnWriter.
  // For JSON columns we skip the PK wrapper here -- it's already been emitted
  // once at the start of the batch (InitImpl sets _emit_pk=true, and the first
  // non-JSON column will emit it; if all columns are JSON we handle it here).
  if (_emit_pk) {
    _current_writer = [&, data_writer = std::move(_current_writer)](
                        std::string_view full_key,
                        std::span<const rocksdb::Slice> cell_slices) {
      auto row_key = key_utils::ExtractRowKey(full_key);
      _pk_field.value = irs::ViewCast<irs::byte_type>(row_key);
      _pk_field.SetStringValue(row_key);
      VELOX_CHECK(
        _document->template Insert<irs::Action::INDEX | irs::Action::STORE>(
          _pk_field),
        "Failed to insert PK field into IResearch document");
      data_writer(full_key, cell_slices);
    };
    _emit_pk = false;
  }
}

void SearchSinkInsertBaseImpl::TraverseJsonDocument(
  simdjson::ondemand::document& doc, int depth) {
  if (depth > kMaxJsonDepth) {
    return;
  }
  auto type_result = doc.type();
  if (type_result.error()) {
    return;
  }
  switch (type_result.value()) {
    case simdjson::ondemand::json_type::object: {
      simdjson::ondemand::object obj;
      if (doc.get_object().get(obj)) {
        return;
      }
      for (auto field_result : obj) {
        if (field_result.error()) {
          continue;
        }
        auto field = field_result.value_unsafe();
        auto key_result = field.unescaped_key();
        if (key_result.error()) {
          continue;
        }
        std::string_view key = key_result.value_unsafe();
        size_t prev_len = _json_path_buffer.size();
        _json_path_buffer += '.';
        _json_path_buffer += key;
        TraverseJsonValue(field.value(), depth + 1);
        _json_path_buffer.resize(prev_len);
      }
      break;
    }
    case simdjson::ondemand::json_type::array: {
      simdjson::ondemand::array arr;
      if (doc.get_array().get(arr)) {
        return;
      }
      for (auto elem_result : arr) {
        if (elem_result.error()) {
          continue;
        }
        TraverseJsonValue(elem_result.value_unsafe(), depth + 1);
      }
      break;
    }
    case simdjson::ondemand::json_type::string: {
      auto str_result = doc.get_string();
      if (str_result.error()) {
        return;
      }
      EmitJsonStringField(str_result.value_unsafe());
      break;
    }
    case simdjson::ondemand::json_type::number: {
      auto num_result = doc.get_double();
      if (num_result.error()) {
        return;
      }
      EmitJsonNumericField(num_result.value_unsafe());
      break;
    }
    case simdjson::ondemand::json_type::boolean: {
      auto bool_result = doc.get_bool();
      if (bool_result.error()) {
        return;
      }
      EmitJsonBoolField(bool_result.value_unsafe());
      break;
    }
    case simdjson::ondemand::json_type::null:
      EmitJsonNullField();
      break;
    default:
      break;
  }
}

void SearchSinkInsertBaseImpl::TraverseJsonValue(simdjson::ondemand::value val,
                                                 int depth) {
  if (depth > kMaxJsonDepth) {
    return;
  }
  auto type_result = val.type();
  if (type_result.error()) {
    return;
  }

  switch (type_result.value()) {
    case simdjson::ondemand::json_type::object: {
      simdjson::ondemand::object obj;
      if (val.get_object().get(obj)) {
        return;
      }
      for (auto field_result : obj) {
        if (field_result.error()) {
          continue;
        }
        auto field = field_result.value_unsafe();
        auto key_result = field.unescaped_key();
        if (key_result.error()) {
          continue;
        }
        std::string_view key = key_result.value_unsafe();

        size_t prev_len = _json_path_buffer.size();
        _json_path_buffer += '.';
        _json_path_buffer += key;

        // field.value() returns simdjson::ondemand::value directly
        TraverseJsonValue(field.value(), depth + 1);
        _json_path_buffer.resize(prev_len);
      }
      break;
    }
    case simdjson::ondemand::json_type::array: {
      simdjson::ondemand::array arr;
      if (val.get_array().get(arr)) {
        return;
      }
      for (auto elem_result : arr) {
        if (elem_result.error()) {
          continue;
        }
        TraverseJsonValue(elem_result.value_unsafe(), depth + 1);
      }
      break;
    }
    case simdjson::ondemand::json_type::string: {
      auto str_result = val.get_string();
      if (str_result.error()) {
        return;
      }
      EmitJsonStringField(str_result.value_unsafe());
      break;
    }
    case simdjson::ondemand::json_type::number: {
      auto num_result = val.get_double();
      if (num_result.error()) {
        return;
      }
      EmitJsonNumericField(num_result.value_unsafe());
      break;
    }
    case simdjson::ondemand::json_type::boolean: {
      auto bool_result = val.get_bool();
      if (bool_result.error()) {
        return;
      }
      EmitJsonBoolField(bool_result.value_unsafe());
      break;
    }
    case simdjson::ondemand::json_type::null: {
      EmitJsonNullField();
      break;
    }
    default:
      break;
  }
}

void SearchSinkInsertBaseImpl::EmitJsonStringField(std::string_view value) {
  SDB_PRINT("EmitJsonStringField path=", _json_path_buffer, " val=", value);
  _json_field_name_buffer = _json_path_buffer;
  search::mangling::MangleString(_json_field_name_buffer);
  _json_str_field.name = _json_field_name_buffer;
  _json_str_field.SetStringValue(value);
  VELOX_CHECK(_document->template Insert<irs::Action::INDEX>(&_json_str_field),
              "Failed to insert JSON string field into IResearch document");
}

void SearchSinkInsertBaseImpl::EmitJsonNumericField(double value) {
  _json_field_name_buffer = _json_path_buffer;
  search::mangling::MangleNumeric(_json_field_name_buffer);
  _json_num_field.name = _json_field_name_buffer;
  _json_num_field.SetNumericValue(value);
  VELOX_CHECK(_document->template Insert<irs::Action::INDEX>(&_json_num_field),
              "Failed to insert JSON numeric field into IResearch document");
}

void SearchSinkInsertBaseImpl::EmitJsonBoolField(bool value) {
  _json_field_name_buffer = _json_path_buffer;
  search::mangling::MangleBool(_json_field_name_buffer);
  _json_bool_field.name = _json_field_name_buffer;
  _json_bool_field.SetBooleanValue(value);
  VELOX_CHECK(_document->template Insert<irs::Action::INDEX>(&_json_bool_field),
              "Failed to insert JSON bool field into IResearch document");
}

void SearchSinkInsertBaseImpl::EmitJsonNullField() {
  // Null field name was already set in SetupJsonColumnWriter.
  _json_null_field.SetNullValue();
  VELOX_CHECK(_document->template Insert<irs::Action::INDEX>(&_json_null_field),
              "Failed to insert JSON null field into IResearch document");
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
  if (_document) {
    _document.reset();
  }
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
