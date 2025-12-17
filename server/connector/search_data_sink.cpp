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

#include "search_data_sink.hpp"

#include <velox/type/Type.h>
#include <velox/vector/FlatVector.h>

#include <iresearch/analysis/tokenizers.hpp>

#include "basics/assert.h"
#include "basics/fwd.h"
#include "catalog/mangling.h"
#include "catalog/search_analyzer_impl.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace {

using namespace sdb::search;
using namespace sdb::connector;

constexpr auto kPkFieldName = std::numeric_limits<key_utils::ColumnId>::max();

constexpr size_t kDefaultPoolSize = 8;  // arbitrary value
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gStringStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gNumberStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gBoolStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<AnalyzerImpl::Builder> gNullStreamPool(
  kDefaultPoolSize);

void SetNameToBuffer(std::string& name_buffer, key_utils::ColumnId column_id) {
  sdb::basics::StrResize(name_buffer, sizeof(key_utils::ColumnId));
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

template<velox::VectorEncoding::Simple Encoding>
void WriteStringVector(key_utils::ColumnId column_id,
                       const velox::BaseVector& vector,
                       velox::vector_size_t idx, std::string_view pk,
                       Field& field, std::string& name_buffer) {
  SetNameToBuffer(name_buffer, column_id);
  mangling::MangleString(name_buffer);
  field.name = name_buffer;
  if constexpr (Encoding == velox::VectorEncoding::Simple::FLAT) {
    const auto* str_vector =
      vector.asUnchecked<velox::FlatVector<velox::StringView>>();

    field.SetStringValue(
      static_cast<std::string_view>(str_vector->rawValues()[idx]));
  } else {
    // Handle copying only when necessary
    const auto* str_vector =
      vector.asUnchecked<velox::SimpleVector<velox::StringView>>();
    field.SetStringValue(
      static_cast<std::string_view>(str_vector->valueAt(idx)));
  }
}

template<bool Flat, typename T>
void WriteNumericVector(key_utils::ColumnId column_id,
                        const velox::BaseVector& vector,
                        velox::vector_size_t idx, std::string_view pk,
                        Field& field, std::string& name_buffer) {
  SetNameToBuffer(name_buffer, column_id);
  mangling::MangleNumeric(name_buffer);
  field.name = name_buffer;
  if constexpr (Flat) {
    const auto* str_vector = vector.asUnchecked<velox::FlatVector<T>>();
    field.SetNumericValue(static_cast<double>(str_vector->rawValues()[idx]));
  } else {
    const auto* str_vector = vector.asUnchecked<velox::SimpleVector<T>>();
    field.SetNumericValue(static_cast<double>(str_vector->valueAt(idx)));
  }
};


[[maybe_unused]] void WriteBooleanVector(key_utils::ColumnId column_id,
                        const velox::BaseVector& vector,
                        velox::vector_size_t idx, std::string_view pk,
                        Field& field, std::string& name_buffer) {
  SetNameToBuffer(name_buffer, column_id);
  mangling::MangleBool(name_buffer);
  field.name = name_buffer;
  field.SetBooleanValue(
    vector.asUnchecked<velox::SimpleVector<bool>>()->valueAt(idx));
};

using Writer = std::function<void(key_utils::ColumnId column_id,
                                  const velox::BaseVector& vector,
                                  velox::vector_size_t idx, std::string_view pk,
                                  Field& field, std::string& name_buffer)>;

}  // namespace

namespace sdb::connector {

template<velox::TypeKind Kind>
Writer AddColumnWriter(velox::VectorEncoding::Simple encoding) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  if constexpr (Kind == velox::TypeKind::VARCHAR ||
                Kind == velox::TypeKind::VARBINARY) {
    // Here copy is expensive so we care about encoding and will try to avoid it
    // whenever possible
    switch (encoding) {
      case velox::VectorEncoding::Simple::FLAT:
        return &WriteStringVector<velox::VectorEncoding::Simple::FLAT>;
      case velox::VectorEncoding::Simple::DICTIONARY:
        return &WriteStringVector<velox::VectorEncoding::Simple::DICTIONARY>;
      case velox::VectorEncoding::Simple::SEQUENCE:
        return &WriteStringVector<velox::VectorEncoding::Simple::SEQUENCE>;
      case velox::VectorEncoding::Simple::CONSTANT:
        return &WriteStringVector<velox::VectorEncoding::Simple::CONSTANT>;
      case velox::VectorEncoding::Simple::LAZY:
        return &WriteStringVector<velox::VectorEncoding::Simple::LAZY>;
      default:
        VELOX_UNSUPPORTED(
          "Search Sink does not support string vector encoding ",
          mapSimpleToName(encoding));
    }
  } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
    if (encoding == velox::VectorEncoding::Simple::FLAT) {
      return &WriteNumericVector<true, T>;
    }
    return &WriteNumericVector<false, T>;
  } else if constexpr (std::is_same_v<T, bool>) {
    return &WriteBooleanVector;
  } else {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "TypeKind ",
              velox::TypeKindName::toName(Kind),
              " is not supported in search index");
  }
}

// col_id ->  writer. column_id, vector, idx
// writers:
//.  string, numeric, boolean.
void SearchDataSink::AppendData(
  velox::RowVector& input, const std::span<key_utils::ColumnId>& column_ids,
  const primary_key::Keys& keys) {
  SDB_ASSERT(input.size() == keys.size());
  SDB_ASSERT(column_ids.size() == input.childrenSize());
  const auto num_rows = input.size();
  std::string name_buffer;
  name_buffer.reserve(sizeof(key_utils::ColumnId) + 1);  // extra space for mangling
  Field field;
  std::vector<Writer> writers;
  writers.reserve(column_ids.size());
  for (auto& column_vector : input.children()) {
    VELOX_CHECK(!isJsonType(column_vector->type()),
                "JSON type is not supported yet");

    writers.emplace_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      AddColumnWriter, column_vector->typeKind(), column_vector->encoding()));
  }
  for (velox::vector_size_t row = 0; row < num_rows; ++row) {
    // TODO(Dronplane) handle Nested/flush
    auto document = _trx.Insert();
    std::vector<char> buffer;
    for (velox::vector_size_t col = 0; col < input.childrenSize(); ++col) {
      const auto& column_vector = input.childAt(col);
      const auto column_id = column_ids[col];
      writers[col](column_id, *column_vector, row,
                   keys[row], field, name_buffer);
      document.template Insert<irs::Action::INDEX>(field);
    }
    SetNameToBuffer(name_buffer, kPkFieldName);
    field.SetPkValue(keys[row]);
    field.name = name_buffer;
    document.template Insert<irs::Action::INDEX | irs::Action::STORE>(field);
    // TODO(Dronplane) use PK as sort column
  }
}

}  // namespace sdb::connector
