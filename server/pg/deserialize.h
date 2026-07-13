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

#include <unicode/calendar.h>

#include <duckdb/common/operator/cast_operators.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "pg/serialize.h"

namespace sdb::pg {

// Lazily-built, per-context cache of a record type's field deserializers --
// the deserialize twin of serialize's TypesSerializationCache. Defined below
// (needs DeserializationFunction); held by pointer so DeserializeContext stays
// a trivial aggregate when no record types are decoded.
struct RecordDeserializers;

struct DeserializeContext {
  const catalog::Snapshot* snapshot = nullptr;
  std::unique_ptr<RecordDeserializers> record_cache;
  // Session zone for offset-less TIMESTAMPTZ text; null = UTC. Calendars are
  // mutated by FromNaive -- do not share contexts across threads.
  std::unique_ptr<icu::Calendar> session_calendar;
  // Zone names from input text, lazily resolved; unknown names cache a null.
  std::unordered_map<std::string, std::unique_ptr<icu::Calendar>>
    named_calendars;

  icu::Calendar* CalendarFor(std::string_view tz_name);
};

// Resolves the session TimeZone into `session_calendar` (null for UTC).
void FillDeserializeContext(duckdb::ClientContext& client,
                            DeserializeContext& context);

// A decoder parses the PG-wire bytes of one field and stores the result through
// a Sink. Two sinks share one set of decoders: COPY FROM decodes directly into
// the output chunk's column vectors (VectorSink), and bind parameters decode
// straight into a duckdb::Value (ValueSink) -- no per-param scratch Vector.
//
// The store primitives split into three groups: Fixed<T> covers every type
// Value::CreateValue maps faithfully (all the numerics, temporals, hugeints and
// string_t); Varchar handles plain text; and the type-specific Blob/Uuid/Bit/
// Decimal/Enum exist only because CreateValue would build the wrong Value for
// them. SetNull writes a NULL slot (nested children only).
//
// For VarFormat::Text, `data` is the already-unescaped field body: COPY text
// un-escaping happens in the copy reader and bind text values arrive raw, so
// the decoder only parses the canonical text form (no CopyText escaping
// concern).

// COPY FROM target: decode into vec[row].
struct VectorSink {
  duckdb::Vector& vec;
  duckdb::idx_t row;

  const duckdb::LogicalType& Type() const { return vec.GetType(); }

  template<typename T>
  void Fixed(T v) {
    duckdb::FlatVector::GetDataMutable<T>(vec)[row] = v;
  }

  void Varchar(std::string_view s) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
      duckdb::StringVector::AddStringOrBlob(vec, s.data(), s.size());
  }

  void Blob(duckdb::string_t v) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] = v;
  }

  // `bits` is the "0101" bit-char form; the cast packs it into the bitstring.
  bool Bit(std::string_view bits) {
    duckdb::CastParameters params(/*strict=*/true, nullptr);
    duckdb::string_t result;
    if (!duckdb::TryCastToBit::Operation(
          duckdb::string_t{bits.data(), static_cast<uint32_t>(bits.size())},
          result, vec, params)) {
      return false;
    }
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] = result;
    return true;
  }

  void Uuid(duckdb::hugeint_t v) {
    duckdb::FlatVector::GetDataMutable<duckdb::hugeint_t>(vec)[row] = v;
  }

  template<typename Phys>
  void Decimal(Phys v, uint8_t, uint8_t) {
    duckdb::FlatVector::GetDataMutable<Phys>(vec)[row] = v;
  }

  template<typename Phys>
  void Enum(Phys v) {
    duckdb::FlatVector::GetDataMutable<Phys>(vec)[row] = v;
  }

  void SetNull() { duckdb::FlatVector::SetNull(vec, row, true); }
};

// Bind parameter target: decode into a single duckdb::Value.
struct ValueSink {
  const duckdb::LogicalType& type;
  duckdb::Value& out;

  const duckdb::LogicalType& Type() const { return type; }

  template<typename T>
  void Fixed(T v) {
    out = duckdb::Value::CreateValue(v);
  }

  void Varchar(std::string_view s) { out = duckdb::Value(std::string{s}); }

  void Blob(duckdb::string_t v) {
    out = duckdb::Value::BLOB(duckdb::const_data_ptr_cast(v.GetData()),
                              v.GetSize());
  }

  // The bitstring cast needs a Vector for its heap; bind BIT is rare, so pack
  // through a one-slot scratch and lift out the Value rather than duplicating
  // the bit-packing logic non-throwing here.
  bool Bit(std::string_view bits) {
    duckdb::Vector scratch{duckdb::LogicalType::BIT, 1};
    duckdb::CastParameters params(/*strict=*/true, nullptr);
    duckdb::string_t result;
    if (!duckdb::TryCastToBit::Operation(
          duckdb::string_t{bits.data(), static_cast<uint32_t>(bits.size())},
          result, scratch, params)) {
      return false;
    }
    out = duckdb::Value::BIT(duckdb::const_data_ptr_cast(result.GetData()),
                             result.GetSize());
    return true;
  }

  void Uuid(duckdb::hugeint_t v) { out = duckdb::Value::UUID(v); }

  template<typename Phys>
  void Decimal(Phys v, uint8_t width, uint8_t scale) {
    out = duckdb::Value::DECIMAL(v, width, scale);
  }

  template<typename Phys>
  void Enum(Phys v) {
    out = duckdb::Value::ENUM(static_cast<uint64_t>(v), type);
  }

  void SetNull() { out = duckdb::Value(type); }
};

template<typename Sink>
using DeserializationFunction = bool (*)(DeserializeContext& context,
                                         std::string_view data, Sink& sink);

template<typename Sink>
DeserializationFunction<Sink> GetDeserialization(
  const duckdb::LogicalType& type, VarFormat format);

// Field deserializers of record-like types (STRUCT/MAP), cached by
// (type identity | format bit) -- a bind decodes mixed-format params, so the
// same type can need both a text and a binary decoder set. The deserialize twin
// of serialize's TypesSerializationCache; children only ever decode into
// vectors, so the value type is fixed to VectorSink.
struct RecordDeserializers {
  containers::NodeHashMap<uintptr_t,
                          std::vector<DeserializationFunction<VectorSink>>>
    fields;
};

}  // namespace sdb::pg
