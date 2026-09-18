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

#include "otel/protojson.h"

#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <simdjson.h>

#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <string>
#include <utility>

// ProtoJSON, read straight into the generated messages through protobuf
// reflection: the field set comes from the descriptors, so a field added to
// opentelemetry-proto is picked up by regenerating, not by editing this file.
//
// Only OTLP's three departures from the ProtoJSON rules are spelled out here,
// each in one place:
//
//   * 64-bit integers arrive as decimal strings (and, from hand-written
//     payloads, as bare numbers)
//   * enums arrive as their proto value name or as an integer
//   * trace and span ids are hex, where the bytes rule would say base64
//
// ProtoJSON:  https://protobuf.dev/programming-guides/json/
// OTLP/JSON:  https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
namespace sdb::otel {
namespace {

namespace gp = ::google::protobuf;

using Value = simdjson::ondemand::value;
using Object = simdjson::ondemand::object;

[[noreturn]] void Throw(std::string_view field, std::string_view expected) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("OTLP/JSON: field [", field, "] must be ", expected));
}

std::string_view ReadString(Value value, std::string_view field) {
  std::string_view text;
  if (value.get_string().get(text) != simdjson::SUCCESS) {
    Throw(field, "a string");
  }
  return text;
}

bool ReadBool(Value value, std::string_view field) {
  bool flag = false;
  if (value.get_bool().get(flag) == simdjson::SUCCESS) {
    return flag;
  }
  std::string_view text;
  if (value.get_string().get(text) == simdjson::SUCCESS) {
    if (text == "true") {
      return true;
    }
    if (text == "false") {
      return false;
    }
  }
  Throw(field, "a boolean");
}

template<typename Integer, typename Wide>
Integer Narrow(Wide number, std::string_view field) {
  if (!std::in_range<Integer>(number)) {
    Throw(field, "an integer in range");
  }
  return static_cast<Integer>(number);
}

// int32/int64/uint32/uint64 all arrive either as a JSON number or, per
// ProtoJSON's 64-bit rule, as a decimal string.
template<typename Integer>
Integer ReadInteger(Value value, std::string_view field) {
  if (uint64_t number = 0;
      value.get_uint64().get(number) == simdjson::SUCCESS) {
    return Narrow<Integer>(number, field);
  }
  if (int64_t number = 0; value.get_int64().get(number) == simdjson::SUCCESS) {
    return Narrow<Integer>(number, field);
  }
  std::string_view text;
  if (Integer number = 0; value.get_string().get(text) == simdjson::SUCCESS &&
                          absl::SimpleAtoi(text, &number)) {
    return number;
  }
  Throw(field, "an integer");
}

double ReadDouble(Value value, std::string_view field) {
  double number = 0;
  if (value.get_double().get(number) == simdjson::SUCCESS) {
    return number;
  }
  std::string_view text;
  if (value.get_string().get(text) == simdjson::SUCCESS) {
    if (text == "NaN") {
      return std::numeric_limits<double>::quiet_NaN();
    }
    if (text == "Infinity") {
      return std::numeric_limits<double>::infinity();
    }
    if (text == "-Infinity") {
      return -std::numeric_limits<double>::infinity();
    }
    if (absl::SimpleAtod(text, &number)) {
      return number;
    }
  }
  Throw(field, "a number");
}

int32_t ReadEnum(Value value, const gp::FieldDescriptor& field) {
  std::string_view text;
  if (value.get_string().get(text) != simdjson::SUCCESS) {
    return ReadInteger<int32_t>(value, field.name());
  }
  if (const auto* named = field.enum_type()->FindValueByName(text)) {
    return named->number();
  }
  int32_t parsed = 0;
  if (absl::SimpleAtoi(text, &parsed)) {
    return parsed;
  }
  Throw(field.name(), "a known enum name or an integer");
}

// "The traceId and spanId byte arrays are represented as case-insensitive
// hex-encoded strings; they are not base64-encoded as is defined in the
// standard Protobuf JSON Mapping."
// https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
//
// The reference implementation reads it the same way, and applies it to every
// field of those types -- parent_span_id included, which the spec text does
// not name:
//   MarshalJSON converts TraceID into a hex string / UnmarshalJSON decodes
//   TraceID from hex string
// https://github.com/open-telemetry/opentelemetry-collector/blob/main/pdata/internal/traceid.go
// https://github.com/open-telemetry/opentelemetry-collector/blob/main/pdata/internal/spanid.go
//
// The spec says nothing about the other bytes fields (AnyValue.bytes_value),
// so those keep the standard mapping's base64:
// https://protobuf.dev/programming-guides/json/
bool IsHexIdField(const gp::FieldDescriptor& field) {
  const auto& name = field.name();
  return name == "trace_id" || name == "span_id" || name == "parent_span_id";
}

std::string ReadBytes(Value value, const gp::FieldDescriptor& field) {
  const auto text = ReadString(value, field.name());
  const bool hex = IsHexIdField(field);
  std::string raw;
  if (!(hex ? absl::HexStringToBytes(text, &raw)
            : absl::Base64Unescape(text, &raw))) {
    Throw(field.name(), hex ? "a hex string" : "base64");
  }
  return raw;
}

void ParseMessage(Value value, gp::Message& out);

// Reflection spells the singular and repeated writers as separate members, so
// which pair to use is the only thing that varies per field type.
template<typename T>
using Writer = void (gp::Reflection::*)(gp::Message*,
                                        const gp::FieldDescriptor*, T) const;

template<typename T>
void Store(gp::Message& out, const gp::FieldDescriptor& field, bool repeated,
           T value, Writer<T> set, Writer<T> add) {
  const auto* reflection = out.GetReflection();
  const auto writer = repeated ? add : set;
  (reflection->*writer)(&out, &field, value);
}

// One field occurrence: the singular value, or one element of a repeated one.
void ParseSingular(Value value, gp::Message& out,
                   const gp::FieldDescriptor& field, bool repeated) {
  using Reflect = gp::Reflection;
  const auto& name = field.name();
  switch (field.cpp_type()) {
    case gp::FieldDescriptor::CPPTYPE_INT32:
      Store(out, field, repeated, ReadInteger<int32_t>(value, name),
            &Reflect::SetInt32, &Reflect::AddInt32);
      return;
    case gp::FieldDescriptor::CPPTYPE_INT64:
      Store(out, field, repeated, ReadInteger<int64_t>(value, name),
            &Reflect::SetInt64, &Reflect::AddInt64);
      return;
    case gp::FieldDescriptor::CPPTYPE_UINT32:
      Store(out, field, repeated, ReadInteger<uint32_t>(value, name),
            &Reflect::SetUInt32, &Reflect::AddUInt32);
      return;
    case gp::FieldDescriptor::CPPTYPE_UINT64:
      Store(out, field, repeated, ReadInteger<uint64_t>(value, name),
            &Reflect::SetUInt64, &Reflect::AddUInt64);
      return;
    case gp::FieldDescriptor::CPPTYPE_DOUBLE:
      Store(out, field, repeated, ReadDouble(value, name), &Reflect::SetDouble,
            &Reflect::AddDouble);
      return;
    case gp::FieldDescriptor::CPPTYPE_FLOAT:
      Store(out, field, repeated, static_cast<float>(ReadDouble(value, name)),
            &Reflect::SetFloat, &Reflect::AddFloat);
      return;
    case gp::FieldDescriptor::CPPTYPE_BOOL:
      Store(out, field, repeated, ReadBool(value, name), &Reflect::SetBool,
            &Reflect::AddBool);
      return;
    case gp::FieldDescriptor::CPPTYPE_ENUM:
      Store(out, field, repeated, ReadEnum(value, field),
            &Reflect::SetEnumValue, &Reflect::AddEnumValue);
      return;
    case gp::FieldDescriptor::CPPTYPE_STRING: {
      // SetString/AddString are overloaded, so they do not go through Store.
      auto text = field.type() == gp::FieldDescriptor::TYPE_BYTES
                    ? ReadBytes(value, field)
                    : std::string{ReadString(value, name)};
      const auto* reflection = out.GetReflection();
      if (repeated) {
        reflection->AddString(&out, &field, std::move(text));
      } else {
        reflection->SetString(&out, &field, std::move(text));
      }
      return;
    }
    case gp::FieldDescriptor::CPPTYPE_MESSAGE: {
      const auto* reflection = out.GetReflection();
      ParseMessage(value, repeated ? *reflection->AddMessage(&out, &field)
                                   : *reflection->MutableMessage(&out, &field));
      return;
    }
  }
  Throw(name, "a supported type");
}

void ParseMessage(Value value, gp::Message& out) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw(out.GetDescriptor()->name(), "an object");
  }
  const auto* descriptor = out.GetDescriptor();
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw(descriptor->name(), "an object");
    }
    // Senders use either the ProtoJSON lowerCamelCase name or the original
    // snake_case one; unknown fields are skipped, as ProtoJSON allows.
    const auto* field = descriptor->FindFieldByCamelcaseName(key);
    if (field == nullptr) {
      field = descriptor->FindFieldByName(key);
    }
    if (field == nullptr) {
      continue;
    }
    auto field_value = member.value().value();
    if (!field->is_repeated()) {
      ParseSingular(field_value, out, *field, /*repeated=*/false);
      continue;
    }
    simdjson::ondemand::array items;
    if (field_value.get_array().get(items) != simdjson::SUCCESS) {
      Throw(field->name(), "an array");
    }
    for (auto item : items) {
      ParseSingular(item.value(), out, *field, /*repeated=*/true);
    }
  }
}

template<typename Request>
void ParseRequest(std::string_view json, Request& out) {
  simdjson::ondemand::parser parser;
  simdjson::padded_string padded{json};
  simdjson::ondemand::document doc;
  if (const auto ec = parser.iterate(padded).get(doc);
      ec != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("OTLP/JSON: ", simdjson::error_message(ec)));
  }
  simdjson::ondemand::value root;
  if (doc.get_value().get(root) != simdjson::SUCCESS) {
    Throw("<root>", "an object");
  }
  ParseMessage(root, out);
}

}  // namespace

void ParseLogsRequest(std::string_view json, ExportLogsRequest& out) {
  ParseRequest(json, out);
}

void ParseTracesRequest(std::string_view json, ExportTracesRequest& out) {
  ParseRequest(json, out);
}

void ParseMetricsRequest(std::string_view json, ExportMetricsRequest& out) {
  ParseRequest(json, out);
}

}  // namespace sdb::otel
