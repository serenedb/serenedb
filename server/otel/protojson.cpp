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

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <simdjson.h>

#include <concepts>
#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "server/utils/simdjson_sink.h"

// OTLP/JSON:  https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
// Enum names: https://github.com/open-telemetry/opentelemetry-proto
namespace sdb::otel {
namespace {

using JsonType = utils::JsonSource::JsonType;

struct ProtoJsonArg {
  ValueArena* arena = nullptr;
  mutable std::string snake;

  std::string_view FieldName(std::string_view name) const {
    snake.clear();
    for (const char c : name) {
      if (absl::ascii_isupper(c)) {
        snake.push_back('_');
        snake.push_back(absl::ascii_tolower(c));
      } else {
        snake.push_back(c);
      }
    }
    return snake;
  }
};

template<typename Context>
concept ProtoJsonContext = std::same_as<typename Context::Arg, ProtoJsonArg> &&
                           requires(Context ctx) { ctx.io().Type(); };

[[noreturn]] void Throw(std::string_view expected) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("OTLP/JSON: expected ", expected));
}

template<typename Record>
struct SignalNames;

template<>
struct SignalNames<LogRecord> {
  static constexpr std::string_view kResources = "resource_logs";
  static constexpr std::string_view kScopes = "scope_logs";
  static constexpr std::string_view kRecords = "log_records";
};

template<>
struct SignalNames<Span> {
  static constexpr std::string_view kResources = "resource_spans";
  static constexpr std::string_view kScopes = "scope_spans";
  static constexpr std::string_view kRecords = "spans";
};

template<>
struct SignalNames<Metric> {
  static constexpr std::string_view kResources = "resource_metrics";
  static constexpr std::string_view kScopes = "scope_metrics";
  static constexpr std::string_view kRecords = "metrics";
};

template<typename E>
inline constexpr std::string_view kProtoEnumPrefix;
template<>
inline constexpr std::string_view kProtoEnumPrefix<SpanKind> = "SPAN_KIND_";
template<>
inline constexpr std::string_view kProtoEnumPrefix<StatusCode> = "STATUS_CODE_";
template<>
inline constexpr std::string_view kProtoEnumPrefix<AggregationTemporality> =
  "AGGREGATION_TEMPORALITY_";
template<>
inline constexpr std::string_view kProtoEnumPrefix<SeverityNumber> =
  "SEVERITY_NUMBER_";

template<typename Context, typename T>
void ReadField(Context ctx, T& out) {
  irs::utils::ReadObject(ctx.io(), out, ctx.arg());
}

template<typename Context, typename T, typename Extra>
void ReadFields(Context ctx, T& out, Extra extra) {
  ctx.io().ForEachObjectField([&](std::string_view name) {
    const std::string key{ctx.arg().FieldName(name)};
    if (extra(std::string_view{key})) {
      return;
    }
    boost::pfr::for_each_field_with_name(
      out, [&]<typename F>(std::string_view field, F& value) {
        if constexpr (!irs::utils::kIsVariant<F>) {
          if (field == key) {
            ReadField(ctx, value);
          }
        }
      });
  });
}

template<ProtoJsonContext Context, typename T>
  requires(std::integral<T> && !std::same_as<T, bool>)
void SerdeRead(Context ctx, T& out) {
  auto& src = ctx.io();
  if (src.Type() == JsonType::string) {
    if (!absl::SimpleAtoi(src.ReadString(), &out)) {
      Throw("an integer");
    }
    return;
  }
  if constexpr (std::is_signed_v<T>) {
    const int64_t number = src.ReadSignedInt64();
    if (number < std::numeric_limits<T>::min() ||
        number > std::numeric_limits<T>::max()) {
      Throw("an integer in range");
    }
    out = static_cast<T>(number);
  } else {
    const uint64_t number = src.ReadUnsignedInt64();
    if (number > std::numeric_limits<T>::max()) {
      Throw("an integer in range");
    }
    out = static_cast<T>(number);
  }
}

template<ProtoJsonContext Context>
void SerdeRead(Context ctx, std::string_view& out) {
  out = ctx.io().ReadStringView();
}

template<ProtoJsonContext Context>
void SerdeRead(Context ctx, bool& out) {
  auto& src = ctx.io();
  if (src.Type() != JsonType::string) {
    out = src.ReadBool();
    return;
  }
  const auto text = src.ReadString();
  if (text != "true" && text != "false") {
    Throw("a boolean");
  }
  out = text == "true";
}

template<ProtoJsonContext Context>
void SerdeRead(Context ctx, double& out) {
  auto& src = ctx.io();
  if (src.Type() != JsonType::string) {
    out = src.ReadDouble();
    return;
  }
  const auto text = src.ReadString();
  if (text == "NaN") {
    out = std::numeric_limits<double>::quiet_NaN();
  } else if (text == "Infinity") {
    out = std::numeric_limits<double>::infinity();
  } else if (text == "-Infinity") {
    out = -std::numeric_limits<double>::infinity();
  } else if (!absl::SimpleAtod(text, &out)) {
    Throw("a number");
  }
}

template<ProtoJsonContext Context, typename E>
  requires std::is_enum_v<E>
void SerdeRead(Context ctx, E& out) {
  auto& src = ctx.io();
  if (src.Type() != JsonType::string) {
    out = static_cast<E>(src.ReadSignedInt64());
    return;
  }
  const auto text = src.ReadString();
  int32_t number = 0;
  if (absl::SimpleAtoi(text, &number)) {
    out = static_cast<E>(number);
    return;
  }
  const auto name = absl::StripPrefix(text, kProtoEnumPrefix<E>);
  const auto value =
    magic_enum::enum_cast<E>(name, magic_enum::case_insensitive);
  if (!value) {
    Throw(absl::StrCat("a ", magic_enum::enum_type_name<E>(), " name"));
  }
  out = *value;
}

template<ProtoJsonContext Context, size_t HexLength>
void SerdeRead(Context ctx, HexId<HexLength>& out) {
  auto text = ctx.io().ReadString();
  if (text.empty()) {
    out.hex.clear();
    return;
  }
  if (text.size() != HexLength) {
    Throw("a hex string of the declared width");
  }
  absl::AsciiStrToLower(&text);
  if (text.find_first_not_of("0123456789abcdef") != std::string::npos) {
    Throw("a hex string");
  }
  if (text.find_first_not_of('0') == std::string::npos) {
    text.clear();
  }
  out.hex = std::move(text);
}

template<ProtoJsonContext Context>
void SerdeRead(Context ctx, AnyValue*& out) {
  out = ctx.arg().arena->Make();
  auto& src = ctx.io();
  src.ForEachObjectField([&](std::string_view name) {
    const std::string key{ctx.arg().FieldName(name)};
    const auto read = [&]<typename T>(T value) {
      ReadField(ctx, value);
      out->value = std::move(value);
    };
    if (key == "string_value") {
      read(std::string_view{});
    } else if (key == "bool_value") {
      read(bool{});
    } else if (key == "int_value") {
      read(int64_t{});
    } else if (key == "double_value") {
      read(double{});
    } else if (key == "bytes_value") {
      out->value = BytesValue{.data = src.ReadString()};
    } else if (key == "array_value") {
      read(ArrayValue{});
    } else if (key == "kvlist_value") {
      read(KvlistValue{});
    }
  });
}

template<ProtoJsonContext Context, typename Point>
  requires(std::same_as<Point, NumberDataPoint> ||
           std::same_as<Point, Exemplar>)
void SerdeRead(Context ctx, Point& out) {
  ReadFields(ctx, out, [&](std::string_view key) {
    if (key == "as_int") {
      int64_t number = 0;
      ReadField(ctx, number);
      out.value = number;
      return true;
    }
    if (key == "as_double") {
      double number = 0;
      ReadField(ctx, number);
      out.value = number;
      return true;
    }
    return false;
  });
}

template<ProtoJsonContext Context>
void SerdeRead(Context ctx, Metric& out) {
  const auto shape = [&]<typename Shape>(std::type_identity<Shape>) {
    ReadField(ctx, out.data.template emplace<Shape>());
    return true;
  };
  ReadFields(ctx, out, [&](std::string_view key) {
    if (key == "gauge") {
      return shape(std::type_identity<Gauge>{});
    }
    if (key == "sum") {
      return shape(std::type_identity<Sum>{});
    }
    if (key == "histogram") {
      return shape(std::type_identity<Histogram>{});
    }
    if (key == "exponential_histogram") {
      return shape(std::type_identity<ExponentialHistogram>{});
    }
    if (key == "summary") {
      return shape(std::type_identity<Summary>{});
    }
    return false;
  });
}

template<ProtoJsonContext Context, typename Record>
void SerdeRead(Context ctx, ScopeRecords<Record>& out) {
  ReadFields(ctx, out, [&](std::string_view key) {
    if (key != SignalNames<Record>::kRecords) {
      return false;
    }
    ReadField(ctx, out.records);
    return true;
  });
}

template<ProtoJsonContext Context, typename Record>
void SerdeRead(Context ctx, ResourceRecords<Record>& out) {
  ReadFields(ctx, out, [&](std::string_view key) {
    if (key != SignalNames<Record>::kScopes) {
      return false;
    }
    ReadField(ctx, out.scopes);
    return true;
  });
}

template<typename Record>
void ParseRequest(std::string_view json, bool padded,
                  ExportRequest<Record>& out) {
  // The model's text fields point into the parser's string buffer.
  auto parser = std::make_shared<simdjson::ondemand::parser>();
  out.storage = parser;
  simdjson::padded_string copy;
  simdjson::padded_string_view input;
  if (padded) {
    input = simdjson::padded_string_view{json.data(), json.size(),
                                         json.size() + kJsonPadding};
  } else {
    copy = simdjson::padded_string{json};
    input = copy;
  }
  simdjson::ondemand::document doc;
  if (const auto ec = parser->iterate(input).get(doc);
      ec != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("OTLP/JSON: ", simdjson::error_message(ec)));
  }
  simdjson::ondemand::json_type type;
  if (doc.type().get(type) != simdjson::SUCCESS ||
      type != simdjson::ondemand::json_type::object) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("OTLP/JSON: field [<root>] must be an object"));
  }
  try {
    utils::JsonSource src{doc};
    const ProtoJsonArg arg{.arena = &out.arena};
    src.ForEachObjectField([&](std::string_view name) {
      if (arg.FieldName(name) == SignalNames<Record>::kResources) {
        irs::utils::ReadObject(src, out.resources, arg);
      }
    });
  } catch (const std::exception& error) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("OTLP/JSON: ", error.what()));
  }
}

}  // namespace

static_assert(kJsonPadding >= simdjson::SIMDJSON_PADDING);

void ParseLogsRequest(std::string_view json, ExportLogsRequest& out,
                      bool padded) {
  ParseRequest(json, padded, out);
}

void ParseTracesRequest(std::string_view json, ExportTracesRequest& out,
                        bool padded) {
  ParseRequest(json, padded, out);
}

void ParseMetricsRequest(std::string_view json, ExportMetricsRequest& out,
                         bool padded) {
  ParseRequest(json, padded, out);
}

}  // namespace sdb::otel
