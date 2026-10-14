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

#include "connector/curve_index.h"

#include <absl/strings/ascii.h>

#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/pg_logical_types.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kMortonCurve = "morton";
constexpr std::string_view kHilbertCurve = "hilbert";

enum CurveOption : size_t {
  kKindOption,
  kMaxLevelOption,
  kLevelStepOption,
  kMaxCellsOption,
  kLowerOption,
  kUpperOption,
  kOptionCount,
};

constexpr std::string_view kCoordinateTypes =
  "an integer, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMPTZ or INET";

constexpr uint8_t kInetV4 = 1;
constexpr uint8_t kInetV6 = 2;

std::optional<CurveFamily> CoordinateFamily(const duckdb::LogicalType& type) {
  using enum duckdb::LogicalTypeId;
  if (pg::IsInet(type)) {
    return CurveFamily::Inet;
  }
  switch (type.id()) {
    case SQLNULL:
      return CurveFamily::Null;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return CurveFamily::Signed;
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
      return CurveFamily::Unsigned;
    case FLOAT:
    case DOUBLE:
      return CurveFamily::Float;
    case DATE:
      return CurveFamily::Date;
    case TIMESTAMP:
      return CurveFamily::Timestamp;
    case TIMESTAMP_TZ:
      return CurveFamily::TimestampTz;
    default:
      return std::nullopt;
  }
}

uint32_t IntegerWidth(duckdb::LogicalTypeId type) {
  using enum duckdb::LogicalTypeId;
  switch (type) {
    case TINYINT:
    case UTINYINT:
      return 8;
    case SMALLINT:
    case USMALLINT:
      return 16;
    case INTEGER:
    case UINTEGER:
    case DATE:
      return 32;
    default:
      return 64;
  }
}

uint64_t SignedKey(int64_t value, uint32_t width) noexcept {
  return (static_cast<uint64_t>(value) + (uint64_t{1} << (width - 1)))
         << (64 - width);
}

uint64_t UnsignedKey(uint64_t value, uint32_t width) noexcept {
  return value << (64 - width);
}

uint32_t KeyWidth(const CurveAxis& axis) noexcept {
  switch (axis.family) {
    case CurveFamily::Signed:
    case CurveFamily::Unsigned:
    case CurveFamily::Date:
      return IntegerWidth(axis.type.id());
    default:
      return 64;
  }
}

uint32_t KeyShift(const CurveAxis& axis) noexcept {
  if (axis.family == CurveFamily::Inet) {
    return axis.normalized && axis.key_hi < irs::curve::kSignBit ? 31 : 0;
  }
  return 64 - KeyWidth(axis);
}

uint32_t AxisBits(const CurveAxis& axis) noexcept {
  if (axis.family == CurveFamily::Null) {
    return 0;
  }
  if (!axis.normalized) {
    return 64 - KeyShift(axis);
  }
  if (axis.ipv6_range) {
    const auto span = axis.ipv6_hi - axis.ipv6_lo;
    return absl::Uint128High64(span) == 0
             ? std::bit_width(absl::Uint128Low64(span))
             : 64;
  }
  if (axis.family == CurveFamily::Float) {
    return 64;
  }
  return std::bit_width(axis.key_hi - axis.key_lo) - KeyShift(axis);
}

uint64_t InetKey(uint8_t ip_type, duckdb::hugeint_t address) noexcept {
  if (ip_type == kInetV6) {
    return irs::curve::kSignBit |
           ((static_cast<uint64_t>(address.upper) ^ irs::curve::kSignBit) >> 1);
  }
  if (ip_type == kInetV4) {
    return address.lower << 31;
  }
  return 0;
}

struct InetValue {
  uint8_t ip_type = 0;
  duckdb::hugeint_t address;
};

std::optional<InetValue> ToInet(const duckdb::Value& bound) {
  if (!pg::IsInet(bound.type())) {
    return std::nullopt;
  }
  const auto& parts = duckdb::StructValue::GetChildren(bound);
  if (parts[0].IsNull() || parts[1].IsNull()) {
    return std::nullopt;
  }
  return InetValue{.ip_type = parts[0].GetValue<uint8_t>(),
                   .address = parts[1].GetValue<duckdb::hugeint_t>()};
}

absl::uint128 Ipv6Address(duckdb::hugeint_t address) noexcept {
  return absl::MakeUint128(
    static_cast<uint64_t>(address.upper) ^ irs::curve::kSignBit, address.lower);
}

uint64_t Ipv6RangeKey(const CurveAxis& axis, InetValue value) noexcept {
  if (value.ip_type != kInetV6) {
    return 0;
  }
  const auto offset =
    std::clamp(Ipv6Address(value.address), axis.ipv6_lo, axis.ipv6_hi) -
    axis.ipv6_lo;
  const auto span = axis.ipv6_hi - axis.ipv6_lo;
  const auto high = absl::Uint128High64(span);
  if (high == 0) {
    return absl::Uint128Low64(offset)
           << std::countl_zero(absl::Uint128Low64(span));
  }
  return absl::Uint128Low64(offset >> (64 - std::countl_zero(high)));
}

struct RawBound {
  uint64_t key = 0;
  double value = 0;
  bool exact = false;
};

bool IsSignedInteger(duckdb::LogicalTypeId type) {
  using enum duckdb::LogicalTypeId;
  return type == TINYINT || type == SMALLINT || type == INTEGER ||
         type == BIGINT;
}

bool IsUnsignedInteger(duckdb::LogicalTypeId type) {
  using enum duckdb::LogicalTypeId;
  return type == UTINYINT || type == USMALLINT || type == UINTEGER ||
         type == UBIGINT;
}

std::optional<RawBound> ToRaw(const CurveAxis& axis,
                              const duckdb::Value& bound) {
  using enum duckdb::LogicalTypeId;
  const auto type = bound.type().id();
  const bool is_signed = IsSignedInteger(type);
  const bool is_unsigned = IsUnsignedInteger(type);
  const auto width = KeyWidth(axis);
  switch (axis.family) {
    case CurveFamily::Signed: {
      const auto max = std::numeric_limits<int64_t>::max() >> (64 - width);
      if (is_signed) {
        const auto value = bound.GetValue<int64_t>();
        const auto clamped = std::clamp(value, -max - 1, max);
        return RawBound{.key = SignedKey(clamped, width),
                        .exact = clamped == value};
      }
      if (is_unsigned) {
        const auto value = bound.GetValue<uint64_t>();
        const auto limit = static_cast<uint64_t>(max);
        return RawBound{
          .key = SignedKey(static_cast<int64_t>(std::min(value, limit)), width),
          .exact = value <= limit};
      }
      return std::nullopt;
    }
    case CurveFamily::Unsigned: {
      const auto max = std::numeric_limits<uint64_t>::max() >> (64 - width);
      if (is_unsigned) {
        const auto value = bound.GetValue<uint64_t>();
        return RawBound{.key = UnsignedKey(std::min(value, max), width),
                        .exact = value <= max};
      }
      if (is_signed) {
        const auto value = bound.GetValue<int64_t>();
        const auto clamped =
          value < 0 ? 0 : std::min(static_cast<uint64_t>(value), max);
        return RawBound{.key = UnsignedKey(clamped, width),
                        .exact = static_cast<uint64_t>(value) == clamped};
      }
      return std::nullopt;
    }
    case CurveFamily::Float:
      if (type == FLOAT || type == DOUBLE) {
        return RawBound{.value = bound.GetValue<double>(), .exact = true};
      }
      if (is_signed) {
        const auto value = bound.GetValue<int64_t>();
        const auto converted = static_cast<double>(value);
        return RawBound{.value = converted,
                        .exact = converted >= -0x1p63 && converted < 0x1p63 &&
                                 static_cast<int64_t>(converted) == value};
      }
      if (is_unsigned) {
        const auto value = bound.GetValue<uint64_t>();
        const auto converted = static_cast<double>(value);
        return RawBound{.value = converted,
                        .exact = converted < 0x1p64 &&
                                 static_cast<uint64_t>(converted) == value};
      }
      if (type == DECIMAL) {
        return RawBound{.value =
                          bound.DefaultCastAs(duckdb::LogicalType::DOUBLE)
                            .GetValue<double>()};
      }
      return std::nullopt;
    case CurveFamily::Date:
      if (type != DATE) {
        return std::nullopt;
      }
      return RawBound{
        .key = SignedKey(bound.GetValue<duckdb::date_t>().days, width),
        .exact = true};
    case CurveFamily::Timestamp:
      if (type != TIMESTAMP) {
        return std::nullopt;
      }
      return RawBound{.key = irs::curve::EncodeSigned(
                        bound.GetValue<duckdb::timestamp_t>().value),
                      .exact = true};
    case CurveFamily::TimestampTz:
      if (type != TIMESTAMP_TZ) {
        return std::nullopt;
      }
      return RawBound{.key = irs::curve::EncodeSigned(
                        bound.GetValue<duckdb::timestamp_tz_t>().value),
                      .exact = true};
    case CurveFamily::Inet: {
      const auto inet = ToInet(bound);
      if (!inet) {
        return std::nullopt;
      }
      return RawBound{.key = InetKey(inet->ip_type, inet->address)};
    }
    case CurveFamily::Null:
      return std::nullopt;
  }
  return std::nullopt;
}

uint64_t MapKey(const CurveAxis& axis, uint64_t key) noexcept {
  return axis.normalized
           ? irs::curve::NormalizeKey(key, axis.key_lo, axis.key_hi)
           : key;
}

uint64_t MapDouble(const CurveAxis& axis, double value) noexcept {
  return axis.normalized ? irs::curve::NormalizeDouble(value, axis.lo, axis.hi)
                         : irs::curve::EncodeDouble(value);
}

uint64_t InetAxisKey(const CurveAxis& axis, InetValue value) noexcept {
  return axis.ipv6_range ? Ipv6RangeKey(axis, value)
                         : MapKey(axis, InetKey(value.ip_type, value.address));
}

template<typename T>
T Load(const duckdb::UnifiedVectorFormat& format, duckdb::idx_t index) {
  return duckdb::UnifiedVectorFormat::GetDataUnsafe<T>(format)[index];
}

bool IsValid(const CurveAxis& axis,
             const duckdb::RecursiveUnifiedVectorFormat& format,
             duckdb::idx_t row) {
  const auto& unified = format.unified;
  if (!unified.validity.RowIsValid(unified.sel->get_index(row))) {
    return false;
  }
  if (axis.family != CurveFamily::Inet) {
    return true;
  }
  for (const auto& child : {&format.children[0], &format.children[1]}) {
    const auto& part = child->unified;
    if (!part.validity.RowIsValid(part.sel->get_index(row))) {
      return false;
    }
  }
  return true;
}

uint64_t AxisKey(const CurveAxis& axis,
                 const duckdb::RecursiveUnifiedVectorFormat& format,
                 duckdb::idx_t row) {
  using enum duckdb::LogicalTypeId;
  const auto& unified = format.unified;
  const auto index = unified.sel->get_index(row);
  const auto signed_key = [&](int64_t value, uint32_t width) {
    return MapKey(axis, SignedKey(value, width));
  };
  const auto unsigned_key = [&](uint64_t value, uint32_t width) {
    return MapKey(axis, UnsignedKey(value, width));
  };
  if (axis.family == CurveFamily::Inet) {
    const auto& type = format.children[0].unified;
    const auto& address = format.children[1].unified;
    return InetAxisKey(
      axis, {.ip_type = Load<uint8_t>(type, type.sel->get_index(row)),
             .address =
               Load<duckdb::hugeint_t>(address, address.sel->get_index(row))});
  }
  switch (axis.type.id()) {
    case TINYINT:
      return signed_key(Load<int8_t>(unified, index), 8);
    case SMALLINT:
      return signed_key(Load<int16_t>(unified, index), 16);
    case INTEGER:
    case DATE:
      return signed_key(Load<int32_t>(unified, index), 32);
    case BIGINT:
    case TIMESTAMP:
    case TIMESTAMP_TZ:
      return signed_key(Load<int64_t>(unified, index), 64);
    case UTINYINT:
      return unsigned_key(Load<uint8_t>(unified, index), 8);
    case USMALLINT:
      return unsigned_key(Load<uint16_t>(unified, index), 16);
    case UINTEGER:
      return unsigned_key(Load<uint32_t>(unified, index), 32);
    case UBIGINT:
      return unsigned_key(Load<uint64_t>(unified, index), 64);
    case FLOAT:
      return MapDouble(axis, Load<float>(unified, index));
    case DOUBLE:
      return MapDouble(axis, Load<double>(unified, index));
    default:
      SDB_ASSERT(false);
      return 0;
  }
}

void ReplaceOption(duckdb::unique_ptr<duckdb::Expression>& argument,
                   duckdb::Value value) {
  auto alias = argument->GetAlias();
  argument =
    duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(value));
  argument->SetAlias(std::move(alias));
}

duckdb::Value OptionValue(duckdb::ClientContext& context,
                          duckdb::unique_ptr<duckdb::Expression>& argument,
                          std::string_view name) {
  if (!argument->IsFoldable() || argument->HasParameter()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("curve(): option \"", name, "\" must be a constant"));
  }
  auto value = duckdb::ExpressionExecutor::EvaluateScalar(context, *argument);
  ReplaceOption(argument, value);
  return value;
}

uint32_t LimitOption(const duckdb::Value& value, std::string_view name,
                     uint32_t min, uint32_t max) {
  if (!value.IsNull()) {
    const auto n = value.GetValue<int64_t>();
    if (n >= min && n <= max) {
      return static_cast<uint32_t>(n);
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("curve(): option \"", name, "\" must be between ",
                          min, " and ", max, ", got ", value.ToString()));
}

const duckdb::Value* BoundComponent(const duckdb::Value& bounds,
                                    std::string_view name, uint32_t axis,
                                    uint32_t dimensions) {
  if (bounds.IsNull()) {
    return nullptr;
  }
  if (!duckdb::StructType::IsStruct(bounds.type()) ||
      pg::IsInet(bounds.type()) ||
      duckdb::StructType::GetChildCount(bounds.type()) != dimensions) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG("curve(): option \"", name, "\" must be a ROW of ", dimensions,
              " values, got ", bounds.type().ToString()));
  }
  const auto& component = duckdb::StructValue::GetChildren(bounds)[axis];
  return component.IsNull() ? nullptr : &component;
}

[[noreturn]] void ThrowBoundMismatch(std::string_view what,
                                     const duckdb::Value& bound, uint32_t axis,
                                     const CurveAxis& target) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
    ERR_MSG("curve(): ", what, " ", bound.type().ToString(),
            " does not match coordinate ", axis + 1, " of type ",
            target.type.ToString(), "; cast the bound explicitly"));
}

[[noreturn]] void ThrowBadBounds(duckdb::ClientContext& context, uint32_t index,
                                 std::string_view need,
                                 const duckdb::Value& lower,
                                 const duckdb::Value& upper) {
  const auto text = [&](const duckdb::Value& value) {
    return value.CastAs(context, duckdb::LogicalType::VARCHAR).ToString();
  };
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("curve(): coordinate ", index + 1, " needs ", need,
                          ", got ", text(lower), " and ", text(upper)));
}

constexpr std::string_view kInetBoundsNeed =
  "INET bounds with lower < upper on the curve; masks do not change the "
  "position";

void Normalize(duckdb::ClientContext& context, CurveAxis& axis, uint32_t index,
               const duckdb::Value* lower, const duckdb::Value* upper) {
  if (!lower && !upper) {
    return;
  }
  if (!lower || !upper) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("curve(): \"lower\" and \"upper\" must both bound coordinate ",
              index + 1, " or both leave it NULL"));
  }
  if (axis.family == CurveFamily::Inet) {
    const auto lo = ToInet(*lower);
    const auto hi = ToInet(*upper);
    if (lo && hi && lo->ip_type == kInetV6 && hi->ip_type == kInetV6) {
      axis.ipv6_lo = Ipv6Address(lo->address);
      axis.ipv6_hi = Ipv6Address(hi->address);
      if (axis.ipv6_lo >= axis.ipv6_hi) {
        ThrowBadBounds(context, index, kInetBoundsNeed, *lower, *upper);
      }
      axis.normalized = true;
      axis.ipv6_range = true;
      return;
    }
  }
  const auto lo = ToRaw(axis, *lower);
  if (!lo) {
    ThrowBoundMismatch("lower bound", *lower, index, axis);
  }
  const auto hi = ToRaw(axis, *upper);
  if (!hi) {
    ThrowBoundMismatch("upper bound", *upper, index, axis);
  }
  if (axis.family == CurveFamily::Float) {
    if (!std::isfinite(lo->value) || !std::isfinite(hi->value) ||
        !(lo->value < hi->value)) {
      ThrowBadBounds(context, index, "finite bounds with lower < upper", *lower,
                     *upper);
    }
    if (!std::isfinite(hi->value - lo->value)) {
      ThrowBadBounds(context, index, "bounds with a finite difference", *lower,
                     *upper);
    }
  } else if (lo->key >= hi->key) {
    ThrowBadBounds(context, index,
                   axis.family == CurveFamily::Inet
                     ? kInetBoundsNeed
                     : std::string_view{"bounds with lower < upper"},
                   *lower, *upper);
  }
  axis.normalized = true;
  axis.key_lo = lo->key;
  axis.key_hi = hi->key;
  axis.lo = lo->value;
  axis.hi = hi->value;
}

}  // namespace

std::optional<CurveKey> CurveBoundKey(const CurveAxis& axis,
                                      const duckdb::Value& bound) {
  if (axis.family == CurveFamily::Inet) {
    const auto inet = ToInet(bound);
    if (!inet) {
      return std::nullopt;
    }
    return CurveKey{.key = InetAxisKey(axis, *inet)};
  }
  const auto raw = ToRaw(axis, bound);
  if (!raw) {
    return std::nullopt;
  }
  const bool injective = !axis.normalized && axis.family != CurveFamily::Inet;
  return CurveKey{.key = axis.family == CurveFamily::Float
                           ? MapDouble(axis, raw->value)
                           : MapKey(axis, raw->key),
                  .step = uint64_t{1} << KeyShift(axis),
                  .exact = raw->exact && injective};
}

CurveInterval CurveLimits(const CurveAxis& axis) noexcept {
  constexpr auto kMax = std::numeric_limits<uint64_t>::max();
  if (axis.normalized) {
    return {.max = kMax};
  }
  if (axis.family == CurveFamily::Float) {
    return {
      .min = irs::curve::EncodeDouble(-std::numeric_limits<double>::infinity()),
      .max = irs::curve::EncodeDouble(std::numeric_limits<double>::quiet_NaN()),
    };
  }
  return {.max = kMax << KeyShift(axis)};
}

duckdb::unique_ptr<duckdb::FunctionData> CurveSpec::Copy() const {
  return duckdb::make_uniq<CurveSpec>(*this);
}

bool CurveSpec::Equals(const duckdb::FunctionData& other) const {
  const auto& spec = other.Cast<CurveSpec>();
  return options == spec.options && axes == spec.axes;
}

duckdb::unique_ptr<CurveSpec> BindCurveSpec(
  duckdb::ClientContext& context,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& args) {
  SDB_ASSERT(args.size() >= kOptionCount);
  const auto dimensions = args.size() - kOptionCount;
  if (dimensions < 2 || dimensions > irs::curve::kMaxDimensions) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("curve(): takes 2 to ", irs::curve::kMaxDimensions,
                            " coordinates, got ", dimensions));
  }
  auto spec = duckdb::make_uniq<CurveSpec>();
  auto& options = spec->options;
  options.dimensions = static_cast<uint32_t>(dimensions);
  for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
    const auto& type = args[axis]->GetReturnType();
    const auto family = CoordinateFamily(type);
    if (!family) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                      ERR_MSG("curve(): coordinate ", axis + 1, " must be ",
                              kCoordinateTypes, ", got ", type.ToString()));
    }
    spec->axes[axis].type = type;
    spec->axes[axis].family = *family;
  }
  const auto option = [&](CurveOption index, std::string_view name) {
    return OptionValue(context, args[dimensions + index], name);
  };

  const auto kind = option(kKindOption, "kind");
  const auto curve = kind.IsNull()
                       ? std::string{kMortonCurve}
                       : absl::AsciiStrToLower(duckdb::StringValue::Get(kind));
  if (curve != kMortonCurve && curve != kHilbertCurve) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("curve(): option \"kind\" must be '", kMortonCurve,
                            "' or '", kHilbertCurve, "', got '", curve, "'"));
  }
  options.hilbert = curve == kHilbertCurve;
  const auto max_level = option(kMaxLevelOption, "max_level");
  if (!max_level.IsNull()) {
    options.max_level =
      LimitOption(max_level, "max_level", 0, irs::curve::kMaxLevel);
  }
  const auto step = option(kLevelStepOption, "level_step");
  options.level_step =
    step.IsNull() ? irs::curve::DefaultLevelStep(options.dimensions)
                  : LimitOption(step, "level_step", 1,
                                irs::curve::MaxLevelStep(options.dimensions));
  const auto cells = option(kMaxCellsOption, "max_cells");
  options.max_cells =
    cells.IsNull() ? irs::curve::DefaultMaxCells(options.dimensions)
                   : LimitOption(cells, "max_cells", 1, irs::curve::kMaxCells);

  const auto lower = option(kLowerOption, "lower");
  const auto upper = option(kUpperOption, "upper");
  bool normalized = false;
  for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
    Normalize(context, spec->axes[axis], axis,
              BoundComponent(lower, "lower", axis, options.dimensions),
              BoundComponent(upper, "upper", axis, options.dimensions));
    normalized |= spec->axes[axis].normalized;
  }
  if (max_level.IsNull()) {
    options.max_level = 0;
    for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
      options.max_level =
        std::max(options.max_level, AxisBits(spec->axes[axis]));
    }
  }
  ReplaceOption(args[dimensions + kKindOption], duckdb::Value{curve});
  ReplaceOption(
    args[dimensions + kMaxLevelOption],
    duckdb::Value::INTEGER(static_cast<int32_t>(options.max_level)));
  ReplaceOption(
    args[dimensions + kLevelStepOption],
    duckdb::Value::INTEGER(static_cast<int32_t>(options.level_step)));
  ReplaceOption(
    args[dimensions + kMaxCellsOption],
    duckdb::Value::INTEGER(static_cast<int32_t>(options.max_cells)));
  if (!normalized) {
    ReplaceOption(args[dimensions + kLowerOption], duckdb::Value{});
    ReplaceOption(args[dimensions + kUpperOption], duckdb::Value{});
  }
  return spec;
}

const CurveSpec* TryGetCurveSpec(const duckdb::Expression& expression) {
  if (expression.GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_FUNCTION) {
    return nullptr;
  }
  const auto& function = expression.Cast<duckdb::BoundFunctionExpression>();
  if (function.Function().GetName().GetIdentifierName() != kCurveFunction ||
      !function.BindInfo()) {
    return nullptr;
  }
  return &function.BindInfo()->Cast<CurveSpec>();
}

void EncodeCurveCodes(const CurveSpec& spec, duckdb::DataChunk& args,
                      duckdb::Vector& result) {
  const auto count = args.size();
  const auto dimensions = spec.options.dimensions;
  std::array<duckdb::RecursiveUnifiedVectorFormat, irs::curve::kMaxDimensions>
    formats;
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    if (spec.axes[axis].family == CurveFamily::Null) {
      result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
      duckdb::ConstantVector::SetNull(result, true);
      return;
    }
    duckdb::Vector::RecursiveToUnifiedFormat(args.data[axis], formats[axis]);
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  irs::curve::Point point;
  point.fill(0);
  irs::curve::Code code;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    bool valid = true;
    for (uint32_t axis = 0; axis < dimensions && valid; ++axis) {
      valid = IsValid(spec.axes[axis], formats[axis], row);
    }
    if (!valid) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      point[axis] = AxisKey(spec.axes[axis], formats[axis], row);
    }
    const auto size = irs::curve::LeafCode(point, spec.options, code.data());
    out[row] = duckdb::StringVector::AddStringOrBlob(
      result, reinterpret_cast<const char*>(code.data()), size);
  }
  if (args.AllConstant()) {
    result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  }
}

}  // namespace sdb::connector
