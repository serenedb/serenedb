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

#include "pg/types/money.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <duckdb/common/types/decimal.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/aggregate/distributive_function_utils.hpp>
#include <duckdb/function/aggregate_function.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <string>

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/types/cast.h"
#include "pg/types/logical.h"
#include "pg/types/mapping.h"

namespace sdb::pg {
namespace {

constexpr int kFractionDigits = 2;
constexpr int64_t kCentsPerUnit = 100;
constexpr int kGroupDigits = 3;

[[noreturn]] void ThrowOutOfRange(std::string_view message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG(message));
}

[[noreturn]] void ThrowDivisionByZero() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                  ERR_MSG("division by zero"));
}

int64_t AddMoney(int64_t left, int64_t right) {
  int64_t sum = 0;
  if (__builtin_add_overflow(left, right, &sum)) {
    ThrowOutOfRange("money out of range");
  }
  return sum;
}

int64_t SubtractMoney(int64_t left, int64_t right) {
  int64_t difference = 0;
  if (__builtin_sub_overflow(left, right, &difference)) {
    ThrowOutOfRange("money out of range");
  }
  return difference;
}

int64_t RoundMoney(double value) {
  const auto rounded = std::rint(value);
  if (std::isnan(rounded) ||
      rounded < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
      rounded >= -static_cast<double>(std::numeric_limits<int64_t>::min())) {
    ThrowOutOfRange("money out of range");
  }
  return static_cast<int64_t>(rounded);
}

double CheckFloatResult(double result, double operand) {
  if (std::isinf(result) && !std::isinf(operand)) {
    ThrowOutOfRange("value out of range: overflow");
  }
  return result;
}

template<typename T>
int64_t MultiplyMoney(int64_t money, T factor) {
  if constexpr (std::is_floating_point_v<T>) {
    const auto operand = static_cast<double>(factor);
    return RoundMoney(
      CheckFloatResult(static_cast<double>(money) * operand, operand));
  } else {
    int64_t product = 0;
    if (__builtin_mul_overflow(money, static_cast<int64_t>(factor), &product)) {
      ThrowOutOfRange("money out of range");
    }
    return product;
  }
}

template<typename T>
int64_t DivideMoney(int64_t money, T divisor) {
  if (divisor == 0) {
    ThrowDivisionByZero();
  }
  if constexpr (std::is_floating_point_v<T>) {
    const auto dividend = static_cast<double>(money);
    return RoundMoney(
      CheckFloatResult(dividend / static_cast<double>(divisor), dividend));
  } else {
    if (divisor == -1 && money == std::numeric_limits<int64_t>::min()) {
      ThrowOutOfRange("money out of range");
    }
    return money / static_cast<int64_t>(divisor);
  }
}

double MoneyRatio(int64_t dividend, int64_t divisor) {
  if (divisor == 0) {
    ThrowDivisionByZero();
  }
  return static_cast<double>(dividend) / static_cast<double>(divisor);
}

std::optional<int64_t> MoneyFromInteger(int64_t amount) {
  int64_t money = 0;
  if (__builtin_mul_overflow(amount, kCentsPerUnit, &money)) {
    return std::nullopt;
  }
  return money;
}

void AppendNumberWords(std::string& out, int64_t value) {
  static constexpr std::array<std::string_view, 28> kSmall = {
    "zero",     "one",      "two",      "three",   "four",    "five",
    "six",      "seven",    "eight",    "nine",    "ten",     "eleven",
    "twelve",   "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
    "eighteen", "nineteen", "twenty",   "thirty",  "forty",   "fifty",
    "sixty",    "seventy",  "eighty",   "ninety",
  };
  const auto tens = [](int64_t units) { return kSmall[18 + units / 10]; };
  const auto tu = value % 100;
  if (value <= 20) {
    absl::StrAppend(&out, kSmall[value]);
  } else if (tu == 0) {
    absl::StrAppend(&out, kSmall[value / 100], " hundred");
  } else if (value > 99) {
    if (value % 10 == 0 && tu > 10) {
      absl::StrAppend(&out, kSmall[value / 100], " hundred ", tens(tu));
    } else if (tu < 20) {
      absl::StrAppend(&out, kSmall[value / 100], " hundred and ", kSmall[tu]);
    } else {
      absl::StrAppend(&out, kSmall[value / 100], " hundred ", tens(tu), " ",
                      kSmall[tu % 10]);
    }
  } else if (value % 10 == 0 && tu > 10) {
    absl::StrAppend(&out, tens(tu));
  } else if (tu < 20) {
    absl::StrAppend(&out, kSmall[tu]);
  } else {
    absl::StrAppend(&out, tens(tu), " ", kSmall[tu % 10]);
  }
}

std::string CashWords(int64_t money) {
  struct Scale {
    uint64_t cents;
    std::string_view name;
  };
  static constexpr Scale kScales[] = {
    {100'000'000'000'000'000, " quadrillion "},
    {100'000'000'000'000, " trillion "},
    {100'000'000'000, " billion "},
    {100'000'000, " million "},
    {100'000, " thousand "},
  };
  std::string out;
  if (money < 0) {
    out = "minus ";
  }
  const auto value =
    money < 0 ? 0 - static_cast<uint64_t>(money) : static_cast<uint64_t>(money);
  for (const auto& scale : kScales) {
    if (const auto group = (value / scale.cents) % 1000; group != 0) {
      AppendNumberWords(out, static_cast<int64_t>(group));
      absl::StrAppend(&out, scale.name);
    }
  }
  const auto dollars = value / kCentsPerUnit;
  const auto cents = static_cast<int64_t>(value % kCentsPerUnit);
  if (const auto units = dollars % 1000; units != 0) {
    AppendNumberWords(out, static_cast<int64_t>(units));
  }
  if (dollars == 0) {
    absl::StrAppend(&out, "zero");
  }
  absl::StrAppend(&out, dollars == 1 ? " dollar and " : " dollars and ");
  AppendNumberWords(out, cents);
  absl::StrAppend(&out, cents == 1 ? " cent" : " cents");
  out[0] = absl::ascii_toupper(static_cast<unsigned char>(out[0]));
  return out;
}

void CashWordsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](int64_t money) {
      return duckdb::StringVector::AddString(result, CashWords(money));
    });
}

std::string PgTypeName(const duckdb::LogicalType& type) {
  const auto oid = static_cast<int32_t>(Type2Oid(type));
  if (const auto name = SqlTypeName(oid); !name.empty()) {
    return std::string{name};
  }
  if (const auto* builtin = FindBuiltinType(oid)) {
    return std::string{builtin->name};
  }
  return type.ToString();
}

duckdb::BoundCastInfo CannotCast(duckdb::BindCastInput&,
                                 const duckdb::LogicalType& source,
                                 const duckdb::LogicalType& target) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_CANNOT_COERCE),
                  ERR_MSG("cannot cast type ", PgTypeName(source), " to ",
                          PgTypeName(target)));
}

bool MoneyToText(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    source, result, count, [&](int64_t money) {
      char buffer[kMoneyText];
      return duckdb::StringVector::AddString(result, buffer,
                                             WriteMoney(money, buffer));
    });
  return true;
}

template<typename T>
bool IntegerToMoney(duckdb::Vector& source, duckdb::Vector& result,
                    duckdb::idx_t count, duckdb::CastParameters& parameters) {
  bool converted = true;
  duckdb::UnaryExecutor::Execute<T, int64_t>(
    source, result, count, [&](T amount) -> std::optional<int64_t> {
      if (const auto money = MoneyFromInteger(amount)) {
        return *money;
      }
      InputError(SQL_ERROR_DATA(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                ERR_MSG("bigint out of range")),
                 parameters);
      converted = false;
      return std::nullopt;
    });
  return converted;
}

duckdb::LogicalType CentsType() {
  return duckdb::LogicalType::DECIMAL(duckdb::Decimal::MAX_WIDTH_DECIMAL,
                                      kFractionDigits);
}

struct DecimalCastData final : duckdb::BoundCastData {
  explicit DecimalCastData(duckdb::BoundCastInfo decimal)
    : decimal{std::move(decimal)} {}

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<DecimalCastData>(decimal.Copy());
  }

  duckdb::BoundCastInfo decimal;
};

bool MoneyToDecimal(duckdb::Vector& source, duckdb::Vector& result,
                    duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto& decimal = parameters.cast_data->Cast<DecimalCastData>().decimal;
  duckdb::Vector cents{CentsType(), count};
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::hugeint_t>(
    source, cents, count,
    [](int64_t money) { return duckdb::hugeint_t{money}; });
  duckdb::CastParameters decimal_parameters{parameters, decimal.GetCastData(),
                                            parameters.local_state};
  return decimal.Cast(cents, result, count, decimal_parameters);
}

bool DecimalToMoney(duckdb::Vector& source, duckdb::Vector& result,
                    duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto& decimal = parameters.cast_data->Cast<DecimalCastData>().decimal;
  duckdb::Vector cents{CentsType(), count};
  duckdb::CastParameters decimal_parameters{parameters, decimal.GetCastData(),
                                            parameters.local_state};
  bool converted = decimal.Cast(source, cents, count, decimal_parameters);
  duckdb::UnaryExecutor::Execute<duckdb::hugeint_t, int64_t>(
    cents, result, count,
    [&](duckdb::hugeint_t value) -> std::optional<int64_t> {
      int64_t money = 0;
      if (duckdb::Hugeint::TryCast(value, money)) {
        return money;
      }
      InputError(SQL_ERROR_DATA(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                ERR_MSG("bigint out of range")),
                 parameters);
      converted = false;
      return std::nullopt;
    });
  return converted;
}

duckdb::BoundCastInfo BindMoneyToDecimal(duckdb::BindCastInput& input,
                                         const duckdb::LogicalType&,
                                         const duckdb::LogicalType& target) {
  return {MoneyToDecimal, duckdb::make_uniq<DecimalCastData>(
                            input.GetCastFunction(CentsType(), target))};
}

duckdb::BoundCastInfo BindDecimalToMoney(duckdb::BindCastInput& input,
                                         const duckdb::LogicalType& source,
                                         const duckdb::LogicalType&) {
  return {DecimalToMoney, duckdb::make_uniq<DecimalCastData>(
                            input.GetCastFunction(source, CentsType()))};
}

void RegisterCasts(duckdb::DatabaseInstance& db) {
  auto& casts = duckdb::DBConfig::GetConfig(db).GetCastFunctions();
  const auto money = MONEY();
  RegisterTextInput(casts, money, TextInputCast<int64_t, TryParseMoney>,
                    InitInput);
  casts.RegisterCastFunction(money, duckdb::LogicalType::VARCHAR, MoneyToText,
                             50);
  casts.RegisterCastFunction(money, duckdb::LogicalTypeId::DECIMAL,
                             BindMoneyToDecimal, -1);
  casts.RegisterCastFunction(duckdb::LogicalTypeId::DECIMAL, money,
                             BindDecimalToMoney, -1);
  casts.RegisterCastFunction(duckdb::LogicalType::INTEGER, money,
                             IntegerToMoney<int32_t>, -1);
  casts.RegisterCastFunction(duckdb::LogicalType::BIGINT, money,
                             IntegerToMoney<int64_t>, -1);
  for (const auto id : {
         duckdb::LogicalTypeId::BOOLEAN,
         duckdb::LogicalTypeId::TINYINT,
         duckdb::LogicalTypeId::SMALLINT,
         duckdb::LogicalTypeId::INTEGER,
         duckdb::LogicalTypeId::BIGINT,
         duckdb::LogicalTypeId::HUGEINT,
         duckdb::LogicalTypeId::UTINYINT,
         duckdb::LogicalTypeId::USMALLINT,
         duckdb::LogicalTypeId::UINTEGER,
         duckdb::LogicalTypeId::UBIGINT,
         duckdb::LogicalTypeId::UHUGEINT,
         duckdb::LogicalTypeId::FLOAT,
         duckdb::LogicalTypeId::DOUBLE,
         duckdb::LogicalTypeId::BIGNUM,
       }) {
    const duckdb::LogicalType number{id};
    casts.RegisterCastFunction(money, number, CannotCast, -1);
    if (id != duckdb::LogicalTypeId::INTEGER &&
        id != duckdb::LogicalTypeId::BIGINT) {
      casts.RegisterCastFunction(number, money, CannotCast, -1);
    }
  }
}

template<typename L, typename R, typename Result, auto Apply>
void Binary(duckdb::DataChunk& args, duckdb::ExpressionState&,
            duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<L, R, Result>(args.data[0], args.data[1],
                                                result, args.size(), Apply);
}

duckdb::ScalarFunction Operator(std::string_view name, duckdb::LogicalType left,
                                duckdb::LogicalType right,
                                duckdb::LogicalType result,
                                duckdb::scalar_function_t function) {
  duckdb::ScalarFunction scalar{duckdb::Identifier{name},
                                {std::move(left), std::move(right)},
                                std::move(result),
                                function};
  auto& signature = scalar.GetSignature();
  signature.GetParameter(0).SetName("left");
  signature.GetParameter(1).SetName("right");
  scalar.SetFallible();
  return scalar;
}

constexpr std::array<std::string_view, 2> kDivisionOperators = {"/", "//"};

template<typename T>
void RegisterFactorOperators(duckdb::ExtensionLoader& loader,
                             const duckdb::LogicalType& type) {
  const auto money = MONEY();
  loader.AddFunctionOverload(Operator(
    "*", money, type, money, Binary<int64_t, T, int64_t, &MultiplyMoney<T>>));
  loader.AddFunctionOverload(Operator(
    "*", type, money, money,
    Binary<T, int64_t, int64_t,
           [](T factor, int64_t m) { return MultiplyMoney(m, factor); }>));
  for (const auto name : kDivisionOperators) {
    loader.AddFunctionOverload(Operator(
      name, money, type, money, Binary<int64_t, T, int64_t, &DivideMoney<T>>));
  }
}

struct MoneySumState {
  using STATE_TYPE = duckdb::OptionalStateType<int64_t>;

  int64_t val;
  bool is_set;
};

struct MoneyAdd {
  template<typename T>
  static T Operation(T left, T right) {
    return AddMoney(left, right);
  }
};

struct MoneySum : duckdb::EmptyValAggregate<MoneyAdd, duckdb::ConstantInit<0>> {
  using Base = duckdb::EmptyValAggregate<MoneyAdd, duckdb::ConstantInit<0>>;
  using Base::UpdateClusteredLocal;

  template<typename Input, typename State>
  static void UpdateClusteredLocal(State& local, const Input& input,
                                   duckdb::idx_t count) {
    for (duckdb::idx_t i = 0; i < count; ++i) {
      Base::UpdateClusteredLocal<Input>(local, input);
    }
  }
};

void RegisterFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto money = MONEY();
  loader.AddFunctionOverload(Operator(
    "+", money, money, money, Binary<int64_t, int64_t, int64_t, &AddMoney>));
  loader.AddFunctionOverload(
    Operator("-", money, money, money,
             Binary<int64_t, int64_t, int64_t, &SubtractMoney>));
  for (const auto name : kDivisionOperators) {
    loader.AddFunctionOverload(
      Operator(name, money, money, duckdb::LogicalType::DOUBLE,
               Binary<int64_t, int64_t, double, &MoneyRatio>));
  }
  RegisterFactorOperators<int16_t>(loader, duckdb::LogicalType::SMALLINT);
  RegisterFactorOperators<int32_t>(loader, duckdb::LogicalType::INTEGER);
  RegisterFactorOperators<int64_t>(loader, duckdb::LogicalType::BIGINT);
  RegisterFactorOperators<float>(loader, duckdb::LogicalType::FLOAT);
  RegisterFactorOperators<double>(loader, duckdb::LogicalType::DOUBLE);
  loader.RegisterFunction(duckdb::ScalarFunction{
    "cashlarger",
    {money, money},
    money,
    Binary<int64_t, int64_t, int64_t,
           [](int64_t l, int64_t r) { return std::max(l, r); }>});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "cashsmaller",
    {money, money},
    money,
    Binary<int64_t, int64_t, int64_t,
           [](int64_t l, int64_t r) { return std::min(l, r); }>});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "cash_words", {money}, duckdb::LogicalType::VARCHAR, CashWordsFunction});
  auto sum =
    duckdb::AggregateFunction::UnaryAggregate<MoneySumState, int64_t, int64_t,
                                              MoneySum>(money, money);
  sum.SetName("sum");
  sum.GetSignature().GetParameter(0).SetName("arg");
  loader.AddFunctionOverload(std::move(sum));
}

}  // namespace

std::optional<irs::pg::SqlErrorData> TryParseMoney(std::string_view text,
                                                   int64_t& money) {
  const auto out_of_range = [&] {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
      ERR_MSG("value \"", text, "\" is out of range for type money"));
  };
  size_t i = 0;
  const auto skip_spaces = [&] {
    while (i < text.size() &&
           absl::ascii_isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
  };
  const auto skip = [&](char symbol) {
    const bool found = i < text.size() && text[i] == symbol;
    i += found;
    return found;
  };
  skip_spaces();
  skip('$');
  skip_spaces();
  bool negative = skip('-') || skip('(');
  if (!negative) {
    skip('+');
  }
  skip_spaces();
  skip('$');
  skip_spaces();
  int64_t value = 0;
  int decimals = 0;
  bool seen_dot = false;
  for (; i < text.size(); ++i) {
    const char c = text[i];
    if (absl::ascii_isdigit(static_cast<unsigned char>(c)) &&
        (!seen_dot || decimals < kFractionDigits)) {
      if (__builtin_mul_overflow(value, 10, &value) ||
          __builtin_sub_overflow(value, c - '0', &value)) {
        return out_of_range();
      }
      decimals += seen_dot;
    } else if (c == '.' && !seen_dot) {
      seen_dot = true;
    } else if (c != ',') {
      break;
    }
  }
  if (i < text.size() && text[i] >= '5' && text[i] <= '9' &&
      __builtin_sub_overflow(value, 1, &value)) {
    return out_of_range();
  }
  for (; decimals < kFractionDigits; ++decimals) {
    if (__builtin_mul_overflow(value, 10, &value)) {
      return out_of_range();
    }
  }
  while (i < text.size() &&
         absl::ascii_isdigit(static_cast<unsigned char>(text[i]))) {
    ++i;
  }
  for (; i < text.size(); ++i) {
    const char c = text[i];
    if (c == '-') {
      negative = true;
    } else if (!absl::ascii_isspace(static_cast<unsigned char>(c)) &&
               c != ')' && c != '+' && c != '$') {
      return SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ERR_MSG("invalid input syntax for type money: \"", text, "\""));
    }
  }
  if (!negative) {
    if (value == std::numeric_limits<int64_t>::min()) {
      return out_of_range();
    }
    value = -value;
  }
  money = value;
  return std::nullopt;
}

size_t WriteMoney(int64_t money, char* buffer) {
  std::array<char, kMoneyText> digits{};
  auto begin = digits.end();
  auto value =
    money < 0 ? 0 - static_cast<uint64_t>(money) : static_cast<uint64_t>(money);
  int position = kFractionDigits;
  do {
    if (position == 0) {
      *--begin = '.';
    } else if (position < 0 && position % kGroupDigits == 0) {
      *--begin = ',';
    }
    *--begin = static_cast<char>('0' + value % 10);
    value /= 10;
    --position;
  } while (value != 0 || position >= 0);
  auto* out = buffer;
  if (money < 0) {
    *out++ = '-';
  }
  *out++ = '$';
  out = std::copy(begin, digits.end(), out);
  return static_cast<size_t>(out - buffer);
}

void RegisterMoney(duckdb::DatabaseInstance& db) {
  RegisterCasts(db);
  RegisterFunctions(db);
}

}  // namespace sdb::pg
