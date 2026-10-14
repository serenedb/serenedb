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

#include <absl/numeric/int128.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/utils/space_filling_curve.hpp>
#include <optional>

namespace sdb::connector {

inline constexpr std::string_view kCurveFunction = "curve";

enum class CurveFamily : uint8_t {
  Null,
  Signed,
  Unsigned,
  Float,
  Date,
  Timestamp,
  TimestampTz,
  Inet,
};

struct CurveKey {
  uint64_t key = 0;
  uint64_t step = 1;
  bool exact = false;
};

struct CurveInterval {
  uint64_t min = 0;
  uint64_t max = 0;
};

struct CurveAxis {
  duckdb::LogicalType type;
  CurveFamily family = CurveFamily::Null;
  bool normalized = false;
  uint64_t key_lo = 0;
  uint64_t key_hi = 0;
  double lo = 0;
  double hi = 0;
  bool ipv6_range = false;
  absl::uint128 ipv6_lo = 0;
  absl::uint128 ipv6_hi = 0;

  bool operator==(const CurveAxis&) const = default;
};

std::optional<CurveKey> CurveBoundKey(const CurveAxis& axis,
                                      const duckdb::Value& bound);
CurveInterval CurveLimits(const CurveAxis& axis) noexcept;

struct CurveSpec final : public duckdb::FunctionData {
  irs::curve::Options options;
  std::array<CurveAxis, irs::curve::kMaxDimensions> axes;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final;
  bool Equals(const duckdb::FunctionData& other) const final;
};

duckdb::unique_ptr<CurveSpec> BindCurveSpec(
  duckdb::ClientContext& context,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& args);

const CurveSpec* TryGetCurveSpec(const duckdb::Expression& expression);

void EncodeCurveCodes(const CurveSpec& spec, duckdb::DataChunk& args,
                      duckdb::Vector& result);

class CurveTokenizer final
  : public irs::analysis::TypedTokenizer<CurveTokenizer> {
 public:
  explicit CurveTokenizer(irs::curve::Options options) : _options{options} {
    irs::curve::Validate(options);
  }

  static constexpr std::string_view type_name() noexcept { return "curve"; }

  irs::TokenTraits Traits() const noexcept final {
    return {.output = duckdb::LogicalTypeId::BLOB};
  }

  const irs::curve::Options& Options() const noexcept { return _options; }

  template<irs::TokenLayout Layout>
  bool DoFill(duckdb::string_t value, irs::TokenSink& sink) {
    const std::span code{reinterpret_cast<const uint8_t*>(value.GetData()),
                         value.GetSize()};
    if (code.size() !=
        irs::curve::CodeSize(_options.dimensions, _options.max_level)) {
      return false;
    }
    const auto emit = [&](std::span<const uint8_t> term) {
      sink.Emit<Layout>(term.data(), static_cast<uint32_t>(term.size()));
    };
    irs::curve::CodeTerms(code, _options, emit);
    return true;
  }

 private:
  irs::curve::Options _options;
};

}  // namespace sdb::connector
