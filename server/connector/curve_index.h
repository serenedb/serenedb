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

#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/utils/space_filling_curve.hpp>

namespace sdb::connector {

void ValidateCurveType(std::string_view label, const duckdb::LogicalType& type);
void ValidateCurveBounds(const duckdb::LogicalType& point,
                         const duckdb::LogicalType& lower,
                         const duckdb::LogicalType& upper);

class CurveTuples {
 public:
  CurveTuples(const duckdb::Vector& tuples, const duckdb::LogicalType& point);

  bool IsValid(duckdb::idx_t row) const {
    return _tuples.validity.RowIsValid(_tuples.sel->get_index(row));
  }
  bool IsValid(duckdb::idx_t row, uint32_t axis) const {
    const auto& format = _axes[axis].format;
    return format.validity.RowIsValid(format.sel->get_index(row));
  }
  uint64_t Get(duckdb::idx_t row, uint32_t axis) const;

 private:
  struct Axis {
    duckdb::UnifiedVectorFormat format;
    duckdb::LogicalTypeId type = duckdb::LogicalTypeId::SQLNULL;
    bool as_double = false;
  };

  duckdb::UnifiedVectorFormat _tuples;
  std::array<Axis, irs::curve::kMaxDimensions> _axes;
};

irs::curve::Box CurveBox(const CurveTuples& lower, duckdb::idx_t lower_row,
                         const CurveTuples& upper, duckdb::idx_t upper_row,
                         uint32_t dimensions);
irs::curve::Box CurveBox(const duckdb::LogicalType& point,
                         const duckdb::Value& lower,
                         const duckdb::Value& upper);
void PackCurvePoints(const duckdb::Vector& points, duckdb::idx_t count,
                     uint32_t dimensions, duckdb::Vector& packed);

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
    const std::string_view bytes{value.GetData(), value.GetSize()};
    const auto emit = [&](std::span<const uint8_t> term) {
      sink.Emit<Layout>(term.data(), static_cast<uint32_t>(term.size()));
    };
    irs::curve::PointTerms(UnpackPoint(bytes), _options, emit);
    return true;
  }

 private:
  irs::curve::Point UnpackPoint(std::string_view bytes) const noexcept;

  irs::curve::Options _options;
};

}  // namespace sdb::connector
