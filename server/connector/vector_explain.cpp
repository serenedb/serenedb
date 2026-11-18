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

#include "connector/vector_explain.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/substitute.h>

#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <string>

#include "connector/functions/vector.h"

namespace sdb::connector {
namespace {

struct DistanceFunction {
  std::string_view name;
  std::string_view formula;
  bool norm = false;
};

DistanceFunction DistanceFunctionOf(const VectorScorerOptions& vs) {
  switch (vs.metric) {
    case irs::VectorMetric::L2Sqr:
      if (vs.score_emit == ScoreEmit::ZeroMinus) {
        return {kL2SqrDistance, "sum(($0[i] - query[i])^2)"};
      }
      if (vs.norm) {
        return {kL2Norm, "sqrt(sum($0[i]^2))", true};
      }
      return {kL2Distance, "sqrt(sum(($0[i] - query[i])^2))"};
    case irs::VectorMetric::L1:
      if (vs.norm) {
        return {kL1Norm, "sum(abs($0[i]))", true};
      }
      return {kL1Distance, "sum(abs($0[i] - query[i]))"};
    case irs::VectorMetric::Cosine:
      if (vs.score_emit == ScoreEmit::Identity) {
        return {kCosineSimilarity,
                "sum($0[i] * query[i]) / (sqrt(sum($0[i]^2)) * "
                "sqrt(sum(query[i]^2)))"};
      }
      return {kCosineDistance,
              "1 - sum($0[i] * query[i]) / (sqrt(sum($0[i]^2)) * "
              "sqrt(sum(query[i]^2)))"};
    case irs::VectorMetric::InnerProduct:
      if (vs.score_emit == ScoreEmit::Negate) {
        return {kNegativeIP, "-sum($0[i] * query[i])"};
      }
      return {kIP, "sum($0[i] * query[i])"};
  }
  SDB_UNREACHABLE();
}

std::string Method(const VectorScorerOptions& vs) {
  if (vs.exact) {
    return "computed from the row's stored vector";
  }
  auto method = absl::StrCat(
    "found by an ", absl::AsciiStrToLower(magic_enum::enum_name(vs.kind)),
    " search");
  if (vs.quant != irs::VectorQuantization::None) {
    absl::StrAppend(&method, " over ",
                    absl::AsciiStrToLower(magic_enum::enum_name(vs.quant)),
                    " codes");
  }
  return method;
}

void AddSearchParameters(const VectorScorerOptions& vs,
                         irs::explain::Node& out) {
  using irs::explain::AddDetail;
  const bool ranged = vs.radius != std::numeric_limits<float>::max();
  if (vs.top_k != 0) {
    AddDetail(out, "k", uint64_t{vs.top_k},
              "k, the number of nearest rows the search returns");
  }
  if (ranged) {
    AddDetail(out, "radius", vs.radius,
              vs.radius_inclusive
                ? "radius, the largest distance the search returns"
                : "radius, the search returns distances below it");
  }
  if (vs.exact || vs.top_k == 0 || ranged) {
    return;
  }
  if (vs.kind == irs::AnnKind::Hnsw) {
    AddDetail(out, "ef_search", uint64_t{vs.ef_search},
              "ef_search, candidates the search keeps in its beam");
  } else if (vs.nprobe != 0) {
    AddDetail(out, "nprobe", uint64_t{vs.nprobe},
              "nprobe, clusters the search scans in each segment");
  }
  if (vs.quant != irs::VectorQuantization::None && !vs.rescore_all) {
    AddDetail(out, "rescore_width", vs.rescore_width,
              "rescore_width, how far a code score may be off and still be "
              "read again at full precision");
  }
}

}  // namespace

irs::explain::Node ExplainDistance(const VectorScorerOptions& vs,
                                   std::string_view column,
                                   std::optional<irs::score_t> returned,
                                   std::optional<irs::score_t> stored) {
  const auto function = DistanceFunctionOf(vs);
  const auto call = absl::StrCat(function.name, "(", column,
                                 function.norm ? "" : ", query", ")");
  if (!stored) {
    return {
      .key = "distance",
      .value = std::numeric_limits<irs::score_t>::quiet_NaN(),
      .description =
        absl::StrCat(call, " is NULL because ", column, " is NULL in the row"),
    };
  }
  const auto exact = ApplyScoreEmit(vs.score_emit, *stored);
  irs::explain::Node out{
    .key = "distance",
    .value = returned ? ApplyScoreEmit(vs.score_emit, *returned) : exact,
    .description =
      absl::StrCat(call, " = ", absl::Substitute(function.formula, column),
                   ", ", Method(vs)),
  };
  if (!vs.exact) {
    irs::explain::AddDetail(
      out, "exact", exact,
      "exact, the distance computed from the row's stored vector");
  }
  irs::explain::AddDetail(out, "dims", uint64_t{vs.dims},
                          "dims, dimensions of the vectors");
  AddSearchParameters(vs, out);
  return out;
}

DistanceExplainer::DistanceExplainer(const irs::SubReader& segment,
                                     const VectorScorerOptions& vs,
                                     std::string column)
  : _vs{vs}, _column{std::move(column)} {
  const auto* stored = segment.Column(vs.field_id);
  const auto* columns = segment.GetColReader();
  if (!stored || !columns) {
    return;
  }
  _reader.emplace(*stored, *columns, vs.dims);
  _reader->SetQuery(vs.query_vector, vs.metric);
  if (irs::detail::MayHaveNullRows(*stored)) {
    _validity.emplace(*columns, *stored);
  }
}

irs::explain::Node DistanceExplainer::Explain(
  irs::doc_id_t doc, std::optional<irs::score_t> returned) {
  return ExplainDistance(_vs, _column, returned, Stored(doc));
}

std::optional<irs::score_t> DistanceExplainer::Stored(irs::doc_id_t doc) {
  if (!_reader) {
    return std::nullopt;
  }
  if (_validity &&
      !_validity->ValidRow(static_cast<uint64_t>(doc) - irs::doc_limits::min(),
                           _scratch)) {
    return std::nullopt;
  }
  return _reader->ComputeDistance(doc, nullptr);
}

}  // namespace sdb::connector
