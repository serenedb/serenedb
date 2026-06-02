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

#include <iresearch/search/bm25.hpp>
#include <iresearch/search/dfi.hpp>
#include <iresearch/search/indri_dirichlet.hpp>
#include <iresearch/search/lm_dirichlet.hpp>
#include <iresearch/search/lm_jelinek_mercer.hpp>
#include <iresearch/search/raw_boost.hpp>
#include <iresearch/search/raw_dl.hpp>
#include <iresearch/search/raw_tf.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/tfidf.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>
#include <string_view>
#include <variant>

namespace sdb::catalog::persistence {

struct ScorerOptions {
  using DfiMeasure = irs::DFIMeasure;
  using Bm25 = irs::BM25::Options;
  using Tfidf = irs::TFIDF::Options;
  using LmJm = irs::LMJelinekMercer::Options;
  using LmDirichlet = irs::LMDirichlet::Options;
  using IndriDirichlet = irs::IndriDirichlet::Options;
  using Dfi = irs::DFI::Options;
  using RawBoost = irs::RawBoost::Options;
  using RawTf = irs::RawTF::Options;
  using RawDL = irs::RawDL::Options;

  using Params = std::variant<Bm25, Tfidf, LmJm, LmDirichlet, IndriDirichlet,
                              Dfi, RawBoost, RawTf, RawDL>;

  Params params;

  bool operator==(const ScorerOptions&) const = default;

  std::string_view Name() const noexcept {
    return std::visit(
      []<typename P>(const P&) -> std::string_view {
        return P::Owner::type_name();
      },
      params);
  }

  // EXPLAIN-friendly spelling, e.g. `bm25(k1=1.2, b=0.75)`.
  std::string ToString() const;
};

}  // namespace sdb::catalog::persistence
namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<sdb::catalog::persistence::ScorerOptions::DfiMeasure>(
  sdb::catalog::persistence::ScorerOptions::DfiMeasure value) noexcept {
  using DfiMeasure = sdb::catalog::persistence::ScorerOptions::DfiMeasure;
  switch (value) {
    case DfiMeasure::Standardized:
      return "standardized";
    case DfiMeasure::Saturated:
      return "saturated";
    case DfiMeasure::ChiSquared:
      return "chi_squared";
  }
  return invalid_tag;
}

}  // namespace magic_enum
