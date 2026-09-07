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
#include <iresearch/search/constant_score.hpp>
#include <iresearch/search/dfi.hpp>
#include <iresearch/search/idf.hpp>
#include <iresearch/search/indri_dirichlet.hpp>
#include <iresearch/search/lm_dirichlet.hpp>
#include <iresearch/search/lm_jelinek_mercer.hpp>
#include <iresearch/search/raw_boost.hpp>
#include <iresearch/search/raw_dl.hpp>
#include <iresearch/search/raw_tf.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/tfidf.hpp>
#include <string_view>
#include <variant>

namespace irs {

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
  using Idf = irs::IDF::Options;
  using Constant = irs::ConstantScore::Options;

  using Params = std::variant<Bm25, Tfidf, LmJm, LmDirichlet, IndriDirichlet,
                              Dfi, RawBoost, RawTf, RawDL, Idf, Constant>;

  Params params;

  bool operator==(const ScorerOptions&) const = default;

  std::string_view Name() const noexcept {
    return std::visit(
      []<typename P>(const P&) -> std::string_view {
        return P::Owner::type_name();
      },
      params);
  }
};

inline Scorer::ScoreBoundType BoundTypeOf(const ScorerOptions& opts) noexcept {
  return std::visit(
    []<typename P>(const P& p) {
      using Owner = typename P::Owner;
      if constexpr (requires { Owner::BoundTypeOf(p); }) {
        return Owner::BoundTypeOf(p);
      } else {
        return Scorer::ScoreBoundType::None;
      }
    },
    opts.params);
}

}  // namespace irs
