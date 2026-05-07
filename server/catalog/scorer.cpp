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

#include "catalog/scorer.h"

#include <absl/strings/str_cat.h>

#include <magic_enum/magic_enum.hpp>

namespace sdb::catalog {

std::string Scorer::ToString() const {
  return std::visit(
    [&]<typename P>(const P& p) -> std::string {
      if constexpr (std::is_same_v<P, Bm25>) {
        return absl::StrCat("bm25(k1=", p.k1, ", b=", p.b, ")");
      } else if constexpr (std::is_same_v<P, Tfidf>) {
        return absl::StrCat(
          "tfidf(with_norms=", p.with_norms ? "true" : "false", ")");
      } else if constexpr (std::is_same_v<P, RawTf>) {
        return "raw_tf()";
      } else if constexpr (std::is_same_v<P, LmJm>) {
        return absl::StrCat("lm_jm(lambda=", p.lambda, ")");
      } else if constexpr (std::is_same_v<P, LmDirichlet>) {
        return absl::StrCat("lm_dirichlet(mu=", p.mu, ")");
      } else if constexpr (std::is_same_v<P, IndriDirichlet>) {
        return absl::StrCat("indri_dirichlet(mu=", p.mu, ")");
      } else if constexpr (std::is_same_v<P, Dfi>) {
        return absl::StrCat("dfi(measure=", magic_enum::enum_name(p.measure),
                            ")");
      }
    },
    params);
}

std::unique_ptr<irs::Scorer> MakeIrsScorer(const Scorer& spec) {
  return std::visit(
    []<typename P>(const P& p) -> std::unique_ptr<irs::Scorer> {
      if constexpr (std::is_same_v<P, Scorer::Bm25>) {
        return std::make_unique<irs::BM25>(p.k1, p.b);
      } else if constexpr (std::is_same_v<P, Scorer::Tfidf>) {
        return std::make_unique<irs::TFIDF>(p.with_norms);
      } else if constexpr (std::is_same_v<P, Scorer::RawTf>) {
        return std::make_unique<irs::RawTF>();
      } else if constexpr (std::is_same_v<P, Scorer::LmJm>) {
        return std::make_unique<irs::LMJelinekMercer>(p.lambda);
      } else if constexpr (std::is_same_v<P, Scorer::LmDirichlet>) {
        return std::make_unique<irs::LMDirichlet>(p.mu);
      } else if constexpr (std::is_same_v<P, Scorer::IndriDirichlet>) {
        return std::make_unique<irs::IndriDirichlet>(p.mu);
      } else if constexpr (std::is_same_v<P, Scorer::Dfi>) {
        irs::DFIMeasure m{};
        switch (p.measure) {
          case Scorer::DfiMeasure::Standardized:
            m = irs::DFIMeasure::Standardized;
            break;
          case Scorer::DfiMeasure::Saturated:
            m = irs::DFIMeasure::Saturated;
            break;
          case Scorer::DfiMeasure::ChiSquared:
            m = irs::DFIMeasure::ChiSquared;
            break;
        }
        return std::make_unique<irs::DFI>(m);
      }
    },
    spec.params);
}

}  // namespace sdb::catalog
