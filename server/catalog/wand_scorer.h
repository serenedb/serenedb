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
#include <iresearch/search/raw_tf.hpp>
#include <iresearch/search/tfidf.hpp>
#include <vpack/serializer.h>

#include <cstdint>
#include <string_view>
#include <variant>

#include "basics/exceptions.h"

namespace sdb::catalog {

// Scorer specification persisted on an inverted index whose user opted in to
// `WITH (optimize_top_k = '<scorer-expr>')`. The writer instantiates this
// concrete scorer at shard creation time so the WAND impact data baked into
// each posting is consistent with the coefficients the user expects at
// query time. Mirrors the scorer kinds the optimizer recognises in
// `BM25(...)` / `TFIDF(...)` / etc. SQL expressions.
//
// Each per-arm struct exposes `kName == irs::<Scorer>::type_name()` so the
// persisted discriminator and the user-visible `WITH` keyword stay in lock
// step with the iresearch scorer registration -- no parallel name table.
struct WandScorer {
  enum class DfiMeasure : uint8_t {
    Standardized,
    Saturated,
    ChiSquared,
  };

  struct Bm25 {
    static constexpr std::string_view kName = irs::BM25::type_name();
    float k1 = 1.2f;
    float b = 0.75f;
  };
  struct Tfidf {
    static constexpr std::string_view kName = irs::TFIDF::type_name();
    bool with_norms = false;
  };
  struct RawTf {
    static constexpr std::string_view kName = irs::RawTF::type_name();
  };
  struct LmJm {
    static constexpr std::string_view kName = irs::LMJelinekMercer::type_name();
    float lambda = 0.1f;
  };
  struct LmDirichlet {
    static constexpr std::string_view kName = irs::LMDirichlet::type_name();
    float mu = 2000.0f;
  };
  struct IndriDirichlet {
    static constexpr std::string_view kName = irs::IndriDirichlet::type_name();
    float mu = 2000.0f;
  };
  struct Dfi {
    static constexpr std::string_view kName = irs::DFI::type_name();
    DfiMeasure measure = DfiMeasure::Standardized;
  };

  using Params = std::variant<Bm25, Tfidf, RawTf, LmJm, LmDirichlet,
                              IndriDirichlet, Dfi>;

  Params params;

  // Lowercase short scorer name -- always matches the active arm's kName.
  std::string_view Name() const noexcept {
    return std::visit(
      []<typename P>(const P&) -> std::string_view { return P::kName; },
      params);
  }
};

// vpack serialisation hook for WandScorer. The persisted shape is a 2-tuple
// `[name, arm_fields]`: `name` discriminates which variant arm follows, and
// the arm itself is written via WriteTuple so each per-arm aggregate is
// (de)serialised positionally by boost::pfr (no per-field code here). The
// hook is picked up via ADL by the standard vpack::WriteTuple /
// vpack::ReadTuple machinery.
template<typename Context>
void VPackWrite(Context ctx, const WandScorer& s) {
  auto& b = ctx.vpack();
  b.openArray(true);
  b.add(s.Name());
  std::visit([&](const auto& p) { vpack::WriteTuple(b, p, ctx.arg()); },
             s.params);
  b.close();
}

template<typename Context>
void VPackRead(Context ctx, WandScorer& s) {
  vpack::ArrayIterator it{ctx.vpack()};
  if (!it.valid() || !(*it).isString()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "Invalid 'wand_scorer' tuple: missing scorer name");
  }
  // 1. Discriminator -> default-construct the matching variant arm.
  const auto name = (*it).stringView();
  if (name == WandScorer::Bm25::kName) {
    s.params = WandScorer::Bm25{};
  } else if (name == WandScorer::Tfidf::kName) {
    s.params = WandScorer::Tfidf{};
  } else if (name == WandScorer::RawTf::kName) {
    s.params = WandScorer::RawTf{};
  } else if (name == WandScorer::LmJm::kName) {
    s.params = WandScorer::LmJm{};
  } else if (name == WandScorer::LmDirichlet::kName) {
    s.params = WandScorer::LmDirichlet{};
  } else if (name == WandScorer::IndriDirichlet::kName) {
    s.params = WandScorer::IndriDirichlet{};
  } else if (name == WandScorer::Dfi::kName) {
    s.params = WandScorer::Dfi{};
  } else {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Unknown 'wand_scorer' name '", name,
              "'");
  }
  it.next();
  if (!it.valid()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "Invalid 'wand_scorer' tuple: missing arm payload for '", name,
              "'");
  }
  // 2. Fill the active arm via the standard tuple reader (boost::pfr).
  std::visit([&](auto& p) { vpack::ReadTuple(*it, p, ctx.arg()); }, s.params);
}

}  // namespace sdb::catalog
