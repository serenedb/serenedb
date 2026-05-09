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

#include <vpack/serializer.h>

#include <cstdint>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/dfi.hpp>
#include <iresearch/search/indri_dirichlet.hpp>
#include <iresearch/search/lm_dirichlet.hpp>
#include <iresearch/search/lm_jelinek_mercer.hpp>
#include <iresearch/search/raw_tf.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/tfidf.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "basics/exceptions.h"

namespace duckdb {

class BoundFunctionExpression;
class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

struct ScorerOptions {
  enum class DfiMeasure : uint8_t {
    Standardized,
    Saturated,
    ChiSquared,
  };

  struct Bm25 {
    static constexpr std::string_view kName = irs::BM25::type_name();
    float k1 = 1.2f;
    float b = 0.75f;
    bool operator==(const Bm25&) const = default;
  };
  struct Tfidf {
    static constexpr std::string_view kName = irs::TFIDF::type_name();
    bool with_norms = false;
    bool operator==(const Tfidf&) const = default;
  };
  struct RawTf {
    static constexpr std::string_view kName = irs::RawTF::type_name();
    bool operator==(const RawTf&) const = default;
  };
  struct LmJm {
    static constexpr std::string_view kName = irs::LMJelinekMercer::type_name();
    float lambda = 0.1f;
    bool operator==(const LmJm&) const = default;
  };
  struct LmDirichlet {
    static constexpr std::string_view kName = irs::LMDirichlet::type_name();
    float mu = 2000.0f;
    bool operator==(const LmDirichlet&) const = default;
  };
  struct IndriDirichlet {
    static constexpr std::string_view kName = irs::IndriDirichlet::type_name();
    float mu = 2000.0f;
    bool operator==(const IndriDirichlet&) const = default;
  };
  struct Dfi {
    static constexpr std::string_view kName = irs::DFI::type_name();
    DfiMeasure measure = DfiMeasure::Standardized;
    bool operator==(const Dfi&) const = default;
  };

  using Params =
    std::variant<Bm25, Tfidf, RawTf, LmJm, LmDirichlet, IndriDirichlet, Dfi>;

  Params params;

  bool operator==(const ScorerOptions&) const = default;

  std::string_view Name() const noexcept {
    return std::visit(
      []<typename P>(const P&) -> std::string_view { return P::kName; },
      params);
  }

  // EXPLAIN-friendly spelling, e.g. `bm25(k1=1.2, b=0.75)`.
  std::string ToString() const;
};

// Used by the shard (writer-side WAND data) and by the runtime SearchScan.
std::unique_ptr<irs::Scorer> MakeScorer(const ScorerOptions& spec);

// Returns nullopt if any param child is non-constant; throws sdb::SqlException
// on out-of-range values or unknown scorer name.
std::optional<ScorerOptions> ExtractScorerFromBound(
  const duckdb::BoundFunctionExpression& func, std::string_view name);

// Parse `WITH (optimize_top_k = '<expr>')` via DuckDB's binder so overload
// resolution / implicit coercion match `ORDER BY BM25(...)`. Throws
// sdb::SqlException on any parse / bind / value error.
ScorerOptions ParseScorerExpression(duckdb::ClientContext& context,
                                    std::string input);

// vpack serialisation hook for Scorer. The persisted shape is a 2-tuple
// `[name, arm_fields]`: `name` discriminates which variant arm follows, and
// the arm itself is written via WriteTuple so each per-arm aggregate is
// (de)serialised positionally by boost::pfr (no per-field code here). The
// hook is picked up via ADL by the standard vpack::WriteTuple /
// vpack::ReadTuple machinery.
template<typename Context>
void VPackWrite(Context ctx, const ScorerOptions& s) {
  auto& b = ctx.vpack();
  b.openArray(true);
  b.add(s.Name());
  std::visit([&](const auto& p) { vpack::WriteTuple(b, p, ctx.arg()); },
             s.params);
  b.close();
}

template<typename Context>
void VPackRead(Context ctx, ScorerOptions& s) {
  vpack::ArrayIterator it{ctx.vpack()};
  if (!it.valid() || !(*it).isString()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "Invalid 'scorer' tuple: missing scorer name");
  }
  // 1. Discriminator -> default-construct the matching variant arm.
  const auto name = (*it).stringView();
  if (name == ScorerOptions::Bm25::kName) {
    s.params = ScorerOptions::Bm25{};
  } else if (name == ScorerOptions::Tfidf::kName) {
    s.params = ScorerOptions::Tfidf{};
  } else if (name == ScorerOptions::RawTf::kName) {
    s.params = ScorerOptions::RawTf{};
  } else if (name == ScorerOptions::LmJm::kName) {
    s.params = ScorerOptions::LmJm{};
  } else if (name == ScorerOptions::LmDirichlet::kName) {
    s.params = ScorerOptions::LmDirichlet{};
  } else if (name == ScorerOptions::IndriDirichlet::kName) {
    s.params = ScorerOptions::IndriDirichlet{};
  } else if (name == ScorerOptions::Dfi::kName) {
    s.params = ScorerOptions::Dfi{};
  } else {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Unknown 'scorer' name '", name, "'");
  }
  it.next();
  if (!it.valid()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "Invalid 'scorer' tuple: missing arm payload for '", name, "'");
  }
  // 2. Fill the active arm via the standard tuple reader (boost::pfr).
  std::visit([&](auto& p) { vpack::ReadTuple(*it, p, ctx.arg()); }, s.params);
}

}  // namespace sdb::catalog
namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<sdb::catalog::ScorerOptions::DfiMeasure>(
  sdb::catalog::ScorerOptions::DfiMeasure value) noexcept {
  using DfiMeasure = sdb::catalog::ScorerOptions::DfiMeasure;
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
