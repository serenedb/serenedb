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

#include <iresearch/analysis/geo_tokenizer.hpp>
#include <iresearch/analysis/shingle_tokenizer.hpp>
#include <iresearch/analysis/wildcard_tokenizer.hpp>
#include <iresearch/utils/geo/coding.hpp>
#include <optional>
#include <string>
#include <vector>

#include "tests_shared.hpp"
#include "token_sink_utils.hpp"
#include "tokenizer_fuzz_checks.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_specs.hpp"

namespace {

using namespace tests::fuzz;

irs::bytes_view AsBytes(std::string_view v) noexcept {
  return {reinterpret_cast<const irs::byte_type*>(v.data()), v.size()};
}

bool ReadVarint(const irs::byte_type*& p, const irs::byte_type* end,
                uint32_t& out) {
  out = 0;
  for (uint32_t shift = 0; shift <= 28; shift += 7) {
    if (p == end) {
      return false;
    }
    const auto byte = *p++;
    out |= static_cast<uint32_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) {
      return true;
    }
  }
  return false;
}

std::optional<std::vector<std::string>> DecodeWildcardStore(
  irs::bytes_view blob, std::string& error) {
  std::vector<std::string> out;
  const auto* p = blob.data();
  const auto* const end = p + blob.size();
  while (p != end) {
    uint32_t size = 0;
    if (!ReadVarint(p, end, size)) {
      error = "truncated length prefix";
      return std::nullopt;
    }
    if (static_cast<size_t>(end - p) < static_cast<size_t>(size) + 2) {
      error = "record runs past the blob";
      return std::nullopt;
    }
    if (*p != irs::analysis::WildcardTokenizer::kBoundary) {
      error = "missing leading sentinel";
      return std::nullopt;
    }
    ++p;
    out.emplace_back(reinterpret_cast<const char*>(p), size);
    p += size;
    if (*p != irs::analysis::WildcardTokenizer::kBoundary) {
      error = "missing trailing sentinel";
      return std::nullopt;
    }
    ++p;
  }
  return out;
}

std::optional<std::vector<std::string>> DecodeShingleStore(irs::bytes_view blob,
                                                           std::string& error) {
  std::vector<std::string> out;
  const auto* p = blob.data();
  const auto* const end = p + blob.size();
  while (p != end) {
    irs::bytes_view token;
    const auto* next =
      irs::analysis::ShingleTokenizer::ReadTokenChecked(p, end, token);
    if (next == nullptr) {
      error = "truncated token record";
      return std::nullopt;
    }
    out.emplace_back(reinterpret_cast<const char*>(token.data()), token.size());
    p = next;
  }
  return out;
}

std::vector<std::string> BaseTerms(irs::analysis::Tokenizer& base,
                                   std::string_view value,
                                   std::vector<uint32_t>& positions) {
  const auto res = AnalyzeValue(base, value, irs::TokenLayout::TermsPos);
  std::vector<std::string> out;
  positions.clear();
  if (!res.ok) {
    return out;
  }
  out.reserve(res.tokens.size());
  for (const auto& token : res.tokens) {
    out.push_back(token.term);
    positions.push_back(token.pos);
  }
  return out;
}

bool IsShingleSpec(const Spec& spec) {
  return spec.name.starts_with("shingle");
}

bool IsWildcardSpec(const Spec& spec) {
  return spec.name.starts_with("wildcard");
}

bool IsGeoJsonSpec(const Spec& spec) {
  return spec.name.starts_with("geojson");
}

bool IsGeoSpec(const Spec& spec) { return spec.name.starts_with("geo"); }

using DocToken = std::pair<irs::doc_id_t, Token>;
using DocStore = std::pair<irs::doc_id_t, std::string>;

struct DocFill {
  std::vector<DocToken> tokens;
  std::vector<DocStore> stores;
};

class StoreLog final : public irs::StoreSink {
 public:
  explicit StoreLog(DocFill& out) noexcept : _out{&out} {}

  void OnStore(irs::doc_id_t doc, irs::bytes_view blob) final {
    _out->stores.emplace_back(
      doc,
      std::string{reinterpret_cast<const char*>(blob.data()), blob.size()});
  }

 private:
  DocFill* _out;
};

template<typename Fill>
DocFill CollectDocs(const irs::TokenTraits& traits, irs::TokenLayout layout,
                    Fill&& fill) {
  DocFill out;
  const bool with_pos =
    layout != irs::TokenLayout::Terms && traits.explicit_pos;
  const bool with_offs = layout == irs::TokenLayout::TermsPosOffs;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    uint32_t tok = 0;
    for (const auto& run : runs) {
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        Token token;
        token.term.assign(batch.terms[tok].GetData(),
                          batch.terms[tok].GetSize());
        if (with_pos) {
          token.pos = batch.pos[tok];
        }
        if (with_offs) {
          token.offs_start = batch.offs_start[tok];
          token.offs_end = batch.offs_end[tok];
        }
        out.tokens.emplace_back(run.doc, std::move(token));
      }
    }
  };
  tests::FnTokenSink sink{layout, collect};
  StoreLog store{out};
  sink.writer.Bind(sink, &store);
  fill(sink.writer);
  sink.writer.Finish();
  return out;
}

}  // namespace

TEST(TokenizerStore, WildcardBlobDecodesToTheBaseTerms) {
  for (const auto* spec : SelectedSpecs()) {
    if (!IsWildcardSpec(*spec)) {
      continue;
    }
    SCOPED_TRACE(spec->name);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    ASSERT_TRUE(tokenizer->Traits().store);
    ASSERT_TRUE(spec->model_children)
      << "a wildcard spec must publish its base analyzer";
    auto children = spec->model_children();
    ASSERT_EQ(1u, children.size());
    auto& base = *children.front();

    const auto values = SpecCorpus(*spec, Seed(), 96);
    for (size_t i = 0; i < values.size(); ++i) {
      SCOPED_TRACE(testing::Message()
                   << "value=" << i << " " << Describe(values[i]));
      const auto res =
        AnalyzeValue(*tokenizer, values[i], irs::TokenLayout::TermsPos);
      std::vector<uint32_t> positions;
      const auto expected = BaseTerms(base, values[i], positions);
      if (!res.ok) {
        continue;
      }
      if (expected.empty()) {
        EXPECT_TRUE(res.store.empty())
          << "a base analyzer that produced nothing left a stored blob";
        continue;
      }
      std::string error;
      const auto decoded = DecodeWildcardStore(AsBytes(res.store), error);
      ASSERT_TRUE(decoded.has_value()) << "malformed wildcard store: " << error;
      ASSERT_EQ(expected.size(), decoded->size());
      for (size_t k = 0; k < expected.size(); ++k) {
        ASSERT_EQ(expected[k], (*decoded)[k]) << "term " << k;
      }
    }
  }
}

TEST(TokenizerStore, ShingleBlobDecodesToTheBaseTokensWithFillers) {
  for (const auto* spec : SelectedSpecs()) {
    if (!IsShingleSpec(*spec)) {
      continue;
    }
    SCOPED_TRACE(spec->name);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    if (!tokenizer->Traits().store) {
      continue;
    }
    ASSERT_TRUE(spec->model_children)
      << "a shingle spec must publish its base analyzer";
    auto children = spec->model_children();
    ASSERT_EQ(1u, children.size());
    auto& base = *children.front();
    const auto filler = spec->params.delim == 0
                          ? std::string{}
                          : std::string(1, spec->params.delim);

    const auto values = SpecCorpus(*spec, Seed(), 96);
    for (size_t i = 0; i < values.size(); ++i) {
      SCOPED_TRACE(testing::Message()
                   << "value=" << i << " " << Describe(values[i]));
      const auto res =
        AnalyzeValue(*tokenizer, values[i], irs::TokenLayout::TermsPos);
      if (!res.ok || res.store.empty()) {
        continue;
      }
      std::vector<uint32_t> positions;
      const auto base_terms = BaseTerms(base, values[i], positions);

      std::vector<std::string> expected;
      uint32_t previous = 0;
      for (size_t k = 0; k < base_terms.size(); ++k) {
        for (uint32_t gap =
               positions[k] > previous ? positions[k] - previous - 1 : 0;
             gap != 0; --gap) {
          expected.push_back(filler);
        }
        previous = positions[k];
        expected.push_back(base_terms[k]);
      }

      std::string error;
      const auto decoded = DecodeShingleStore(AsBytes(res.store), error);
      ASSERT_TRUE(decoded.has_value()) << "malformed shingle store: " << error;
      ASSERT_EQ(expected.size(), decoded->size());
      for (size_t k = 0; k < expected.size(); ++k) {
        ASSERT_EQ(expected[k], (*decoded)[k]) << "token " << k;
      }
    }
  }
}

TEST(TokenizerStore, GeoBlobCarriesTheDeclaredCoding) {
  using irs::analysis::GeoJsonTokenizer;
  namespace coding = irs::geo::coding;

  for (const auto* spec : SelectedSpecs()) {
    if (!IsGeoJsonSpec(*spec)) {
      continue;
    }
    SCOPED_TRACE(spec->name);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    const auto* geo = dynamic_cast<const GeoJsonTokenizer*>(tokenizer.get());
    ASSERT_NE(nullptr, geo);
    const auto declared = geo->coding();
    ASSERT_EQ(declared != GeoJsonTokenizer::Coding::Source,
              tokenizer->Traits().store);
    if (!tokenizer->Traits().store) {
      continue;
    }
    const auto options =
      static_cast<coding::Options>(std::to_underlying(declared));

    const auto values = SpecCorpus(*spec, Seed(), 96);
    for (size_t i = 0; i < values.size(); ++i) {
      SCOPED_TRACE(testing::Message()
                   << "value=" << i << " " << Describe(values[i]));
      const auto res =
        AnalyzeValue(*tokenizer, values[i], irs::TokenLayout::Terms);
      if (!res.ok) {
        ASSERT_TRUE(res.store.empty());
        continue;
      }
      if (res.tokens.empty() && res.store.empty()) {
        continue;
      }
      ASSERT_FALSE(res.store.empty()) << "a parsed shape left no stored blob";

      const auto shape_type = geo->shapeType();
      if (shape_type == GeoJsonTokenizer::Type::Shape) {
        const auto tag = static_cast<uint8_t>(res.store.front());
        ASSERT_EQ(std::to_underlying(options), coding::ToPoint(tag))
          << "stored tag names a different coding";
        ASSERT_LT(coding::ToType(tag) >> 5, 5u) << "stored tag names no shape";
      } else if (shape_type == GeoJsonTokenizer::Type::Centroid) {
        ASSERT_EQ(coding::ToSize(options), res.store.size())
          << "centroid blob is not one coded point";
      }
    }
  }
}

TEST(TokenizerStore, FillRowMatchesPerValueFills) {
  constexpr size_t kRowSizes[] = {3, 1, 0, 2, 4};
  for (const auto* spec : SelectedSpecs()) {
    if (IsGeoSpec(*spec)) {
      continue;
    }
    SCOPED_TRACE(spec->name);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    const auto traits = tokenizer->Traits();

    const auto values = SpecCorpus(*spec, Seed(), 48);
    std::vector<std::vector<duckdb::string_t>> rows;
    for (size_t v = 0, r = 0; v < values.size(); ++r) {
      auto& row = rows.emplace_back();
      for (size_t k = kRowSizes[r % std::size(kRowSizes)];
           k != 0 && v < values.size(); --k) {
        row.push_back(tests::ToStringT(values[v++]));
      }
    }

    for (const auto layout : DeclaredLayouts(traits)) {
      SCOPED_TRACE(LayoutName(layout));
      const auto per_value =
        CollectDocs(traits, layout, [&](irs::TokenSink& w) {
          irs::doc_id_t doc = irs::doc_limits::min();
          for (const auto& row : rows) {
            for (const auto& value : row) {
              tokenizer->Fill(value, doc, w, {layout});
            }
            ++doc;
          }
        });
      const auto per_row = CollectDocs(traits, layout, [&](irs::TokenSink& w) {
        irs::doc_id_t doc = irs::doc_limits::min();
        for (const auto& row : rows) {
          const auto fmt = tests::ValuesFormat(row);
          tokenizer->FillRow(fmt, 0, static_cast<uint32_t>(row.size()), doc++,
                             w, {layout});
        }
      });
      ASSERT_EQ(per_value.tokens, per_row.tokens);

      std::vector<DocStore> joined;
      for (const auto& [doc, blob] : per_value.stores) {
        if (!joined.empty() && joined.back().first == doc) {
          joined.back().second += blob;
        } else {
          joined.emplace_back(doc, blob);
        }
      }
      ASSERT_EQ(joined, per_row.stores);
    }
  }
}
