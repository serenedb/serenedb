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

#include "tokenizer_fuzz_checks.hpp"

#include <simdutf.h>

#include <algorithm>
#include <cstdlib>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <format>
#include <set>

#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_mutator.hpp"

namespace tests::fuzz {
namespace {

constexpr size_t kMaxBlock = 2048;

struct BlockShape {
  size_t width;
  bool nulls;
  BlockMode mode;
};

constexpr BlockShape kBlockShapes[] = {
  {1, false, BlockMode::Flat},    {2, false, BlockMode::Flat},
  {2, false, BlockMode::Reverse}, {3, false, BlockMode::Flat},
  {3, false, BlockMode::Reverse}, {7, true, BlockMode::Flat},
  {13, false, BlockMode::Repeat}, {64, false, BlockMode::Reverse},
  {64, true, BlockMode::Repeat},  {100, true, BlockMode::Reverse},
  {1024, false, BlockMode::Flat}, {kMaxBlock, true, BlockMode::Flat}};

duckdb::string_t Handle(std::string_view value) {
  if (value.empty()) {
    return duckdb::string_t{};
  }
  return duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())};
}

class BlockCollector final : public irs::TokenConsumer, public irs::StoreSink {
 public:
  BlockCollector(irs::TokenLayout layout, const irs::TokenTraits& producer,
                 irs::doc_id_t first_doc, size_t nrows)
    : _layout{layout},
      _first_doc{first_doc},
      _dense{!producer.explicit_pos},
      _offsets{producer.offsets},
      _stable{producer.stable} {
    rows.resize(nrows);
    writer.Bind(*this, this);
  }

  void Consume(irs::TokenBatch& batch, irs::DocRuns runs) final {
    uint32_t tok = 0;
    for (const auto& run : runs) {
      const auto idx = static_cast<size_t>(run.doc - _first_doc);
      if (idx >= rows.size()) {
        overflow = true;
        return;
      }
      auto& out = rows[idx];
      for (uint32_t i = 0; i < run.ntokens; ++i, ++tok) {
        Token token;
        const auto& term = batch.terms[tok];
        token.term.assign(term.GetData(), term.GetSize());
        if (_stable) {
          raw.push_back(term);
          raw_expected.push_back(token.term);
        }
        if (_layout != irs::TokenLayout::Terms) {
          token.pos = _dense ? static_cast<uint32_t>(out.tokens.size() + 1)
                             : batch.pos[tok];
        }
        if (_layout == irs::TokenLayout::TermsPosOffs && _offsets) {
          token.offs_start = batch.offs_start[tok];
          token.offs_end = batch.offs_end[tok];
        }
        out.tokens.push_back(std::move(token));
      }
      out.ok = true;
    }
    if (tok != batch.count) {
      ragged = true;
    }
  }

  void OnStore(irs::doc_id_t doc, irs::bytes_view blob) final {
    const auto idx = static_cast<size_t>(doc - _first_doc);
    if (idx >= rows.size()) {
      overflow = true;
      return;
    }
    rows[idx].store.assign(reinterpret_cast<const char*>(blob.data()),
                           blob.size());
  }

  irs::TokenSink writer;
  std::vector<Result> rows;
  std::vector<duckdb::string_t> raw;
  std::vector<std::string> raw_expected;
  bool overflow = false;
  bool ragged = false;

 private:
  irs::TokenLayout _layout;
  irs::doc_id_t _first_doc;
  bool _dense;
  bool _offsets;
  bool _stable;
};

std::optional<std::string> SameTokens(const Result& lhs, const Result& rhs,
                                      std::string_view what) {
  if (lhs.ok != rhs.ok) {
    return std::format("{}: ok {} vs {}", what, lhs.ok, rhs.ok);
  }
  if (lhs.tokens.size() != rhs.tokens.size()) {
    return std::format("{}: {} tokens vs {}", what, lhs.tokens.size(),
                       rhs.tokens.size());
  }
  for (size_t i = 0; i < lhs.tokens.size(); ++i) {
    if (lhs.tokens[i] != rhs.tokens[i]) {
      return std::format("{}: token {} is {} vs {}", what, i,
                         Describe(lhs.tokens[i].term),
                         Describe(rhs.tokens[i].term)) +
             std::format(" pos {}/{} offs [{},{}) vs [{},{})",
                         lhs.tokens[i].pos, rhs.tokens[i].pos,
                         lhs.tokens[i].offs_start, lhs.tokens[i].offs_end,
                         rhs.tokens[i].offs_start, rhs.tokens[i].offs_end);
    }
  }
  if (lhs.store != rhs.store) {
    return std::format("{}: store {} vs {}", what, Describe(lhs.store),
                       Describe(rhs.store));
  }
  return std::nullopt;
}

std::optional<std::string> SameTerms(const Result& lhs, const Result& rhs,
                                     std::string_view what) {
  if (lhs.ok != rhs.ok) {
    return std::format("{}: ok {} vs {}", what, lhs.ok, rhs.ok);
  }
  if (lhs.tokens.size() != rhs.tokens.size()) {
    return std::format("{}: {} tokens vs {}", what, lhs.tokens.size(),
                       rhs.tokens.size());
  }
  for (size_t i = 0; i < lhs.tokens.size(); ++i) {
    if (lhs.tokens[i].term != rhs.tokens[i].term) {
      return std::format("{}: term {} is {} vs {}", what, i,
                         Describe(lhs.tokens[i].term),
                         Describe(rhs.tokens[i].term));
    }
  }
  return std::nullopt;
}

uint64_t Mix(uint64_t x) {
  x += 0x9E3779B97F4A7C15ull;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
  return x ^ (x >> 31);
}

uint64_t HashBytes(std::string_view s, uint64_t seed) {
  uint64_t h = Mix(seed ^ s.size());
  for (const char c : s) {
    h = Mix(h ^ static_cast<unsigned char>(c));
  }
  return h;
}

}  // namespace

std::ostream& operator<<(std::ostream& os, const Token& token) {
  return os << "{term=" << Describe(token.term) << " pos=" << token.pos
            << " offs=[" << token.offs_start << ", " << token.offs_end << ")}";
}

std::vector<irs::TokenLayout> DeclaredLayouts(const irs::TokenTraits& traits) {
  if (traits.offsets) {
    return {irs::TokenLayout::Terms, irs::TokenLayout::TermsPos,
            irs::TokenLayout::TermsPosOffs};
  }
  return {irs::TokenLayout::Terms, irs::TokenLayout::TermsPos};
}

std::string_view BlockModeName(BlockMode mode) noexcept {
  switch (mode) {
    case BlockMode::Flat:
      return "flat";
    case BlockMode::Reverse:
      return "reverse";
    case BlockMode::Repeat:
      return "repeat";
    case BlockMode::Constant:
      return "constant";
  }
  return "unknown";
}

std::string_view LayoutName(irs::TokenLayout layout) noexcept {
  switch (layout) {
    case irs::TokenLayout::Terms:
      return "terms";
    case irs::TokenLayout::TermsPos:
      return "terms_pos";
    case irs::TokenLayout::TermsPosOffs:
      return "terms_pos_offs";
  }
  return "unknown";
}

bool IsAscii(std::string_view value) noexcept {
  return std::ranges::none_of(
    value, [](char c) { return static_cast<unsigned char>(c) >= 0x80; });
}

bool IsValidUtf8(std::string_view value) noexcept {
  return value.empty() || simdutf::validate_utf8(value.data(), value.size());
}

Result AnalyzeValue(irs::analysis::Tokenizer& tokenizer, std::string_view value,
                    irs::TokenLayout layout, irs::BlockTraits hint) {
  Result out;
  const auto handle = Handle(value);
  irs::ResolveLayout(layout, [&]<irs::TokenLayout L>() {
    irs::ValueAnalyzer analyzer;
    auto tokens = [&] {
      if constexpr (L == irs::TokenLayout::Terms) {
        return irs::ValueTokens<L>{};
      } else {
        return irs::ValueTokens<L>{tokenizer.Traits()};
      }
    }();
    out.ok = analyzer.Analyze(tokenizer, handle, tokens, hint);
    const auto terms = tokens.terms();
    out.tokens.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
      Token tok;
      tok.term.assign(terms[i].GetData(), terms[i].GetSize());
      if constexpr (L != irs::TokenLayout::Terms) {
        tok.pos = tokens.pos()[i];
      }
      if constexpr (L == irs::TokenLayout::TermsPosOffs) {
        if (!tokens.offs_start().empty()) {
          tok.offs_start = tokens.offs_start()[i];
          tok.offs_end = tokens.offs_end()[i];
        }
      }
      out.tokens.push_back(std::move(tok));
    }
    const auto store = tokens.store();
    out.store.assign(reinterpret_cast<const char*>(store.data()), store.size());
  });
  return out;
}

std::vector<Result> FillBlock(irs::analysis::Tokenizer& tokenizer,
                              std::span<const Row> rows,
                              irs::TokenLayout layout, BlockMode mode,
                              std::string* error) {
  const auto count = static_cast<uint32_t>(rows.size());
  const auto first_doc = irs::doc_limits::min();
  BlockCollector collector{layout, tokenizer.Traits(), first_doc, rows.size()};
  if (count == 0) {
    return std::move(collector.rows);
  }
  if (mode == BlockMode::Constant) {
    for (const auto& row : rows) {
      if (row.value != rows[0].value) {
        mode = BlockMode::Repeat;
        break;
      }
    }
  }

  duckdb::Vector vec{duckdb::LogicalType::VARCHAR};
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
  auto& validity = duckdb::FlatVector::ValidityMutable(vec);
  std::vector<uint32_t> physical_of(count, 0);
  if (mode == BlockMode::Repeat) {
    std::vector<const std::string*> seen;
    for (uint32_t i = 0; i < count; ++i) {
      auto it = std::ranges::find(seen, rows[i].value);
      if (it == seen.end()) {
        seen.push_back(rows[i].value);
        it = std::prev(seen.end());
      }
      physical_of[i] = static_cast<uint32_t>(it - seen.begin());
    }
  } else if (mode == BlockMode::Reverse) {
    for (uint32_t i = 0; i < count; ++i) {
      physical_of[i] = count - 1 - i;
    }
  } else if (mode == BlockMode::Flat) {
    for (uint32_t i = 0; i < count; ++i) {
      physical_of[i] = i;
    }
  }

  for (uint32_t i = 0; i < count; ++i) {
    const auto physical = physical_of[i];
    if (rows[i].value == nullptr) {
      slots[physical] = duckdb::string_t{};
      validity.SetInvalid(physical);
      continue;
    }
    slots[physical] = Handle(*rows[i].value);
  }

  if (mode == BlockMode::Constant) {
    vec.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  } else if (mode != BlockMode::Flat) {
    duckdb::SelectionVector sel{count};
    for (uint32_t i = 0; i < count; ++i) {
      sel.set_index(i, physical_of[i]);
    }
    vec.Slice(sel, count);
  }

  duckdb::UnifiedVectorFormat fmt;
  vec.ToUnifiedFormat(count, fmt);
  tokenizer.Fill(fmt, count, first_doc, collector.writer, {layout});
  collector.writer.Finish();

  if (error != nullptr) {
    if (collector.overflow) {
      *error = "block fill emitted a doc outside the block";
    } else if (collector.ragged) {
      *error = "doc runs did not cover the whole batch";
    } else if (tokenizer.Traits().stable) {
      for (size_t i = 0; i < collector.raw.size(); ++i) {
        const auto& term = collector.raw[i];
        if (collector.raw_expected[i] !=
            std::string_view(term.GetData(), term.GetSize())) {
          *error = std::format(
            "stable term {} moved after Finish: {} -> {}", i,
            Describe(collector.raw_expected[i]),
            Describe(std::string_view(term.GetData(), term.GetSize())));
          break;
        }
      }
    }
  }
  return std::move(collector.rows);
}

std::optional<std::string> ValueInvariants(const irs::TokenTraits& traits,
                                           std::string_view value,
                                           irs::TokenLayout layout,
                                           const Result& res) {
  if (!res.ok) {
    if (!res.tokens.empty()) {
      return "a rejected value produced tokens";
    }
    if (!res.store.empty()) {
      return "a rejected value produced a stored blob";
    }
    return std::nullopt;
  }
  if (traits.unique && res.tokens.size() > 1) {
    return std::format("unique traits promise at most one token, got {}",
                       res.tokens.size());
  }
  if (traits.keyword) {
    if (res.tokens.size() > 1) {
      return "keyword traits promise at most one token";
    }
    if (!res.tokens.empty() && res.tokens[0].term != value) {
      return std::format("keyword term {} is not the value {}",
                         Describe(res.tokens[0].term), Describe(value));
    }
  }
  if (layout != irs::TokenLayout::Terms) {
    for (size_t i = 0; i < res.tokens.size(); ++i) {
      const auto pos = res.tokens[i].pos;
      if (pos < irs::pos_limits::min()) {
        return std::format("token {} has position {} below the minimum", i,
                           pos);
      }
      if (pos >= irs::pos_limits::eof()) {
        return std::format("token {} has position {} at eof", i, pos);
      }
      if (i != 0 && pos < res.tokens[i - 1].pos) {
        return std::format("token {} position {} goes back from {}", i, pos,
                           res.tokens[i - 1].pos);
      }
      if (!traits.explicit_pos && pos != i + 1) {
        return std::format("dense producer gave token {} position {}", i, pos);
      }
    }
  }
  if (layout == irs::TokenLayout::TermsPosOffs && traits.offsets) {
    uint32_t last_start = 0;
    for (size_t i = 0; i < res.tokens.size(); ++i) {
      const auto& tok = res.tokens[i];
      if (tok.offs_start > tok.offs_end) {
        return std::format("token {} has offsets [{}, {})", i, tok.offs_start,
                           tok.offs_end);
      }
      if (tok.offs_end > value.size()) {
        return std::format("token {} ends at {} past the value size {}", i,
                           tok.offs_end, value.size());
      }
      if (tok.offs_start < last_start) {
        return std::format("token {} starts at {} before the previous {}", i,
                           tok.offs_start, last_start);
      }
      last_start = tok.offs_start;
    }
  }
  return std::nullopt;
}

Probe::Probe(const Spec& spec) : _spec{&spec} {
  _primary = Make(spec);
  if (!_primary) {
    return;
  }
  _shadow = Make(spec);
  _blocked = Make(spec);
  if (spec.model_children) {
    _children = spec.model_children();
  }
  _traits = _primary->Traits();
  _layouts = DeclaredLayouts(_traits);
}

std::optional<std::string> Probe::CheckLayout(std::string_view value,
                                              irs::TokenLayout layout,
                                              bool full, Result& out) {
  const std::string owned{value};
  out = AnalyzeValue(*_primary, value, layout);
  const auto tag = [&](std::string_view what) {
    return std::format("[{}] {}", LayoutName(layout), what);
  };
  if (auto err = ValueInvariants(_traits, value, layout, out)) {
    return tag(*err);
  }
  const auto again = AnalyzeValue(*_primary, value, layout);
  if (auto err = SameTokens(out, again, tag("repeated fill"))) {
    return err;
  }
  const auto fresh = AnalyzeValue(*_shadow, value, layout);
  if (auto err = SameTokens(out, fresh, tag("second instance"))) {
    return err;
  }
  if (IsAscii(value)) {
    const auto hinted = AnalyzeValue(*_shadow, value, layout, {.ascii = true});
    if (auto err = SameTokens(out, hinted, tag("ascii block hint"))) {
      return err;
    }
  }

  const auto compare_block =
    [&](Result& got, std::string_view what) -> std::optional<std::string> {
    if (!out.ok) {
      if (!got.tokens.empty()) {
        return tag(std::format("{}: rejected value produced {} tokens", what,
                               got.tokens.size()));
      }
      return std::nullopt;
    }
    got.ok = true;
    Result want = out;
    return SameTokens(want, got, tag(what));
  };

  const Row single[1] = {{&owned}};
  std::string block_error;
  auto blocked =
    FillBlock(*_blocked, single, layout, BlockMode::Flat, &block_error);
  if (!block_error.empty()) {
    return tag(block_error);
  }
  if (auto err = compare_block(blocked[0], "block fill")) {
    return err;
  }
  if (!full) {
    return std::nullopt;
  }

  auto reversed =
    FillBlock(*_blocked, single, layout, BlockMode::Reverse, &block_error);
  if (!block_error.empty()) {
    return tag(block_error);
  }
  if (auto err = compare_block(reversed[0], "selected block")) {
    return err;
  }
  const Row with_null[3] = {{nullptr}, {&owned}, {nullptr}};
  auto mixed =
    FillBlock(*_blocked, with_null, layout, BlockMode::Flat, &block_error);
  if (!block_error.empty()) {
    return tag(block_error);
  }
  if (!mixed[0].tokens.empty() || !mixed[2].tokens.empty()) {
    return tag("a null row produced tokens");
  }
  if (auto err = compare_block(mixed[1], "block with nulls")) {
    return err;
  }

  static const std::string kLeadingWords = "the quick brown fox";
  const Row after_words[2] = {{&kLeadingWords}, {&owned}};
  auto trailing =
    FillBlock(*_blocked, after_words, layout, BlockMode::Flat, &block_error);
  if (!block_error.empty()) {
    return tag(block_error);
  }
  if (auto err = compare_block(trailing[1], "block following another value")) {
    return err;
  }

  static const std::string kNonAscii = "\xCF\x89\xCF\x89";
  const Row mixed_ascii[2] = {{&owned}, {&kNonAscii}};
  auto mixed_block =
    FillBlock(*_blocked, mixed_ascii, layout, BlockMode::Flat, &block_error);
  if (!block_error.empty()) {
    return tag(block_error);
  }
  if (auto err =
        compare_block(mixed_block[0], "block sharing a non-ascii row")) {
    return err;
  }

  const Row repeated[3] = {{&owned}, {&owned}, {&owned}};
  for (const auto shared : {BlockMode::Constant, BlockMode::Repeat}) {
    auto same = FillBlock(*_blocked, repeated, layout, shared, &block_error);
    if (!block_error.empty()) {
      return tag(block_error);
    }
    for (size_t i = 0; i < same.size(); ++i) {
      if (auto err = compare_block(
            same[i],
            std::format("{} block row {}", BlockModeName(shared), i))) {
        return err;
      }
    }
  }
  return std::nullopt;
}

std::optional<std::string> Probe::operator()(std::string_view value,
                                             bool full) {
  if (_spec->utf8_only && !IsValidUtf8(value)) {
    return std::nullopt;
  }
  std::vector<Result> per_layout;
  per_layout.reserve(_layouts.size());
  const size_t first = full ? 0 : _layouts.size() - 1;
  for (size_t i = first; i < _layouts.size(); ++i) {
    Result res;
    if (auto err = CheckLayout(value, _layouts[i], full, res)) {
      return err;
    }
    per_layout.push_back(std::move(res));
  }

  for (size_t i = 1; i < per_layout.size(); ++i) {
    if (auto err = SameTerms(per_layout[0], per_layout[i],
                             std::format("layout differential {} vs {}",
                                         LayoutName(_layouts[first + i]),
                                         LayoutName(_layouts[first])))) {
      return err;
    }
  }

  const auto& rich = per_layout.back();
  size_t term_bytes = 0;
  size_t max_term = 0;
  for (const auto& t : rich.tokens) {
    term_bytes += t.term.size();
    max_term = std::max(max_term, t.term.size());
  }
  _behaviour = BehaviourClass(!rich.ok, rich.tokens.size(), term_bytes,
                              max_term, rich.store.size(),
                              rich.tokens.empty() ? 0 : rich.tokens.back().pos);

  if (_spec->model != Model::None && rich.ok) {
    const auto model =
      ModelTokens(_spec->model, _spec->params, _children, value);
    if (model) {
      if (model->size() != rich.tokens.size()) {
        return std::format("model {} expects {} tokens, got {}",
                           ModelName(_spec->model), model->size(),
                           rich.tokens.size());
      }
      for (size_t i = 0; i < model->size(); ++i) {
        if ((*model)[i].term != rich.tokens[i].term) {
          return std::format(
            "model {} expects term {} = {}, got {}", ModelName(_spec->model), i,
            Describe((*model)[i].term), Describe(rich.tokens[i].term));
        }
        if (_traits.explicit_pos && _spec->model != Model::Chain &&
            (*model)[i].pos != rich.tokens[i].pos) {
          return std::format("model {} expects position {} = {}, got {}",
                             ModelName(_spec->model), i, (*model)[i].pos,
                             rich.tokens[i].pos);
        }
      }
    }
  }
  return std::nullopt;
}

void PostingsDigest::Add(std::string_view term, uint64_t row, uint32_t freq,
                         std::span<const uint32_t> pos,
                         std::span<const uint32_t> offs_start,
                         std::span<const uint32_t> offs_end) {
  uint64_t h = HashBytes(term, 0x1234567);
  h = Mix(h ^ row);
  h = Mix(h ^ freq);
  for (const auto p : pos) {
    h = Mix(h ^ (uint64_t{p} << 1));
  }
  for (size_t i = 0; i < offs_start.size(); ++i) {
    h = Mix(h ^ (uint64_t{offs_start[i]} << 32 | offs_end[i]));
  }
  lane0 += h;
  lane1 ^= Mix(h ^ 0xA5A5A5A5A5A5A5A5ull);
  ++entries;
  occurrences += freq;
}

std::string PostingsDigest::Describe() const {
  return std::format("entries={} occurrences={} lane0={:#x} lane1={:#x}",
                     entries, occurrences, lane0, lane1);
}

void FoldValue(PostingsDigest& digest, uint64_t row, const Result& res,
               bool with_freq, bool with_pos, bool with_offs) {
  if (!res.ok || res.tokens.empty()) {
    return;
  }
  std::vector<size_t> order(res.tokens.size());
  for (size_t i = 0; i < order.size(); ++i) {
    order[i] = i;
  }
  std::ranges::stable_sort(order, [&](size_t a, size_t b) {
    return res.tokens[a].term < res.tokens[b].term;
  });
  std::vector<uint32_t> pos;
  std::vector<uint32_t> starts;
  std::vector<uint32_t> ends;
  size_t i = 0;
  while (i < order.size()) {
    const auto& term = res.tokens[order[i]].term;
    pos.clear();
    starts.clear();
    ends.clear();
    size_t j = i;
    while (j < order.size() && res.tokens[order[j]].term == term) {
      if (with_pos) {
        pos.push_back(res.tokens[order[j]].pos);
      }
      if (with_offs) {
        starts.push_back(res.tokens[order[j]].offs_start);
        ends.push_back(res.tokens[order[j]].offs_end);
      }
      ++j;
    }
    std::ranges::sort(pos);
    digest.Add(term, row, with_freq ? static_cast<uint32_t>(j - i) : 1u, pos,
               starts, ends);
    i = j;
  }
}

size_t Drain(irs::analysis::Tokenizer& tokenizer,
             std::span<const std::string> values, irs::TokenLayout layout,
             size_t width) {
  struct Counter final : irs::TokenConsumer {
    void Consume(irs::TokenBatch& batch, irs::DocRuns) final {
      for (uint32_t i = 0; i < batch.count; ++i) {
        bytes += batch.terms[i].GetSize();
      }
      tokens += batch.count;
    }
    size_t tokens = 0;
    size_t bytes = 0;
  } counter;

  irs::TokenSink writer;
  writer.Bind(counter, nullptr);
  const auto step = std::max<size_t>(1, std::min(width, kMaxBlock));
  duckdb::Vector vec{duckdb::LogicalType::VARCHAR};
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
  for (size_t base = 0; base < values.size(); base += step) {
    const auto n = static_cast<uint32_t>(std::min(step, values.size() - base));
    for (uint32_t i = 0; i < n; ++i) {
      slots[i] = Handle(values[base + i]);
    }
    duckdb::UnifiedVectorFormat fmt;
    vec.ToUnifiedFormat(n, fmt);
    tokenizer.Fill(fmt, n, irs::doc_limits::min(), writer, {layout});
    writer.Finish();
  }
  return counter.tokens;
}

uint64_t Seed() { return EnvU64("TOKENIZER_FUZZ_SEED", 0x5EEDC0FFEEull); }

std::vector<const Spec*> SelectedSpecs() {
  const char* only = std::getenv("TOKENIZER_FUZZ_ONLY");
  std::vector<const Spec*> out;
  for (const auto& spec : AllSpecs()) {
    if (only != nullptr && *only != '\0' &&
        spec.name.find(only) == std::string::npos) {
      continue;
    }
    out.push_back(&spec);
  }
  return out;
}

std::vector<const Spec*> SelectedFamilies() {
  std::set<std::string_view> seen;
  std::vector<const Spec*> out;
  for (const auto* spec : SelectedSpecs()) {
    const std::string_view name{spec->name};
    const auto family = name.substr(0, name.find('['));
    if (seen.insert(family).second) {
      out.push_back(spec);
    }
  }
  return out;
}

size_t ValueBudget(const Spec& spec, size_t base) {
  return std::max<size_t>(16, base / std::max<uint32_t>(1, spec.cost));
}

size_t SizeCap(const Spec& spec) {
  return spec.cost >= 8   ? 256
         : spec.cost >= 4 ? 1024
         : spec.cost >= 2 ? 4096
                          : 8192;
}

std::vector<std::string> SpecCorpus(const Spec& spec, uint64_t seed,
                                    size_t random_count) {
  const auto size_cap = SizeCap(spec);
  auto values = MakeCorpus(seed, random_count, spec.native, size_cap);
  for (const auto& d : spec.dict) {
    values.push_back(d);
  }
  if (spec.cost > 1) {
    std::erase_if(
      values, [size_cap](const std::string& v) { return v.size() > size_cap; });
  }
  if (spec.utf8_only) {
    std::erase_if(values, [](const std::string& v) { return !IsValidUtf8(v); });
  }
  return values;
}

void CheckSpec(const Spec& spec, std::span<const std::string> values) {
  Probe probe{spec};
  ASSERT_TRUE(probe.valid()) << spec.name;
  for (size_t i = 0; i < values.size(); ++i) {
    const auto err = probe(values[i], /*full=*/true);
    ASSERT_FALSE(err.has_value()) << spec.name << ": " << *err << "\n  value "
                                  << i << ": " << Describe(values[i]);
  }
}

void CheckSpecBlocks(const Spec& spec, std::span<const std::string> values) {
  auto reference = Make(spec);
  ASSERT_NE(nullptr, reference) << spec.name;
  const auto traits = reference->Traits();

  for (const auto layout : DeclaredLayouts(traits)) {
    SCOPED_TRACE(testing::Message() << "layout=" << LayoutName(layout));
    std::vector<Result> expected;
    expected.reserve(values.size());
    for (const auto& v : values) {
      expected.push_back(AnalyzeValue(*reference, v, layout));
    }

    for (const auto& shape : kBlockShapes) {
      SCOPED_TRACE(testing::Message()
                   << "width=" << shape.width << " nulls=" << shape.nulls
                   << " mode=" << BlockModeName(shape.mode));
      auto blocked = Make(spec);
      ASSERT_NE(nullptr, blocked) << spec.name;
      const auto width = std::min(shape.width, kMaxBlock);
      for (size_t base = 0; base < values.size(); base += width) {
        const auto n = std::min(width, values.size() - base);
        std::vector<Row> rows(n);
        for (size_t i = 0; i < n; ++i) {
          const bool null_row = shape.nulls && (base + i) % 3 == 0;
          rows[i].value = null_row ? nullptr : &values[base + i];
        }
        std::string error;
        const auto got = FillBlock(*blocked, rows, layout, shape.mode, &error);
        ASSERT_TRUE(error.empty()) << error;
        ASSERT_EQ(n, got.size());
        for (size_t i = 0; i < n; ++i) {
          SCOPED_TRACE(testing::Message() << "row=" << (base + i) << " "
                                          << Describe(values[base + i]));
          if (rows[i].value == nullptr) {
            ASSERT_TRUE(got[i].tokens.empty())
              << "a null row must produce no tokens";
            continue;
          }
          const auto& want = expected[base + i];
          if ((want.ok ? want.tokens.size() : 0u) != got[i].tokens.size()) {
            std::string detail;
            for (const auto& tok : got[i].tokens) {
              detail += " got=" + Describe(tok.term);
            }
            for (const auto& tok : want.tokens) {
              detail += " want=" + Describe(tok.term);
            }
            FAIL() << "block fill token count: " << got[i].tokens.size()
                   << " vs " << (want.ok ? want.tokens.size() : 0u) << detail;
          }
          if (!want.ok) {
            continue;
          }
          for (size_t k = 0; k < want.tokens.size(); ++k) {
            ASSERT_EQ(want.tokens[k], got[i].tokens[k])
              << "block fill token=" << k;
          }
          if (traits.store) {
            ASSERT_EQ(want.store, got[i].store) << "block fill store";
          }
        }
      }
    }
  }
}

void CheckSpecStableTerms(const Spec& spec,
                          std::span<const std::string> values) {
  auto tokenizer = Make(spec);
  ASSERT_NE(nullptr, tokenizer) << spec.name;
  if (!tokenizer->Traits().stable) {
    return;
  }
  std::vector<Row> rows(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    rows[i].value = &values[i];
  }
  for (const auto layout : DeclaredLayouts(tokenizer->Traits())) {
    const auto width =
      std::min<size_t>(std::max<size_t>(values.size(), 1), kMaxBlock);
    for (size_t base = 0; base < rows.size(); base += width) {
      const auto n = std::min(width, rows.size() - base);
      std::string error;
      FillBlock(*tokenizer, std::span{rows}.subspan(base, n), layout,
                BlockMode::Flat, &error);
      ASSERT_TRUE(error.empty()) << spec.name << ": " << error;
    }
  }
}

}  // namespace tests::fuzz
