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

#include "iresearch/search/detail/phrase_verify.hpp"

#include <absl/algorithm/container.h>

#include <numeric>

#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs {
namespace {

class NoTokensSource final : public PhraseTokenSource {
 public:
  bool Load(doc_id_t, PhraseDocTokens&) final { return false; }
};

class StoredValueSource final : public PhraseTokenSource {
 public:
  StoredValueSource(const ColReader& col_reader, const ColumnReader& column,
                    analysis::Tokenizer::ptr tokenizer)
    : _ctx{col_reader},
      _column{&column},
      _state{column.InitScan(_ctx)},
      _out{column.Type()},
      _sel{1},
      _tokenizer{std::move(tokenizer)},
      _tokens{_tokenizer->Traits()} {
    _sel.set_index(0, 0);
  }

  bool Load(doc_id_t doc, PhraseDocTokens& out) final {
    out.Clear();
    duckdb::string_t value;
    if (!Fetch(doc, value) || !_analyzer.Analyze(*_tokenizer, value, _tokens)) {
      return false;
    }
    const auto terms = _tokens.terms();
    const auto positions = _tokens.pos();
    for (size_t i = 0; i != terms.size(); ++i) {
      out.Push(AsBytesView(terms[i]), positions[i]);
    }
    return !terms.empty();
  }

 private:
  static constexpr uint32_t kResetEvery = 1024;

  bool Fetch(doc_id_t doc, duckdb::string_t& value) {
    const uint64_t row = doc - doc_limits::min();
    if (row >= _column->RowCount()) {
      return false;
    }
    if (_loads++ % kResetEvery == 0) {
      _out.Reset();
    }
    auto& values = _out.vector;
    _column->GatherDense(_state, row, _sel, 1, 1, values);
    duckdb::UnifiedVectorFormat format;
    values.ToUnifiedFormat(1, format);
    const auto idx = format.sel->get_index(0);
    if (!format.validity.RowIsValid(idx)) {
      return false;
    }
    value = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format)[idx];
    return true;
  }

  ReadContext _ctx;
  const ColumnReader* _column;
  ColumnReader::ScanState _state;
  ColumnReader::VectorScratch _out;
  duckdb::SelectionVector _sel;
  analysis::Tokenizer::ptr _tokenizer;
  ValueAnalyzer _analyzer;
  ValueTokens<TokenLayout::TermsPos> _tokens;
  uint32_t _loads = 0;
};

bool Dense(const PhraseDocTokens& doc) noexcept {
  return doc.positions.empty() ||
         doc.positions.back() - doc.positions.front() + 1 ==
           doc.positions.size();
}

}  // namespace

bool SameVerifier(const PhraseVerifier* lhs,
                  const PhraseVerifier* rhs) noexcept {
  if (lhs == rhs) {
    return true;
  }
  if (!lhs || !rhs) {
    return false;
  }
  return *lhs == *rhs;
}

bool PhraseVerifier::operator==(const PhraseVerifier& rhs) const noexcept {
  const auto* lhs_spec = Spec();
  const auto* rhs_spec = rhs.Spec();
  if (!lhs_spec != !rhs_spec) {
    return false;
  }
  if (lhs_spec && !(*lhs_spec == *rhs_spec)) {
    return false;
  }
  return _source->Equals(*rhs._source);
}

std::unique_ptr<PhraseTokenSource> StoredValueSourceFactory::Open(
  const SubReader& segment) const {
  const auto* col_reader = segment.GetColReader();
  const auto* column = col_reader ? col_reader->Column(_column) : nullptr;
  if (!column) {
    return std::make_unique<NoTokensSource>();
  }
  SDB_ASSERT(column->Type().InternalType() == duckdb::PhysicalType::VARCHAR);
  return std::make_unique<StoredValueSource>(*col_reader, *column,
                                             _make_tokenizer());
}

bool StoredValueSourceFactory::Equals(
  const PhraseTokenSourceFactory& other) const noexcept {
  return other.Name() == Name() &&
         irs::utils::downCast<StoredValueSourceFactory>(other)._column ==
           _column;
}

PhraseVerifyKernel::PhraseVerifyKernel(
  const ByPhraseOptions& phrase, std::span<const std::vector<bstring>> expanded)
  : _slop{phrase.slop()} {
  const auto n = phrase.size();
  _offs_min.reserve(n);
  _offs_max.reserve(n);
  bool sequence = _slop == 0;
  uint32_t slot = 0;
  for (const auto& info : phrase) {
    _offs_min.push_back(info.offs_min);
    _offs_max.push_back(info.offs_max);
    if (slot != 0 && (info.offs_min != 1 || info.offs_max != 1)) {
      sequence = false;
    }
    switch (ByPhraseOptions::KindOf(info.part)) {
      case SlotKind::Term:
        Accept(std::get<ByTermOptions>(info.part).term, slot);
        break;
      case SlotKind::Set:
        sequence = false;
        for (const auto& term : std::get<TermSetOptions>(info.part).terms) {
          Accept(term, slot);
        }
        break;
      case SlotKind::Expansion:
        sequence = false;
        if (slot < expanded.size()) {
          for (const auto& term : expanded[slot]) {
            Accept(term, slot);
          }
        }
        break;
    }
    ++slot;
  }
  Finish();

  if (sequence) {
    _sequence.reserve(n);
    for (const auto& term : _owned) {
      _sequence.emplace_back(term);
    }
    _failure.assign(n, 0);
    uint32_t k = 0;
    for (size_t i = 1; i < n; ++i) {
      while (k > 0 && _sequence[k] != _sequence[i]) {
        k = _failure[k - 1];
      }
      if (_sequence[k] == _sequence[i]) {
        ++k;
      }
      _failure[i] = k;
    }
  }

  if (_slop != 0 && n > 1) {
    _steps.assign(_offs_max.begin() + 1, _offs_max.end());
  }
}

void PhraseVerifyKernel::Accept(bytes_view term, uint32_t slot) {
  _owned.emplace_back(term);
  _owned_slots.push_back(slot);
}

void PhraseVerifyKernel::Finish() {
  std::vector<std::pair<bytes_view, uint32_t>> pending;
  pending.reserve(_owned.size());
  for (size_t i = 0; i != _owned.size(); ++i) {
    pending.emplace_back(_owned[i], _owned_slots[i]);
  }
  absl::c_sort(pending);

  const auto n = static_cast<uint32_t>(_offs_min.size());
  _groups.resize(n);
  std::iota(_groups.begin(), _groups.end(), 0);
  const auto find = [&](uint32_t x) {
    while (_groups[x] != x) {
      _groups[x] = _groups[_groups[x]];
      x = _groups[x];
    }
    return x;
  };

  for (size_t i = 0; i != pending.size();) {
    const auto term = pending[i].first;
    SlotList list{.begin = static_cast<uint32_t>(_slot_ids.size())};
    for (; i != pending.size() && pending[i].first == term; ++i) {
      const auto slot = pending[i].second;
      if (list.size != 0 && _slot_ids.back() == slot) {
        continue;
      }
      if (list.size != 0) {
        const auto a = find(_slot_ids[list.begin]);
        const auto b = find(slot);
        if (a != b) {
          _groups[b] = a;
        }
      }
      _slot_ids.push_back(slot);
      ++list.size;
    }
    _accept.emplace(term, list);
  }

  for (uint32_t i = 0; i != n; ++i) {
    _groups[i] = find(i);
  }
}

bool PhraseVerifyKernel::Match(const PhraseDocTokens& doc, bool count,
                               PhraseVerifyScratch& scratch,
                               PhraseVerdict& out) const {
  out = {};
  if (doc.terms.empty() || _offs_min.empty()) {
    return false;
  }
  if (!_sequence.empty() && Dense(doc)) {
    return MatchSequence(doc.terms, count, out);
  }
  return MatchSlots(doc, count, scratch, out);
}

bool PhraseVerifyKernel::MatchSequence(std::span<const bytes_view> terms,
                                       bool count, PhraseVerdict& out) const {
  const auto m = _sequence.size();
  uint32_t k = 0;
  uint32_t freq = 0;
  for (const auto term : terms) {
    while (k > 0 && term != _sequence[k]) {
      k = _failure[k - 1];
    }
    if (term == _sequence[k]) {
      ++k;
    }
    if (k == m) {
      ++freq;
      if (!count) {
        break;
      }
      k = _failure[k - 1];
    }
  }
  out = {.freq = freq};
  return freq != 0;
}

bool PhraseVerifyKernel::MatchSlots(const PhraseDocTokens& doc, bool count,
                                    PhraseVerifyScratch& scratch,
                                    PhraseVerdict& out) const {
  const auto n = _offs_min.size();
  auto& slots = scratch.slots;
  slots.resize(n);
  for (auto& slot : slots) {
    slot.clear();
  }
  for (size_t i = 0; i != doc.terms.size(); ++i) {
    const auto it = _accept.find(doc.terms[i]);
    if (it == _accept.end()) {
      continue;
    }
    const auto pos = doc.positions[i];
    const auto list = it->second;
    for (uint32_t j = 0; j != list.size; ++j) {
      auto& positions = slots[_slot_ids[list.begin + j]];
      if (positions.empty() || positions.back() != pos) {
        positions.push_back(pos);
      }
    }
  }
  for (const auto& slot : slots) {
    if (slot.empty()) {
      return false;
    }
  }
  if (n == 1) {
    out.freq = count ? static_cast<uint32_t>(slots.front().size()) : 1;
    return true;
  }

  if (_slop != 0) {
    const auto res =
      detail::slop::Run(slots, _slop, _steps, scratch.slop, !count, _groups);
    if (!res.any) {
      return false;
    }
    if (count) {
      out.freq = static_cast<uint32_t>(res.freq);
      out.scale =
        static_cast<score_t>(res.weight / static_cast<double>(res.freq));
    } else {
      out.freq = 1;
    }
    return true;
  }

  auto& valid = scratch.valid;
  auto& next = scratch.next;
  valid.assign(slots.back().begin(), slots.back().end());
  for (size_t i = n - 1; i != 0; --i) {
    const auto& prev = slots[i - 1];
    next.clear();
    size_t j = 0;
    for (const auto p : prev) {
      const uint64_t lo = uint64_t{p} + _offs_min[i];
      const uint64_t hi = uint64_t{p} + _offs_max[i];
      while (j != valid.size() && valid[j] < lo) {
        ++j;
      }
      if (j != valid.size() && valid[j] <= hi) {
        next.push_back(p);
      }
    }
    std::swap(valid, next);
    if (valid.empty()) {
      return false;
    }
  }
  out.freq = count ? static_cast<uint32_t>(valid.size()) : 1;
  return true;
}

}  // namespace irs
