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

#include <limits>
#include <numeric>

#include "iresearch/analysis/text/term_view.hpp"

namespace irs {
namespace {

constexpr uint32_t kResetEvery = 1024;

}  // namespace

bool PhraseVerifySink::Match(analysis::Tokenizer& tokenizer,
                             ValueAnalyzer& analyzer, duckdb::string_t value,
                             bool count, PhraseVerdict& out) {
  _kernel->Begin(_dense, count, _scratch);
  if (!analyzer.Analyze(tokenizer, value, *this)) {
    out = {};
    return false;
  }
  return _kernel->End(_scratch, out);
}

void PhraseVerifySink::Consume(TokenBatch& batch, DocRuns) {
  if (_dense) {
    std::iota(batch.pos, batch.pos + batch.count, _pos + 1);
    _pos += batch.count;
  }
  _kernel->Push(batch.Terms(), batch.pos, _scratch);
}

PhraseTokenReader::PhraseTokenReader(const ColReader& col_reader,
                                     const ColumnReader& column,
                                     analysis::Tokenizer::ptr tokenizer,
                                     const PhraseVerifyKernel& kernel)
  : _ctx{col_reader},
    _column{&column},
    _state{column.InitScan(_ctx)},
    _out{column.Type()},
    _sel{1},
    _tokenizer{std::move(tokenizer)},
    _sink{kernel, _tokenizer->Traits()} {
  SDB_ASSERT(column.Type().InternalType() == duckdb::PhysicalType::VARCHAR);
  _sel.set_index(0, 0);
}

bool PhraseTokenReader::Match(doc_id_t doc, bool count, PhraseVerdict& out) {
  duckdb::string_t value;
  if (!Fetch(doc, value)) {
    out = {};
    return false;
  }
  return _sink.Match(*_tokenizer, _analyzer, value, count, out);
}

bool PhraseTokenReader::Fetch(doc_id_t doc, duckdb::string_t& value) {
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

PhraseVerifyKernel::PhraseVerifyKernel(
  const ByPhraseOptions& phrase, std::span<const std::vector<bstring>> expanded)
  : _slop{phrase.slop()} {
  const auto n = phrase.size();
  _offs_min.reserve(n);
  _offs_max.reserve(n);
  bool sequence = _slop == 0;
  std::vector<uint32_t> slots;
  uint32_t slot = 0;
  const auto accept = [&](bytes_view term) {
    _owned.emplace_back(term);
    slots.push_back(slot);
  };
  for (const auto& info : phrase) {
    _offs_min.push_back(info.offs_min);
    _offs_max.push_back(info.offs_max);
    if (slot != 0 && (info.offs_min != 1 || info.offs_max != 1)) {
      sequence = false;
    }
    switch (ByPhraseOptions::KindOf(info.part)) {
      case SlotKind::Term:
        accept(std::get<ByTermOptions>(info.part).term);
        break;
      case SlotKind::Set:
        sequence = false;
        for (const auto& term : std::get<TermSetOptions>(info.part).terms) {
          accept(term);
        }
        break;
      case SlotKind::Expansion:
        sequence = false;
        if (slot < expanded.size()) {
          for (const auto& term : expanded[slot]) {
            accept(term);
          }
        }
        break;
    }
    ++slot;
  }
  Finish(slots);

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

void PhraseVerifyKernel::Finish(std::span<const uint32_t> slots) {
  std::vector<std::pair<bytes_view, uint32_t>> pending;
  pending.reserve(_owned.size());
  for (size_t i = 0; i != _owned.size(); ++i) {
    pending.emplace_back(_owned[i], slots[i]);
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

void PhraseVerifyKernel::Begin(bool dense, bool count,
                               PhraseVerifyScratch& scratch) const {
  scratch.sequence = dense && !_sequence.empty();
  scratch.count = count;
  scratch.state = 0;
  scratch.freq = 0;
  if (!scratch.sequence) {
    scratch.slots.resize(_offs_min.size());
    for (auto& slot : scratch.slots) {
      slot.clear();
    }
  }
}

void PhraseVerifyKernel::Push(std::span<const duckdb::string_t> terms,
                              const uint32_t* pos,
                              PhraseVerifyScratch& scratch) const {
  if (scratch.sequence) {
    PushSequence(terms, scratch);
  } else {
    PushSlots(terms, pos, scratch);
  }
}

bool PhraseVerifyKernel::End(PhraseVerifyScratch& scratch,
                             PhraseVerdict& out) const {
  out = {};
  if (_offs_min.empty()) {
    return false;
  }
  if (scratch.sequence) {
    out.freq = scratch.freq;
    return scratch.freq != 0;
  }
  return EndSlots(scratch, out);
}

void PhraseVerifyKernel::PushSequence(std::span<const duckdb::string_t> terms,
                                      PhraseVerifyScratch& scratch) const {
  if (scratch.freq != 0 && !scratch.count) {
    return;
  }
  const auto m = _sequence.size();
  auto k = scratch.state;
  auto freq = scratch.freq;
  for (const auto& value : terms) {
    const auto term = AsBytesView(value);
    while (k > 0 && term != _sequence[k]) {
      k = _failure[k - 1];
    }
    if (term == _sequence[k]) {
      ++k;
    }
    if (k == m) {
      ++freq;
      if (!scratch.count) {
        break;
      }
      k = _failure[k - 1];
    }
  }
  scratch.state = k;
  scratch.freq = freq;
}

void PhraseVerifyKernel::PushSlots(std::span<const duckdb::string_t> terms,
                                   const uint32_t* pos,
                                   PhraseVerifyScratch& scratch) const {
  auto& slots = scratch.slots;
  for (size_t i = 0; i != terms.size(); ++i) {
    const auto it = _accept.find(AsBytesView(terms[i]));
    if (it == _accept.end()) {
      continue;
    }
    const auto list = it->second;
    for (uint32_t j = 0; j != list.size; ++j) {
      auto& positions = slots[_slot_ids[list.begin + j]];
      if (positions.empty() || positions.back() != pos[i]) {
        positions.push_back(pos[i]);
      }
    }
  }
}

bool PhraseVerifyKernel::EndSlots(PhraseVerifyScratch& scratch,
                                  PhraseVerdict& out) const {
  const auto n = _offs_min.size();
  const auto count = scratch.count;
  const auto& slots = scratch.slots;
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
  auto& ways = scratch.ways;
  auto& next_ways = scratch.next_ways;
  valid.assign(slots.back().begin(), slots.back().end());
  ways.assign(valid.size(), 1);
  for (size_t i = n - 1; i != 0; --i) {
    const auto& prev = slots[i - 1];
    next.clear();
    next_ways.clear();
    size_t lo = 0;
    size_t hi = 0;
    uint64_t window = 0;
    for (const auto p : prev) {
      const uint64_t min = uint64_t{p} + _offs_min[i];
      const uint64_t max = uint64_t{p} + _offs_max[i];
      for (; hi != valid.size() && valid[hi] <= max; ++hi) {
        window += ways[hi];
      }
      for (; lo != hi && valid[lo] < min; ++lo) {
        window -= ways[lo];
      }
      if (window != 0) {
        next.push_back(p);
        next_ways.push_back(window);
      }
    }
    std::swap(valid, next);
    std::swap(ways, next_ways);
    if (valid.empty()) {
      return false;
    }
  }
  if (!count) {
    out.freq = 1;
    return true;
  }
  const auto freq = absl::c_accumulate(ways, uint64_t{0});
  out.freq = static_cast<uint32_t>(
    std::min<uint64_t>(freq, std::numeric_limits<uint32_t>::max()));
  return true;
}

}  // namespace irs
