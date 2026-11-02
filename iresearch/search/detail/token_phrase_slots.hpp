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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <memory>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_extract.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/token_phrase.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

class AnchorFreqs {
 public:
  virtual ~AnchorFreqs() = default;

  virtual uint32_t At(doc_id_t doc) = 0;
};

template<typename Input>
class AnchorFreqsOf final : public AnchorFreqs {
 public:
  AnchorFreqsOf(const PostingMeta& meta, const SubReader& segment,
                const TermReader& field)
    : _posting{meta, *DocOf(field), segment, field, ScoreArgs{}} {}

  uint32_t At(doc_id_t doc) final {
    return _posting.Probe(doc) == doc ? _posting.Freq() : 0;
  }

 private:
  PostingProbeScored<Input> _posting;
};

inline std::unique_ptr<AnchorFreqs> MakeAnchorFreqs(
  const TokenPhraseQuery& query) {
  const auto* meta = query.Anchor();
  if (!meta) {
    return nullptr;
  }
  const auto& field = query.Reader();
  return ResolveInput(*DocOf(field),
                      [&]<typename Input> -> std::unique_ptr<AnchorFreqs> {
                        return std::make_unique<AnchorFreqsOf<Input>>(
                          *meta, query.Segment(), field);
                      });
}

class PhraseRows {
 public:
  PhraseRows(const TokenPhraseQuery& query, bool count)
    : _ctx{*query.Segment().GetColReader()},
      _column{&query.Column()},
      _state{_column->InitScan(_ctx)},
      _out{_column->Type()},
      _check{query.Compiled(), query.Tokenizer(), count} {}

  PhraseRows(PhraseRows&&) = delete;
  PhraseRows& operator=(PhraseRows&&) = delete;

  size_t Gather(std::span<const doc_id_t> hits) {
    auto n = hits.size();
    while (n != 0 && hits[n - 1] - doc_limits::min() >= _column->RowCount()) {
      --n;
    }
    if (n == 0) {
      return 0;
    }
    auto& out = _out.Reset();
    column_internal::GatherRows(*_column, _state, DocRows{hits.first(n)}, out,
                                0);
    _check.Bind(out, n);
    return n;
  }

  PhraseVerdict Check(size_t row, uint32_t anchors) {
    return _check.Check(row, anchors);
  }

 private:
  ReadContext _ctx;
  const ColumnReader* _column;
  ColumnReader::ScanState _state;
  ColumnReader::VectorScratch _out;
  PhraseCheck _check;
};

template<typename Approx, bool Sloppy>
class TokenPhraseSlots {
 public:
  static constexpr size_t kMaxBatch = 512;

  template<typename ApproxArgs>
  TokenPhraseSlots(std::piecewise_construct_t, ApproxArgs&& approx,
                   const TokenPhraseQuery& query, bool count)
    : _approx{std::make_from_tuple<Approx>(std::forward<ApproxArgs>(approx))},
      _rows{query, count},
      _anchors{MakeAnchorFreqs(query)} {}

  TokenPhraseSlots(TokenPhraseSlots&&) = delete;
  TokenPhraseSlots& operator=(TokenPhraseSlots&&) = delete;

  doc_id_t Seek(doc_id_t target) {
    const auto pending = std::span{_hits}.subspan(_pos);
    const auto it = absl::c_lower_bound(pending, target);
    _pos = static_cast<size_t>(it - std::span{_hits}.begin());
    if (_pos != _hits.size()) {
      return *it;
    }
    _batch = 1;
    return Fill(_end ? doc_limits::eof() : _approx.Seek(target));
  }

  doc_id_t Next(doc_id_t) {
    if (++_pos < _hits.size()) {
      return _hits[_pos];
    }
    _batch = std::min(_batch * 2, kMaxBatch);
    return Fill(_end ? doc_limits::eof() : _approx.Next());
  }

  doc_id_t Probe(doc_id_t target) { return _approx.Probe(target); }

  bool Match(doc_id_t doc) {
    _verdict = {};
    if constexpr (requires(Approx& approx) { approx.Next(); }) {
      SDB_ASSERT(_pos < _hits.size() && _hits[_pos] == doc);
      if (_pos < _gathered) {
        _verdict = _rows.Check(_pos, AnchorsAt(doc));
      }
    } else if (_rows.Gather({&doc, 1}) != 0) {
      _verdict = _rows.Check(0, AnchorsAt(doc));
    }
    return _verdict.freq != 0;
  }

  uint32_t Freq() const noexcept { return _verdict.freq; }

  score_t Scale() const noexcept
    requires(Sloppy)
  {
    return _verdict.scale;
  }

 private:
  doc_id_t Fill(doc_id_t first) {
    _hits.clear();
    _pos = 0;
    if (doc_limits::eof(first)) {
      _end = true;
      return first;
    }
    _hits.emplace_back(first);
    while (_hits.size() < _batch) {
      const auto doc = _approx.Next();
      if (doc_limits::eof(doc)) {
        _end = true;
        break;
      }
      _hits.emplace_back(doc);
    }
    _gathered = _rows.Gather(_hits);
    return first;
  }

  uint32_t AnchorsAt(doc_id_t doc) { return _anchors ? _anchors->At(doc) : 0; }

  Approx _approx;
  PhraseRows _rows;
  std::unique_ptr<AnchorFreqs> _anchors;
  PhraseVerdict _verdict;
  std::vector<doc_id_t> _hits;
  size_t _pos = 0;
  size_t _gathered = 0;
  size_t _batch = 1;
  bool _end = false;
};

}  // namespace irs::detail
