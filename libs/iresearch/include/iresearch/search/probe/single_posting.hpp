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

#include <vector>

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

class SinglePostingDocs {
 public:
  explicit SinglePostingDocs(const PostingMeta& meta) noexcept
    : _doc{doc_limits::min() + meta.doc_delta} {
    SDB_ASSERT(meta.docs_count == 1);
  }

  SinglePostingDocs(const SinglePostingDocs&) = delete;
  SinglePostingDocs& operator=(const SinglePostingDocs&) = delete;
  SinglePostingDocs(SinglePostingDocs&&) = delete;
  SinglePostingDocs& operator=(SinglePostingDocs&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) const noexcept {
    return target <= _doc ? _doc : doc_limits::eof();
  }

 private:
  doc_id_t _doc;
};

class SinglePostingScored {
 public:
  SinglePostingScored(const PostingMeta& meta, const SubReader& segment,
                      const TermReader& field, bool has_freq,
                      const search::ScoreArgs& args) noexcept
    : _leaf{meta},
      _freq{meta.freq},
      _segment{&segment},
      _field{&field},
      _args{args} {
    _provider.has_freq = has_freq;
  }

  SinglePostingScored(const SinglePostingScored&) = delete;
  SinglePostingScored& operator=(const SinglePostingScored&) = delete;
  SinglePostingScored(SinglePostingScored&&) = delete;
  SinglePostingScored& operator=(SinglePostingScored&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) const noexcept {
    return _leaf.Probe(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    SDB_ASSERT(slot < kScoreBlock);
    _gather[slot] = _freq;
  }

  ScoreFunction PrepareScore() {
    _provider.freq.value = _gather;
    SDB_ASSERT(_args.scorer != nullptr);
    return _args.scorer->PrepareScorer({
      .segment = *_segment,
      .field = _field->meta(),
      .doc_attrs = _provider,
      .fetcher = _args.fetcher,
      .stats = _args.stats,
      .boost = _args.boost,
    });
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  struct Provider final : AttributeProvider {
    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      return has_freq && type == irs::Type<FreqBlockAttr>::id() ? &freq
                                                                : nullptr;
    }

    FreqBlockAttr freq;
    bool has_freq = false;
  };

  SinglePostingDocs _leaf;
  uint32_t _freq;
  const SubReader* _segment;
  const TermReader* _field;
  search::ScoreArgs _args;
  Provider _provider;
  uint32_t _gather[kScoreBlock]{};
};

class PlainSinglePostingScored {
 public:
  explicit PlainSinglePostingScored(const PostingMeta& meta) noexcept
    : _leaf{meta} {}

  PlainSinglePostingScored(const PlainSinglePostingScored&) = delete;
  PlainSinglePostingScored& operator=(const PlainSinglePostingScored&) = delete;
  PlainSinglePostingScored(PlainSinglePostingScored&&) = delete;
  PlainSinglePostingScored& operator=(PlainSinglePostingScored&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) const noexcept {
    return _leaf.Probe(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t) const noexcept {}

  ScoreFunction PrepareScore() const noexcept {
    return ScoreFunction::Default();
  }

  void CollectScorers(std::vector<ScoreFunction>&) const noexcept {}

 private:
  SinglePostingDocs _leaf;
};

}  // namespace irs::probe
