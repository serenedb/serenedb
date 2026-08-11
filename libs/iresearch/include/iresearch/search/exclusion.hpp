////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/index/iterators.hpp"

namespace irs {

template<typename IncludeAdapter, typename ExcludeAdapter>
class ExclusionIterator : public DocIterator {
 public:
  ExclusionIterator(IncludeAdapter incl, ExcludeAdapter excl) noexcept
    : _incl{std::move(incl)}, _excl{std::move(excl)} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _incl.GetMutable(type);
  }

  IRS_DOC_ITERATOR_FILL_BLOCK
  IRS_DOC_ITERATOR_COUNT
  IRS_DOC_ITERATOR_EMIT_DOCS
  IRS_DOC_ITERATOR_EMIT_SCORED_DOCS

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    if (_incl.GetMutable(irs::Type<ScoreThresholdAttr>::id()) != nullptr) {
      FilteringCollector filtered{collector, *this};
      _incl.Collect(scorer, fetcher, filtered);
      _doc = doc_limits::eof();
      return;
    }
    DocIterator::CollectImpl(*this, scorer, fetcher, collector);
  }

  doc_id_t advance() final {
    const auto incl = _incl.advance();
    return converge(incl);
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto incl = _incl.seek(target);
    return converge(incl);
  }

  doc_id_t LazySeek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _incl.LazySeek(target);
    if (doc != target) {
      return doc;
    }
    if (Excluded(doc)) {
      return doc + 1;
    }
    return _doc = doc;
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    return _incl.PrepareScore(ctx);
  }

  void FetchScoreArgs(uint16_t index) final { _incl.FetchScoreArgs(index); }

 private:
  class FilteringCollector final : public ScoreCollector {
   public:
    FilteringCollector(ScoreCollector& inner, ExclusionIterator& self) noexcept
      : ScoreCollector{Tag::Generic}, _inner{&inner}, _self{&self} {}

    void Add(score_t score, doc_id_t doc) final {
      if (!_self->Excluded(doc)) {
        _inner->Add(score, doc);
      }
    }

    void AddDocs(const doc_id_t* docs, size_t count,
                 const score_t* scores) final {
      doc_id_t kept_docs[kMaxDocs];
      score_t kept_scores[kMaxDocs];
      while (count != 0) {
        const auto chunk = std::min(count, kMaxDocs);
        size_t n = 0;
        for (size_t i = 0; i != chunk; ++i) {
          kept_docs[n] = docs[i];
          kept_scores[n] = scores[i];
          n += static_cast<size_t>(!_self->Excluded(docs[i]));
        }
        if (n != 0) {
          _inner->AddDocs(kept_docs, n, kept_scores);
        }
        docs += chunk;
        scores += chunk;
        count -= chunk;
      }
    }

    void ConsumeWindow(score_t* scores, uint64_t* mask, doc_id_t min,
                       size_t num_blocks) final {
      static constexpr auto kBits = BitsRequired<uint64_t>();
      uint64_t filtered[kMaxBlocks];
      while (num_blocks != 0) {
        const auto chunk = std::min(num_blocks, kMaxBlocks);
        for (size_t i = 0; i != chunk; ++i) {
          const auto orig = mask[i];
          mask[i] = 0;
          auto keep = orig;
          auto word = orig;
          const auto base = min + static_cast<doc_id_t>(i * kBits);
          while (word != 0) {
            const auto bit = static_cast<uint32_t>(std::countr_zero(word));
            word = PopBit(word);
            if (_self->Excluded(base + bit)) {
              UnsetBit(keep, bit);
            }
          }
          filtered[i] = keep;
          if (keep == 0 && orig != 0) {
            std::memset(scores + i * kBits, 0, kBits * sizeof(score_t));
          }
        }
        _inner->ConsumeWindow(scores, filtered, min, chunk);
        mask += chunk;
        scores += chunk * kBits;
        min += static_cast<doc_id_t>(chunk * kBits);
        num_blocks -= chunk;
      }
    }

   private:
    static constexpr size_t kMaxBlocks = 64;
    static constexpr size_t kMaxDocs = kPostingBlock;

    ScoreCollector* _inner;
    ExclusionIterator* _self;
  };

  IRS_FORCE_INLINE bool Excluded(doc_id_t doc) {
    auto hits = [doc](auto& it) IRS_FORCE_INLINE {
      auto excl = it.value();
      if (excl < doc) {
        excl = it.LazySeek(doc);
      }
      SDB_ASSERT(excl >= doc);
      return excl == doc;
    };

    if constexpr (requires { _excl.begin(); }) {
      for (auto& it : _excl) {
        if (hits(it)) {
          return true;
        }
      }
      return false;
    } else {
      return hits(_excl);
    }
  }

  doc_id_t converge(doc_id_t incl) {
    if (doc_limits::eof(incl)) [[unlikely]] {
      return _doc = incl;
    }
    if (Excluded(incl)) {
      return advance();
    }
    return _doc = incl;
  }

  IncludeAdapter _incl;
  ExcludeAdapter _excl;
};

}  // namespace irs
