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

#include <absl/functional/function_ref.h>

#include <utility>

#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

class UnionMember : public memory::Managed {
 public:
  using ptr = memory::managed_ptr<UnionMember>;
  using Visit = absl::FunctionRef<void(doc_id_t*, uint32_t, score_t*)>;

  doc_id_t Value() const noexcept { return _doc; }

  virtual doc_id_t Seek(doc_id_t target) = 0;
  virtual doc_id_t AdvanceBlock(doc_id_t target) = 0;
  virtual score_t MaxScore(doc_id_t last) = 0;
  virtual doc_id_t SkipNonCompetitive(doc_id_t first, doc_id_t second,
                                      score_t threshold) = 0;
  virtual void ForEachScoredBlock(doc_id_t max, Visit visit) = 0;
  virtual void Fill(doc_id_t min, doc_id_t max, uint64_t* mask,
                    score_t* window) = 0;
  virtual uint32_t ScoreCandidates(doc_id_t* docs, score_t* scores,
                                   uint32_t count, bool required) = 0;

 protected:
  doc_id_t _doc = doc_limits::invalid();
};

template<typename Input>
class TermUnionMember : public UnionMember {
 public:
  template<typename... Args>
  explicit TermUnionMember(Args&&... args)
    : _leaf{std::forward<Args>(args)...} {
    _doc = _leaf.Value();
  }

  doc_id_t Seek(doc_id_t target) final { return _doc = _leaf.Seek(target); }

  doc_id_t AdvanceBlock(doc_id_t target) final {
    const auto last = _leaf.AdvanceBlock(target);
    _doc = _leaf.Value();
    return last;
  }

  score_t MaxScore(doc_id_t last) final { return _leaf.MaxScore(last); }

  doc_id_t SkipNonCompetitive(doc_id_t first, doc_id_t second,
                              score_t threshold) final {
    const auto doc = _leaf.SkipNonCompetitive(first, second, threshold);
    _doc = _leaf.Value();
    return doc;
  }

  void ForEachScoredBlock(doc_id_t max, Visit visit) final {
    _leaf.ForEachScoredBlock(
      max, [&](doc_id_t* docs, uint32_t len, score_t* scores) {
        visit(docs, len, scores);
      });
    _doc = _leaf.Value();
  }

  void Fill(doc_id_t min, doc_id_t max, uint64_t* mask, score_t* window) final {
    _leaf.Fill(min, max, mask, window);
    _doc = _leaf.Value();
  }

  uint32_t ScoreCandidates(doc_id_t* docs, score_t* scores, uint32_t count,
                           bool required) final {
    Span<doc_id_t> cand_docs{docs, count};
    Span<score_t> cand_scores{scores, count};
    _leaf.ScoreCandidates(cand_docs, cand_scores, required);
    _doc = _leaf.Value();
    return cand_docs.count;
  }

 private:
  template<typename T>
  struct Span {
    T* data;
    uint32_t count;

    uint32_t size() const noexcept { return count; }
    void resize(uint32_t n) noexcept { count = n; }
    T& operator[](uint32_t i) const noexcept { return data[i]; }
  };

  PostingPrunedDisj<Input> _leaf;
};

class ErasedUnionLeaf {
 public:
  ErasedUnionLeaf() = default;

  void Reset(UnionMember::ptr member) noexcept { _member = std::move(member); }

  IRS_FORCE_INLINE doc_id_t Value() const noexcept { return _member->Value(); }

  doc_id_t Seek(doc_id_t target) { return _member->Seek(target); }

  doc_id_t AdvanceBlock(doc_id_t target) {
    return _member->AdvanceBlock(target);
  }

  score_t MaxScore(doc_id_t last) { return _member->MaxScore(last); }

  doc_id_t SkipNonCompetitive(doc_id_t first, doc_id_t second,
                              score_t threshold) {
    return _member->SkipNonCompetitive(first, second, threshold);
  }

  template<typename Visitor>
  void ForEachScoredBlock(doc_id_t max, Visitor&& visit) {
    _member->ForEachScoredBlock(max, visit);
  }

  void Fill(doc_id_t min, doc_id_t max, uint64_t* mask, score_t* window) {
    _member->Fill(min, max, mask, window);
  }

  template<typename Docs, typename Scores>
  void ScoreCandidates(Docs& docs, Scores& scores, bool required) {
    const auto count = _member->ScoreCandidates(
      docs.data, scores.data, static_cast<uint32_t>(docs.size()), required);
    docs.resize(count);
    scores.resize(count);
  }

 private:
  UnionMember::ptr _member;
};

}  // namespace irs::top
