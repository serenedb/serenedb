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

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

class BoostIteratorScore2 : public ScoreOperator {
 public:
  BoostIteratorScore2(ScoreFunction req, ScoreFunction opt,
                      bool* opt_matched) noexcept
    : _req{std::move(req)}, _opt{std::move(opt)}, _matches{opt_matched} {}

  score_t Score() const noexcept final {
    auto s = _req.Score();
    if (_matches[0]) {
      _matches[0] = false;
      Merge<ScoreMergeType::Sum>(s, _opt.Score());
    }
    return s;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

 private:
  // TODO(mbkkt) use res directly
  template<ScoreMergeType OuterType>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    _req.Score<ScoreMergeType::Noop>(_req_scores, n);
    _opt.Score<ScoreMergeType::Noop>(_opt_scores, n);
    for (scores_size_t i = 0; i < n; ++i) {
      auto s = _req_scores[i];
      if (_matches[i]) {
        _matches[i] = false;
        Merge<ScoreMergeType::Sum>(s, _opt_scores[i]);
      }
      Merge<OuterType>(res[i], s);
    }
  }

  ScoreFunction _req;
  ScoreFunction _opt;
  ABSL_CACHELINE_ALIGNED mutable score_t _req_scores[kScoreBlock]{};
  ABSL_CACHELINE_ALIGNED mutable score_t _opt_scores[kScoreBlock]{};
  bool* IRS_RESTRICT _matches;
};

class BoostIteratorScoreN : public ScoreOperator {
 public:
  BoostIteratorScoreN(ScoreFunction req, std::vector<ScoreFunction> opts,
                      std::vector<bool*> matches) noexcept
    : _req{std::move(req)},
      _opts{std::move(opts)},
      _matches{std::move(matches)} {}

  score_t Score() const noexcept final {
    auto s = _req.Score();
    for (size_t i = 0; i < _opts.size(); ++i) {
      if (_matches[i][0]) {
        _matches[i][0] = false;
        Merge<ScoreMergeType::Sum>(s, _opts[i].Score());
      }
    }
    return s;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

 private:
  // TODO(mbkkt) use res directly
  template<ScoreMergeType OuterType>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    _req.Score<ScoreMergeType::Noop>(_req_scores, n);
    for (size_t j = 0; j < _opts.size(); ++j) {
      _opts[j].Score<ScoreMergeType::Noop>(_opt_scores, n);
      for (scores_size_t i = 0; i < n; ++i) {
        if (_matches[j][i]) {
          _matches[j][i] = false;
          Merge<ScoreMergeType::Sum>(_req_scores[i], _opt_scores[i]);
        }
      }
    }
    for (scores_size_t i = 0; i < n; ++i) {
      Merge<OuterType>(res[i], _req_scores[i]);
    }
  }

  ScoreFunction _req;
  std::vector<ScoreFunction> _opts;
  std::vector<bool*> _matches;
  ABSL_CACHELINE_ALIGNED mutable score_t _req_scores[kScoreBlock]{};
  ABSL_CACHELINE_ALIGNED mutable score_t _opt_scores[kScoreBlock]{};
};

template<typename RequiredAdapter, typename OptionalAdapter>
class BoostIterator : public DocIterator {
  static constexpr bool kOptIsVector =
    requires { std::declval<OptionalAdapter>().begin(); };

  using Matches =
    std::conditional_t<kOptIsVector, std::vector<std::array<bool, kScoreBlock>>,
                       std::array<bool, kScoreBlock>>;

 public:
  BoostIterator(RequiredAdapter req, OptionalAdapter opt)
    : _req{std::move(req)}, _opt{std::move(opt)} {
    if constexpr (kOptIsVector) {
      _matches.resize(_opt.size());
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _req.GetMutable(type);
  }

  doc_id_t advance() final { return _doc = _req.advance(); }

  doc_id_t seek(doc_id_t target) final {
    if (target <= value()) [[unlikely]] {
      return value();
    }
    return _doc = _req.seek(target);
  }

  doc_id_t LazySeek(doc_id_t target) final {
    const auto doc = _req.LazySeek(target);
    _doc = _req.value();
    return doc;
  }

  void FetchScoreArgs(uint16_t index) final {
    _req.FetchScoreArgs(index);
    if constexpr (kOptIsVector) {
      for (size_t j = 0; j < _opt.size(); ++j) {
        auto opt_doc = _opt[j].value();
        if (opt_doc < _doc) {
          opt_doc = _opt[j].LazySeek(_doc);
        }
        _matches[j][index] = (opt_doc == _doc);
        if (_matches[j][index]) {
          _opt[j].FetchScoreArgs(index);
        }
      }
    } else {
      auto opt_doc = _opt.value();
      if (opt_doc < _doc) {
        opt_doc = _opt.LazySeek(_doc);
      }
      _matches[index] = opt_doc == _doc;
      if (_matches[index]) {
        _opt.FetchScoreArgs(index);
      }
    }
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    auto req_score = _req.PrepareScore(ctx);
    if constexpr (kOptIsVector) {
      std::vector<ScoreFunction> opt_scores;
      opt_scores.reserve(_opt.size());
      std::vector<bool*> matches;
      matches.reserve(_opt.size());
      bool any_non_default = false;
      for (size_t j = 0; j < _opt.size(); ++j) {
        auto s = _opt[j].PrepareScore(ctx);
        if (!s.IsDefault()) {
          any_non_default = true;
        }
        opt_scores.push_back(std::move(s));
        matches.push_back(_matches[j].data());
      }
      if (!any_non_default) {
        return req_score;
      }
      return ScoreFunction::Make<BoostIteratorScoreN>(
        std::move(req_score), std::move(opt_scores), std::move(matches));
    } else {
      auto opt_score = _opt.PrepareScore(ctx);
      if (opt_score.IsDefault()) {
        return req_score;
      }
      // TODO(mbkkt) optimize opt_score default?
      return ScoreFunction::Make<BoostIteratorScore2>(
        std::move(req_score), std::move(opt_score), _matches.data());
    }
  }

  uint32_t count() final { return CountImpl(*this); }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  RequiredAdapter _req;
  OptionalAdapter _opt;
  // TODO(mbkkt) make it score_t 0x0 or 0xFFFFFFF and XOR like mask
  Matches _matches{};
};

}  // namespace irs
