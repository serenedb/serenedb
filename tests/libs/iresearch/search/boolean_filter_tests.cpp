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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <absl/algorithm/container.h>

#include <bit>

#include "filter_test_case_base.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/granular_range_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

// Field-id constants used across this file. The on-disk index is now keyed
// by field_id (not by name); these IDs are arbitrary stable values used by
// both the writer-side fixture (to assign per-field IDs) and the filter
// builders (to address those fields).
inline constexpr irs::field_id kFieldName = tests::FieldIdFor("name");
inline constexpr irs::field_id kFieldSame = tests::FieldIdFor("same");
inline constexpr irs::field_id kFieldDuplicated =
  tests::FieldIdFor("duplicated");
inline constexpr irs::field_id kFieldPrefix = tests::FieldIdFor("prefix");
inline constexpr irs::field_id kFieldTestField =
  tests::FieldIdFor("test-field");
inline constexpr irs::field_id kFieldField = tests::FieldIdFor("field");
inline constexpr irs::field_id kFieldField1 = tests::FieldIdFor("field1");
inline constexpr irs::field_id kFieldField123 = tests::FieldIdFor("field123");
inline constexpr irs::field_id kFieldFieasfdld1 =
  tests::FieldIdFor("fieasfdld1");
inline constexpr irs::field_id kFieldSource = tests::FieldIdFor("source");
inline constexpr irs::field_id kFieldNameUpper =
  tests::FieldIdFor("name_upper");
inline constexpr irs::field_id kFieldAbc = tests::FieldIdFor("abc");
inline constexpr irs::field_id kFieldAbcd = tests::FieldIdFor("abcd");
// Deliberately-bad ids used by tests that expect a no-match.
inline constexpr irs::field_id kFieldInvalid = irs::field_limits::invalid();

template<typename Filter>
Filter MakeFilter(irs::field_id field, const std::string_view term) {
  Filter q;
  *q.mutable_field_id() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

template<typename Filter>
Filter& AddChild(irs::BooleanFilter& root, irs::Occur occur) {
  auto child = std::make_unique<Filter>();
  auto& ref = *child;
  root.Add(std::move(child), occur);
  return ref;
}

irs::BooleanFilter& AddBool(irs::BooleanFilter& root, irs::Occur occur) {
  return AddChild<irs::BooleanFilter>(root, occur);
}

void AddTerm(irs::BooleanFilter& root, irs::Occur occur, irs::field_id field,
             std::string_view term, irs::score_t boost = irs::kNoBoost,
             const irs::Scorer* scorer = nullptr) {
  root.Add(
    irs::TermClause{.field = field,
                    .scorer = scorer,
                    .term = irs::bstring{irs::ViewCast<irs::byte_type>(term)},
                    .boost = boost},
    occur);
}

template<typename Filter>
Filter& Append(irs::BooleanFilter& root, irs::Occur occur, irs::field_id field,
               const std::string_view& term) {
  auto& sub = AddChild<Filter>(root, occur);
  *sub.mutable_field_id() = field;
  sub.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return sub;
}

irs::BooleanFilter& AsDisjunction(irs::BooleanFilter& node) {
  node.SetMinShouldMatch(1);
  return node;
}

// A node is what it includes, less what it excludes. Lucene's `-x` leaves the
// include side empty, so it is nothing; SQL's `NOT x` includes `All`, which
// makes it the complement. This turns the former shape into the latter.
irs::BooleanFilter& AsComplement(irs::BooleanFilter& node) {
  AddChild<irs::All>(node, irs::Occur::Must);
  return node;
}

size_t CountChildren(irs::Filter& filter) {
  size_t count = 0;
  filter.VisitChildren([&](irs::Filter::ptr&) { ++count; });
  return count;
}

}  // namespace
namespace tests {
namespace detail {

struct CompoundSort final : irs::ScorerBase<CompoundSort, void> {
  explicit CompoundSort(std::vector<size_t> indexes) noexcept
    : indexes{std::move(indexes)} {}

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    if (current < indexes.size()) {
      return irs::ScoreFunction::Constant(
        static_cast<irs::score_t>(indexes[current++]));
    } else {
      return irs::ScoreFunction::Default();
    }
  }

  std::vector<size_t> indexes;
  mutable size_t current = 0;
};

struct SegmentReaderMock final : irs::SubReader {
  explicit SegmentReaderMock(uint64_t docs_count) noexcept {
    _meta.docs_count = docs_count;
    _meta.live_docs_count = docs_count;
  }
  uint64_t CountMappedMemory() const final { return 0; }
  const irs::SegmentInfo& Meta() const final { return _meta; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return {}; }
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }
  irs::SegmentInfo _meta;
};

class DocList {
 public:
  using DocidsT = std::vector<irs::doc_id_t>;

  explicit DocList(DocidsT docs) noexcept : _docs{std::move(docs)} {}

  irs::doc_id_t Value() const noexcept { return _doc; }

  irs::doc_id_t Advance() noexcept {
    if (_next == _docs.size()) {
      return _doc = irs::doc_limits::eof();
    }
    return _doc = _docs[_next++];
  }

  irs::doc_id_t Seek(irs::doc_id_t target) noexcept {
    if (irs::doc_limits::eof(_doc) || target <= _doc) {
      return _doc;
    }
    while (Advance() < target) {
    }
    return _doc;
  }

  irs::doc_id_t Window(irs::doc_id_t min, irs::doc_id_t max,
                       uint64_t* own) noexcept {
    const auto words = irs::search::WindowWords(min, max);
    irs::search::Clear(own, words);
    if (!irs::doc_limits::valid(_doc)) {
      Advance();
    }
    while (_doc < min) {
      Advance();
    }
    while (_doc < max) {
      const auto offset = _doc - min;
      own[offset / irs::search::kWindowBits] |=
        uint64_t{1} << (offset % irs::search::kWindowBits);
      Advance();
    }
    return _doc;
  }

  const DocidsT& docs() const noexcept { return _docs; }

 private:
  DocidsT _docs;
  size_t _next = 0;
  irs::doc_id_t _doc = irs::doc_limits::invalid();
};

struct NoAttrs : irs::AttributeProvider {
  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }
};

class LeadDocs : public irs::lead::Node {
 public:
  explicit LeadDocs(DocList::DocidsT docs) noexcept : _list{std::move(docs)} {}

  irs::doc_id_t Advance() final { return _doc = _list.Advance(); }

  irs::doc_id_t Seek(irs::doc_id_t target) final {
    return _doc = _list.Seek(target);
  }

 private:
  DocList _list;
};

class LeadScored : public irs::lead::Node {
 public:
  LeadScored(DocList::DocidsT docs, const irs::SubReader& segment,
             const irs::search::ScoredCtx& ctx, irs::score_t boost,
             const irs::byte_type* stats) noexcept
    : _list{std::move(docs)},
      _segment{segment},
      _ctx{ctx},
      _boost{boost},
      _stats{stats} {}

  irs::doc_id_t Advance() final { return _doc = _list.Advance(); }

  irs::doc_id_t Seek(irs::doc_id_t target) final {
    return _doc = _list.Seek(target);
  }

  void FetchScoreArgs(uint32_t) final {}

  irs::ScoreFunction PrepareScore() final {
    return _ctx.scorer->PrepareScorer({
      .segment = _segment,
      .field = {},
      .doc_attrs = _attrs,
      .fetcher = _ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

 private:
  DocList _list;
  NoAttrs _attrs;
  const irs::SubReader& _segment;
  irs::search::ScoredCtx _ctx;
  irs::score_t _boost;
  const irs::byte_type* _stats;
};

class ProbeDocs : public irs::probe::Node {
 public:
  explicit ProbeDocs(DocList::DocidsT docs) noexcept : _list{std::move(docs)} {}

  irs::doc_id_t Probe(irs::doc_id_t target) final { return _list.Seek(target); }

 private:
  DocList _list;
};

class ProbeScored : public irs::probe::Node {
 public:
  ProbeScored(DocList::DocidsT docs, const irs::SubReader& segment,
              const irs::search::ScoredCtx& ctx, irs::score_t boost,
              const irs::byte_type* stats) noexcept
    : _list{std::move(docs)},
      _segment{segment},
      _ctx{ctx},
      _boost{boost},
      _stats{stats} {}

  irs::doc_id_t Probe(irs::doc_id_t target) final { return _list.Seek(target); }

  void FetchScoreArgs(uint32_t) final {}

  irs::ScoreFunction PrepareScore() final {
    return _ctx.scorer->PrepareScorer({
      .segment = _segment,
      .field = {},
      .doc_attrs = _attrs,
      .fetcher = _ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

 private:
  DocList _list;
  NoAttrs _attrs;
  const irs::SubReader& _segment;
  irs::search::ScoredCtx _ctx;
  irs::score_t _boost;
  const irs::byte_type* _stats;
};

class FillDocs : public irs::fill::Node {
 public:
  explicit FillDocs(DocList::DocidsT docs) noexcept : _list{std::move(docs)} {}

  irs::doc_id_t FillOr(irs::doc_id_t min, irs::doc_id_t max,
                       uint64_t* IRS_RESTRICT mask) final {
    const auto words = irs::search::WindowWords(min, max);
    const auto next = _list.Window(min, max, _own.data());
    for (size_t w = 0; w != words; ++w) {
      mask[w] |= _own[w];
    }
    return next;
  }

  irs::doc_id_t FillAnd(irs::doc_id_t min, irs::doc_id_t max,
                        uint64_t* IRS_RESTRICT mask) final {
    const auto words = irs::search::WindowWords(min, max);
    const auto next = _list.Window(min, max, _own.data());
    for (size_t w = 0; w != words; ++w) {
      mask[w] &= _own[w];
    }
    return next;
  }

  irs::doc_id_t FillAndNot(irs::doc_id_t min, irs::doc_id_t max,
                           uint64_t* IRS_RESTRICT mask) final {
    const auto words = irs::search::WindowWords(min, max);
    const auto next = _list.Window(min, max, _own.data());
    for (size_t w = 0; w != words; ++w) {
      mask[w] &= ~_own[w];
    }
    return next;
  }

 private:
  irs::search::Scratch _own{};
  DocList _list;
};

class FillScored : public irs::fill::Node {
 public:
  FillScored(DocList::DocidsT docs, const irs::SubReader& segment,
             const irs::search::ScoredCtx& ctx, irs::score_t boost,
             const irs::byte_type* stats, irs::ScoreMergeType merge) noexcept
    : _list{std::move(docs)},
      _segment{segment},
      _ctx{ctx},
      _boost{boost},
      _stats{stats},
      _merge{merge} {}

  irs::doc_id_t Fill(irs::doc_id_t min, irs::doc_id_t max,
                     uint64_t* IRS_RESTRICT mask,
                     irs::score_t* IRS_RESTRICT scores) final {
    const auto words = irs::search::WindowWords(min, max);
    const auto next = _list.Window(min, max, _own.data());
    const auto score = _ctx.scorer->PrepareScorer({
      .segment = _segment,
      .field = {},
      .doc_attrs = _attrs,
      .fetcher = _ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
    const auto value = score.Score();
    irs::ResolveMergeType(_merge, [&]<irs::ScoreMergeType Merge> {
      for (size_t w = 0; w != words; ++w) {
        auto word = _own[w];
        mask[w] |= word;
        const auto base = w * irs::search::kWindowBits;
        while (word != 0) {
          const auto offset =
            base + static_cast<size_t>(std::countr_zero(word));
          irs::Merge<Merge>(scores[offset], value);
          word &= word - 1;
        }
      }
    });
    return next;
  }

 private:
  irs::search::Scratch _own{};
  DocList _list;
  NoAttrs _attrs;
  const irs::SubReader& _segment;
  irs::search::ScoredCtx _ctx;
  irs::score_t _boost;
  const irs::byte_type* _stats;
  irs::ScoreMergeType _merge;
};

class CountRoot : public irs::count::Root {
 public:
  explicit CountRoot(size_t count) noexcept : _count{count} {}

  uint64_t Run() final { return _count; }

 private:
  size_t _count;
};

class DocsRoot : public irs::docs::Root {
 public:
  explicit DocsRoot(DocList::DocidsT docs) noexcept : _list{std::move(docs)} {}

  uint32_t Run(irs::doc_id_t* out, uint32_t capacity) final {
    uint32_t size = 0;
    while (size != capacity && !irs::doc_limits::eof(_list.Advance())) {
      out[size++] = _list.Value();
    }
    return size;
  }

 private:
  DocList _list;
};

class ScoredRoot : public irs::scored::Root {
 public:
  ScoredRoot(DocList::DocidsT docs, const irs::SubReader& segment,
             const irs::scored::Context& ctx, irs::score_t boost,
             const irs::byte_type* stats) noexcept
    : _list{std::move(docs)},
      _segment{segment},
      _ctx{ctx},
      _boost{boost},
      _stats{stats} {}

  uint32_t Run(irs::doc_id_t* docs, irs::score_t* scores,
               uint32_t capacity) final {
    const auto score = _ctx.scorer.PrepareScorer({
      .segment = _segment,
      .field = {},
      .doc_attrs = _attrs,
      .fetcher = &_ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
    uint32_t size = 0;
    while (size != capacity && !irs::doc_limits::eof(_list.Advance())) {
      docs[size] = _list.Value();
      scores[size] = score.Score();
      ++size;
    }
    return size;
  }

 private:
  DocList _list;
  NoAttrs _attrs;
  const irs::SubReader& _segment;
  irs::scored::Context _ctx;
  irs::score_t _boost;
  const irs::byte_type* _stats;
};

struct Boosted : public irs::FilterWithType<Boosted> {
  struct Prepared : irs::QueryBuilder {
    Prepared(const irs::SubReader& segment, DocList::DocidsT docs,
             irs::score_t boost)
      : QueryBuilder{segment,
                     static_cast<uint32_t>(
                       std::min<uint64_t>(docs.size(), segment.docs_count())),
                     irs::QueryKind::Other},
        docs{std::move(docs)},
        _boost{boost} {}

    void Visit(irs::PreparedStateVisitor&, irs::score_t) const final {}

    irs::score_t Boost() const noexcept final { return _boost; }

    irs::count::Root::ptr PlanCount(const irs::count::Context&) const final {
      Boosted::gExecuteCount++;
      return irs::memory::make_managed<CountRoot>(docs.size());
    }

    irs::docs::Root::ptr PlanDocs(const irs::docs::Context&) const final {
      Boosted::gExecuteCount++;
      return irs::memory::make_managed<DocsRoot>(docs);
    }

    irs::scored::Root::ptr PlanScored(
      const irs::scored::Context& ctx) const final {
      Boosted::gExecuteCount++;
      return irs::memory::make_managed<ScoredRoot>(docs, Segment(), ctx, _boost,
                                                   Stats().stats);
    }

    irs::top::Root::ptr PlanTop(const irs::top::Context&) const final {
      return {};
    }

    irs::lead::Node::ptr PlanLead(
      const irs::search::ScoredCtx& ctx) const final {
      Boosted::gExecuteCount++;
      if (!Scores()) {
        return irs::memory::make_managed<LeadDocs>(docs);
      }
      return irs::memory::make_managed<LeadScored>(docs, Segment(), ctx, _boost,
                                                   Stats().stats);
    }

    irs::probe::Node::ptr PlanProbe(const irs::search::ScoredCtx& ctx,
                                    uint64_t) const final {
      Boosted::gExecuteCount++;
      if (!Scores()) {
        return irs::memory::make_managed<ProbeDocs>(docs);
      }
      return irs::memory::make_managed<ProbeScored>(docs, Segment(), ctx,
                                                    _boost, Stats().stats);
    }

    irs::fill::Node::ptr PlanFill(const irs::search::ScoredCtx& ctx,
                                  irs::ScoreMergeType merge) const final {
      Boosted::gExecuteCount++;
      if (!Scores()) {
        return irs::memory::make_managed<FillDocs>(docs);
      }
      return irs::memory::make_managed<FillScored>(docs, Segment(), ctx, _boost,
                                                   Stats().stats, merge);
    }

    DocList::DocidsT docs;

   private:
    irs::score_t _boost;
  };

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final {
    auto query = irs::memory::make_managed<Boosted::Prepared>(
      segment, docs, ctx.boost * GetBoost());
    query->SetStats(ctx.Record());
    return query;
  }

  irs::PrepareCollector::ptr MakeCollectorImpl(const irs::Scorer* scorer,
                                               irs::StatsArena& stats,
                                               uint32_t) const final {
    return std::make_unique<irs::AllCollector>(scorer, stats);
  }

  bool equals(const irs::Filter& rhs) const noexcept final {
    return this == &rhs;
  }

  DocList::DocidsT docs;
  static unsigned gExecuteCount;
};

unsigned Boosted::gExecuteCount{0};

struct SeekDoc {
  irs::doc_id_t target;
  irs::doc_id_t expected;
};

struct Unestimated : public irs::FilterWithType<Unestimated> {
  struct Prepared : public irs::QueryBuilder {
    explicit Prepared(const irs::SubReader& segment) : QueryBuilder{segment} {}

    void Visit(irs::PreparedStateVisitor&, irs::score_t) const final {}

    irs::score_t Boost() const noexcept final { return irs::kNoBoost; }

    irs::count::Root::ptr PlanCount(const irs::count::Context&) const final {
      return {};
    }
    irs::docs::Root::ptr PlanDocs(const irs::docs::Context&) const final {
      return {};
    }
    irs::scored::Root::ptr PlanScored(const irs::scored::Context&) const final {
      return {};
    }
    irs::top::Root::ptr PlanTop(const irs::top::Context&) const final {
      return {};
    }
    irs::lead::Node::ptr PlanLead(
      const irs::search::ScoredCtx& ctx) const final {
      if (!Scores()) {
        return irs::memory::make_managed<LeadDocs>(DocList::DocidsT{});
      }
      return irs::memory::make_managed<LeadScored>(
        DocList::DocidsT{}, Segment(), ctx, irs::kNoBoost, Stats().stats);
    }
    irs::probe::Node::ptr PlanProbe(const irs::search::ScoredCtx& ctx,
                                    uint64_t) const final {
      if (!Scores()) {
        return irs::memory::make_managed<ProbeDocs>(DocList::DocidsT{});
      }
      return irs::memory::make_managed<ProbeScored>(
        DocList::DocidsT{}, Segment(), ctx, irs::kNoBoost, Stats().stats);
    }
    irs::fill::Node::ptr PlanFill(const irs::search::ScoredCtx& ctx,
                                  irs::ScoreMergeType merge) const final {
      if (!Scores()) {
        return irs::memory::make_managed<FillDocs>(DocList::DocidsT{});
      }
      return irs::memory::make_managed<FillScored>(
        DocList::DocidsT{}, Segment(), ctx, irs::kNoBoost, Stats().stats,
        merge);
    }
  };

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment,
    const irs::PrepareContext& /*ctx*/) const final {
    return irs::memory::make_managed<Unestimated::Prepared>(segment);
  }

  bool equals(const irs::Filter& rhs) const noexcept final {
    return this == &rhs;
  }
};

struct Estimated : public irs::FilterWithType<Estimated> {
  struct Prepared : public irs::QueryBuilder {
    Prepared(const irs::SubReader& segment, uint32_t est, bool* evaluated)
      : QueryBuilder{
          segment,
          static_cast<uint32_t>(std::min<uint64_t>(est, segment.docs_count())),
          irs::QueryKind::Other} {
      *evaluated = true;
    }

    void Visit(irs::PreparedStateVisitor&, irs::score_t) const final {}

    irs::score_t Boost() const noexcept final { return irs::kNoBoost; }

    irs::count::Root::ptr PlanCount(const irs::count::Context&) const final {
      return {};
    }
    irs::docs::Root::ptr PlanDocs(const irs::docs::Context&) const final {
      return {};
    }
    irs::scored::Root::ptr PlanScored(const irs::scored::Context&) const final {
      return {};
    }
    irs::top::Root::ptr PlanTop(const irs::top::Context&) const final {
      return {};
    }
    irs::lead::Node::ptr PlanLead(
      const irs::search::ScoredCtx& ctx) const final {
      if (!Scores()) {
        return irs::memory::make_managed<LeadDocs>(DocList::DocidsT{});
      }
      return irs::memory::make_managed<LeadScored>(
        DocList::DocidsT{}, Segment(), ctx, irs::kNoBoost, Stats().stats);
    }
    irs::probe::Node::ptr PlanProbe(const irs::search::ScoredCtx& ctx,
                                    uint64_t) const final {
      if (!Scores()) {
        return irs::memory::make_managed<ProbeDocs>(DocList::DocidsT{});
      }
      return irs::memory::make_managed<ProbeScored>(
        DocList::DocidsT{}, Segment(), ctx, irs::kNoBoost, Stats().stats);
    }
    irs::fill::Node::ptr PlanFill(const irs::search::ScoredCtx& ctx,
                                  irs::ScoreMergeType merge) const final {
      if (!Scores()) {
        return irs::memory::make_managed<FillDocs>(DocList::DocidsT{});
      }
      return irs::memory::make_managed<FillScored>(
        DocList::DocidsT{}, Segment(), ctx, irs::kNoBoost, Stats().stats,
        merge);
    }
  };

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment,
    const irs::PrepareContext& /*ctx*/) const final {
    return irs::memory::make_managed<Estimated::Prepared>(segment, est,
                                                          &evaluated);
  }

  bool equals(const irs::Filter& rhs) const noexcept final {
    return this == &rhs;
  }

  mutable bool evaluated = false;
  uint32_t est{};
};

}  // namespace detail
namespace {

detail::Boosted& AddDocs(irs::BooleanFilter& root, irs::Occur occur,
                         detail::DocList::DocidsT docs,
                         irs::score_t boost = irs::kNoBoost) {
  auto& node = AddChild<detail::Boosted>(root, occur);
  node.docs = std::move(docs);
  node.SetBoost(boost);
  return node;
}

std::vector<irs::doc_id_t> Collect(irs::lead::Node& docs) {
  std::vector<irs::doc_id_t> result;
  while (!irs::doc_limits::eof(docs.Advance())) {
    result.push_back(docs.Value());
  }
  EXPECT_TRUE(irs::doc_limits::eof(docs.Advance()));
  EXPECT_TRUE(irs::doc_limits::eof(docs.Value()));
  return result;
}

std::vector<irs::doc_id_t> Disjunction(
  std::span<const std::vector<irs::doc_id_t>> docs) {
  std::vector<irs::doc_id_t> result;
  for (const auto& part : docs) {
    result.insert(result.end(), part.begin(), part.end());
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

std::vector<irs::doc_id_t> Threshold(
  std::span<const std::vector<irs::doc_id_t>> docs, size_t min_match) {
  std::map<irs::doc_id_t, size_t> counts;
  for (const auto& part : docs) {
    for (const auto doc : part) {
      ++counts[doc];
    }
  }
  std::vector<irs::doc_id_t> result;
  for (const auto& [doc, count] : counts) {
    if (count >= min_match) {
      result.push_back(doc);
    }
  }
  return result;
}

std::vector<irs::doc_id_t> Conjunction(
  std::span<const std::vector<irs::doc_id_t>> docs) {
  return Threshold(docs, docs.size());
}

irs::BooleanFilter MakeBucket(std::span<const std::vector<irs::doc_id_t>> docs,
                              irs::Occur occur) {
  irs::BooleanFilter root;
  for (const auto& part : docs) {
    AddDocs(root, occur, part);
  }
  return root;
}

}  // namespace

TEST(boolean_query_boost, hierarchy) {
  // hierarchy of boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    root.SetBoost(value);
    {
      auto& sub = AddBool(root, irs::Occur::Must);
      sub.SetBoost(value);
      AddDocs(sub, irs::Occur::Should, {1, 2}, value);
      AddDocs(sub, irs::Occur::Should, {1, 2, 3}, value);
      AsDisjunction(sub);
    }

    {
      auto& sub = AddBool(root, irs::Occur::Must);
      sub.SetBoost(value);
      AddDocs(sub, irs::Occur::Should, {1, 2}, value);
      AddDocs(sub, irs::Occur::Should, {1, 2, 3}, value);
      AsDisjunction(sub);
    }

    AddDocs(root, irs::Occur::Must, {1, 2}, value);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();

    /* the first hit should be scored as 2*value^3 +2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(4 * value * value * value + value * value, doc_boost);
    }

    /* the second hit should be scored as 2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(4 * value * value * value + value * value, doc_boost);
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    root.SetBoost(value);
    {
      auto& sub = AddBool(root, irs::Occur::Must);
      sub.SetBoost(value);
      AddDocs(sub, irs::Occur::Should, {1, 2}, value);
      AddDocs(sub, irs::Occur::Should, {1, 3}, value);
      AddDocs(sub, irs::Occur::Should, {1, 2});
      AsDisjunction(sub);
    }

    {
      auto& sub = AddBool(root, irs::Occur::Must);
      AddDocs(sub, irs::Occur::Should, {1, 2}, value);
      AddDocs(sub, irs::Occur::Should, {1, 2, 3}, value);
      AddDocs(sub, irs::Occur::Should, {1}, value);
      AsDisjunction(sub);
    }

    AddDocs(root, irs::Occur::Must, {1, 2, 3});

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());

    /* the first hit should be scored as 2*value^3+value^2+3*value^2+value
     * since it exists in all results */
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(2 * value * value * value + 4 * value * value + value,
                doc_boost);
    }

    /* the second hit should be scored as value^3+value^2+2*value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value * value + 3 * value * value + value, doc_boost);
    }

    /* the third hit should be scored as value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value * value + value * value + value, doc_boost);
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    root.SetBoost(value);
    {
      auto& sub = AddBool(root, irs::Occur::Should);
      sub.SetBoost(value);
      AddDocs(sub, irs::Occur::Must, {1, 2});
      AddDocs(sub, irs::Occur::Must, {1, 3}, value);
      AddDocs(sub, irs::Occur::Must, {1, 2});
    }

    {
      auto& sub = AddBool(root, irs::Occur::Should);
      AddDocs(sub, irs::Occur::Must, {1, 2}, value);
      AddDocs(sub, irs::Occur::Must, {1, 2, 3}, value);
      AddDocs(sub, irs::Occur::Must, {1}, value);
    }

    AddDocs(root, irs::Occur::Should, {1, 2, 3});
    AsDisjunction(root);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());

    // the first hit should be scored as value^3+2*value^2+3*value^2+value
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value * value + 5 * value * value + value, doc_boost);
    }

    // the second hit should be scored as value
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value, doc_boost);
    }

    // the third hit should be scored as value
    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value, doc_boost);
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }
}

TEST(boolean_query_boost, and_filter) {
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, {1}, value);

    auto opt = tests::Optimized(std::move(root), &sort);
    tests::PreparedFilter prep{*opt, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value, doc_boost);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // boosted root & single boosted subquery (root boost folds into the child)
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, {1}, value);
    root.SetBoost(value);

    auto opt = tests::Optimized(std::move(root), &sort);
    tests::PreparedFilter prep{*opt, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value * value, doc_boost);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, {1}, value);
    AddDocs(root, irs::Occur::Must, {1, 2}, value);
    root.SetBoost(value);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    /* the first hit should be scored as value*value + value*value since it
     * exists in both results */
    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(2 * value * value, doc_boost);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    root.SetBoost(value);
    AddDocs(root, irs::Occur::Must, {1}, value);
    AddDocs(root, irs::Occur::Must, {1, 2}, value);
    AddDocs(root, irs::Occur::Must, {1, 2});
    AddDocs(root, irs::Occur::Must, {1, 2}, value);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(3 * value * value + value, doc_boost);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // unboosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, {1}, value);
    AddDocs(root, irs::Occur::Must, {1, 2}, value);
    AddDocs(root, irs::Occur::Must, {1, 2}, 0.f);
    AddDocs(root, irs::Occur::Must, {1, 2}, value);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(3 * value, doc_boost);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // unboosted root & several unboosted subqueries
  {
    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, {1}, 0.f);
    AddDocs(root, irs::Occur::Must, {1, 2}, 0.f);
    AddDocs(root, irs::Occur::Must, {1, 2}, 0.f);
    AddDocs(root, irs::Occur::Must, {1, 2}, 0.f);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(irs::score_t(0), doc_boost);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }
}

TEST(boolean_query_boost, or_filter) {
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Should, {1}, value);
    AsDisjunction(root);

    auto opt = tests::Optimized(std::move(root), &sort);
    tests::PreparedFilter prep{*opt, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value, doc_boost);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // boosted root & single boosted subquery (root boost folds into the child)
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    root.SetBoost(value);
    AddDocs(root, irs::Occur::Should, {1}, value);
    AsDisjunction(root);

    auto opt = tests::Optimized(std::move(root), &sort);
    tests::PreparedFilter prep{*opt, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value * value, doc_boost);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    root.SetBoost(value);
    AddDocs(root, irs::Occur::Should, {1}, value);
    AddDocs(root, irs::Occur::Should, {1, 2}, value);
    AsDisjunction(root);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());

    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(2 * value * value, doc_boost);
    }

    {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value, doc_boost);
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }

  // unboosted root & several unboosted subqueries
  {
    tests::sort::Boost sort;
    detail::SegmentReaderMock segment{8};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Should, {1}, 0.f);
    AddDocs(root, irs::Occur::Should, {1, 2}, 0.f);
    AsDisjunction(root);

    tests::PreparedFilter prep{root, segment, &sort};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prep.ExecuteScored(0, fetcher);

    const auto scr = docs->PrepareScore();
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(irs::score_t(0), doc_boost);
  }
}

TEST(boolean_query_estimation, or_filter) {
  MaxMemoryCounter counter;

  // estimated subqueries
  {
    irs::BooleanFilter root;
    std::vector<detail::Estimated*> estimated;
    const auto add = [&](uint32_t est) {
      auto& node = AddChild<detail::Estimated>(root, irs::Occur::Should);
      node.est = est;
      estimated.emplace_back(&node);
    };
    add(100);
    add(320);
    add(10);
    add(1);
    add(100);
    AsDisjunction(root);

    detail::SegmentReaderMock segment_reader{1000};
    tests::PreparedFilter prep{root, segment_reader, nullptr, counter};

    for (auto* est_query : estimated) {
      ASSERT_TRUE(est_query->evaluated);
    }

    ASSERT_EQ(531, prep.Estimate(0));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // unestimated subqueries
  {
    irs::BooleanFilter root;
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    AsDisjunction(root);

    detail::SegmentReaderMock segment_reader{1000};
    tests::PreparedFilter prep{root, segment_reader};
    ASSERT_EQ(1000, prep.Estimate(0));
  }

  // estimated/unestimated subqueries
  {
    irs::BooleanFilter root;
    std::vector<detail::Estimated*> estimated;
    const auto add = [&](uint32_t est) {
      auto& node = AddChild<detail::Estimated>(root, irs::Occur::Should);
      node.est = est;
      estimated.emplace_back(&node);
    };
    add(100);
    add(320);
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    add(10);
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    add(1);
    add(100);
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    AsDisjunction(root);

    detail::SegmentReaderMock segment_reader{1000};
    tests::PreparedFilter prep{root, segment_reader};

    for (auto* est_query : estimated) {
      ASSERT_TRUE(est_query->evaluated);
    }

    ASSERT_EQ(1000, prep.Estimate(0));
  }

  // estimated/unestimated/negative subqueries
  {
    irs::BooleanFilter root;
    std::vector<detail::Estimated*> estimated;
    const auto add_estimated = [&](uint32_t est) {
      auto& node = AddChild<detail::Estimated>(root, irs::Occur::Should);
      node.est = est;
      estimated.emplace_back(&node);
    };
    const auto add_negation = [&](auto make) {
      auto& sub = AddBool(root, irs::Occur::Should);
      make(sub);
    };
    add_estimated(100);
    add_estimated(320);
    add_negation([](irs::BooleanFilter& sub) {
      AddChild<detail::Estimated>(sub, irs::Occur::MustNot).est = 3;
    });
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    add_estimated(10);
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    add_estimated(7);
    add_estimated(100);
    add_negation([](irs::BooleanFilter& sub) {
      AddChild<detail::Unestimated>(sub, irs::Occur::MustNot);
    });
    add_negation([](irs::BooleanFilter& sub) {
      AddChild<detail::Estimated>(sub, irs::Occur::MustNot).est = 0;
    });
    AddChild<detail::Unestimated>(root, irs::Occur::Should);
    AsDisjunction(root);

    // we need order to suppress optimization
    // which will clean include group and leave only 'all' filter
    tests::sort::Boost impl;
    const irs::Scorer* sort{&impl};

    auto optimized = tests::Optimized(std::move(root), sort);

    detail::SegmentReaderMock segment_reader{1000};
    tests::PreparedFilter prep{*optimized, segment_reader, sort};

    ASSERT_EQ(1000, prep.Estimate(0));

    for (auto* est_query : estimated) {
      ASSERT_TRUE(est_query->evaluated);
    }
  }

  // A node with no clauses at all is not a query: `Valid()` rejects it, and
  // preparing one is a caller error rather than an answer of zero.
  {
    irs::BooleanFilter root;
    ASSERT_FALSE(root.Valid());
  }
}

TEST(boolean_query_estimation, and_filter) {
  // estimated subqueries
  {
    irs::BooleanFilter root;
    std::vector<detail::Estimated*> estimated;
    const auto add = [&](uint32_t est) {
      auto& node = AddChild<detail::Estimated>(root, irs::Occur::Must);
      node.est = est;
      estimated.emplace_back(&node);
    };
    add(100);
    add(320);
    add(10);
    add(1);
    add(100);

    detail::SegmentReaderMock segment_reader{1000};
    tests::PreparedFilter prep{root, segment_reader};

    for (auto* est_query : estimated) {
      ASSERT_TRUE(est_query->evaluated);
    }

    ASSERT_EQ(1, prep.Estimate(0));
  }

  // unestimated subqueries
  {
    irs::BooleanFilter root;
    AddChild<detail::Unestimated>(root, irs::Occur::Must);
    AddChild<detail::Unestimated>(root, irs::Occur::Must);
    AddChild<detail::Unestimated>(root, irs::Occur::Must);
    AddChild<detail::Unestimated>(root, irs::Occur::Must);

    detail::SegmentReaderMock segment_reader{1000};
    tests::PreparedFilter prep{root, segment_reader};

    ASSERT_EQ(1000, prep.Estimate(0));
  }

  // estimated/unestimated subqueries
  {
    irs::BooleanFilter root;
    std::vector<detail::Estimated*> estimated;
    const auto add = [&](uint32_t est) {
      auto& node = AddChild<detail::Estimated>(root, irs::Occur::Must);
      node.est = est;
      estimated.emplace_back(&node);
    };
    add(100);
    add(320);
    AddChild<detail::Unestimated>(root, irs::Occur::Must);
    add(10);
    AddChild<detail::Unestimated>(root, irs::Occur::Must);
    add(1);
    add(100);
    AddChild<detail::Unestimated>(root, irs::Occur::Must);

    detail::SegmentReaderMock segment_reader{1000};
    tests::PreparedFilter prep{root, segment_reader};

    for (auto* est_query : estimated) {
      ASSERT_TRUE(est_query->evaluated);
    }

    ASSERT_EQ(1, prep.Estimate(0));
  }

  // estimated/unestimated/negative subqueries
  {
    irs::BooleanFilter root;
    std::vector<detail::Estimated*> estimated;
    const auto add_estimated = [&](uint32_t est) {
      auto& node = AddChild<detail::Estimated>(root, irs::Occur::Must);
      node.est = est;
      estimated.emplace_back(&node);
    };
    add_estimated(100);
    add_estimated(320);
    AddChild<detail::Estimated>(root, irs::Occur::MustNot).est = 3;
    AddChild<detail::Unestimated>(root, irs::Occur::Must);
    add_estimated(10);
    AddChild<detail::Unestimated>(root, irs::Occur::Must);
    add_estimated(7);
    add_estimated(100);
    AddChild<detail::Unestimated>(root, irs::Occur::MustNot);
    AddChild<detail::Estimated>(root, irs::Occur::MustNot).est = 0;
    AddChild<detail::Unestimated>(root, irs::Occur::Must);

    detail::SegmentReaderMock segment_reader{1000};
    auto optimized = tests::Optimized(std::move(root));
    tests::PreparedFilter prep{*optimized, segment_reader};

    for (auto* est_query : estimated) {
      ASSERT_TRUE(est_query->evaluated);
    }

    ASSERT_EQ(7, prep.Estimate(0));
  }

  // A node with no clauses at all is not a query: `Valid()` rejects it, and
  // preparing one is a caller error rather than an answer of zero.
  {
    irs::BooleanFilter root;
    ASSERT_FALSE(root.Valid());
  }
}

TEST(boolean_disjunction, next) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<std::vector<irs::doc_id_t>>> cases{
    {{1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}},
    {{1, 2, 5, 7, 9, 11, 45}, {}},
    {{}, {1, 5, 6, 12, 29}},
    {{1, 2, 5, 7, 9, 11, 45}, {1, 2, 5, 7, 9, 11, 45}},
    {{24}, {}},
    {{}, {}},
    {{1, 5, 6}, {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}},
    {{1, 5, 6}, {1, 2, 5, 7, 9, 11, 45}, {}, {1, 5, 6, 12, 29}},
    {{1, 5, 6}, {1, 5, 79, 101, 141, 1025, 1101}, {1, 5, 6, 12, 29}},
  };

  for (const auto& docs : cases) {
    auto root = MakeBucket(docs, irs::Occur::Should);
    AsDisjunction(root);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
    ASSERT_EQ(Disjunction(docs), Collect(*it));
  }
}

TEST(boolean_disjunction, seek) {
  detail::SegmentReaderMock segment{2048};

  {
    const std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                       {1, 5, 6, 12, 29}};
    const std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {2, 2},
      {5, 5},
      {8, 9},
      {13, 29},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    auto root = MakeBucket(docs, irs::Occur::Should);
    AsDisjunction(root);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it->Seek(target.target))
        << " for target " << target.target;
    }
  }

  {
    const std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                       {1, 5, 6, 12, 29}};
    const std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {100000, irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()}};

    auto root = MakeBucket(docs, irs::Occur::Should);
    AsDisjunction(root);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it->Seek(target.target))
        << " for target " << target.target;
    }
  }

  // empty datasets
  {
    const std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};
    const std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};

    auto root = MakeBucket(docs, irs::Occur::Should);
    AsDisjunction(root);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it->Seek(target.target))
        << " for target " << target.target;
    }
  }
}

TEST(boolean_disjunction, seek_next) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6, 12, 29}};

  auto root = MakeBucket(docs, irs::Occur::Should);
  AsDisjunction(root);

  tests::PreparedFilter prep{root, segment};
  auto it = prep.Execute(0);
  ASSERT_TRUE(bool(it));

  ASSERT_EQ(5, it->Seek(4));
  ASSERT_EQ(6, it->Advance());
  ASSERT_EQ(7, it->Advance());
  ASSERT_EQ(11, it->Seek(10));
  ASSERT_EQ(12, it->Advance());
  ASSERT_EQ(29, it->Advance());
  ASSERT_EQ(45, it->Advance());
  ASSERT_TRUE(irs::doc_limits::eof(it->Advance()));
}

TEST(boolean_disjunction, scored) {
  const irs::score_t value = 5;

  tests::sort::Boost sort;
  detail::SegmentReaderMock segment{2048};

  irs::BooleanFilter root;
  AddDocs(root, irs::Occur::Should, {1, 2, 5, 7, 9, 11, 45}, value);
  AddDocs(root, irs::Occur::Should, {1, 5, 6, 12, 29}, value);
  AsDisjunction(root);

  tests::PreparedFilter prep{root, segment, &sort};

  irs::ColumnArgsFetcher fetcher;
  auto it = prep.ExecuteScored(0, fetcher);
  ASSERT_TRUE(bool(it));
  const auto scr = it->PrepareScore();
  ASSERT_FALSE(scr.IsDefault());

  ASSERT_EQ(1, it->Advance());
  it->FetchScoreArgs(0);
  ASSERT_EQ(2 * value, scr.Score());

  ASSERT_EQ(2, it->Advance());
  it->FetchScoreArgs(0);
  ASSERT_EQ(value, scr.Score());
}

TEST(boolean_min_match, next) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 2, 5, 7, 9, 11, 45},
    {1, 5, 6, 12, 29},
    {1, 5, 6, 12, 29},
    {1, 5, 79, 101, 141, 1025, 1101}};

  for (size_t min_match = 1; min_match <= docs.size(); ++min_match) {
    auto root = MakeBucket(docs, irs::Occur::Should);
    root.SetMinShouldMatch(static_cast<uint32_t>(min_match));

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    ASSERT_EQ(Threshold(docs, min_match), Collect(*it))
      << " for min_match " << min_match;
  }
}

TEST(boolean_min_match, seek) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6, 12, 29}};

  const std::vector<detail::SeekDoc> expected{
    {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
    {1, 1},
    {2, 5},
    {6, 6},
    {7, 12},
    {29, 29},
    {30, irs::doc_limits::eof()}};

  auto root = MakeBucket(docs, irs::Occur::Should);
  root.SetMinShouldMatch(2);

  tests::PreparedFilter prep{root, segment};
  auto it = prep.Execute(0);
  ASSERT_TRUE(bool(it));
  for (const auto& target : expected) {
    ASSERT_EQ(target.expected, it->Seek(target.target))
      << " for target " << target.target;
  }
}

TEST(boolean_min_match, threshold_above_clause_count) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5}, {1, 5}};

  irs::BooleanFilter root;
  for (const auto& part : docs) {
    AddDocs(root, irs::Occur::Should, part);
  }
  ASSERT_EQ(2, root.Size(irs::Occur::Should));
  root.SetMinShouldMatch(2);
  ASSERT_EQ(2, root.MinShouldMatch());
  ASSERT_TRUE(root.Valid());

  tests::PreparedFilter prep{root, segment};
  auto it = prep.Execute(0);
  ASSERT_TRUE(bool(it));
  ASSERT_EQ(Conjunction(docs), Collect(*it));
}

TEST(boolean_conjunction, next) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<std::vector<irs::doc_id_t>>> cases{
    {{1, 5, 6},
     {1, 2, 5, 7, 9, 11, 45},
     {1, 5, 6, 12, 29},
     {1, 5, 79, 101, 141, 1025, 1101}},
    {{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
      17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
     {1, 5, 11, 21, 27, 31}},
    {{1, 5, 11, 21, 27, 31},
     {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
      17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}},
    {{1, 2, 5, 7, 9, 11, 45}, {}},
    {{}, {}},
  };

  for (const auto& docs : cases) {
    auto root = MakeBucket(docs, irs::Occur::Must);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
    ASSERT_EQ(Conjunction(docs), Collect(*it));
  }
}

TEST(boolean_conjunction, seek) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29, 45}, {1, 5, 6, 12, 29, 45}};

  const std::vector<detail::SeekDoc> expected{
    {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
    {1, 1},
    {2, 5},
    {5, 5},
    {6, 45},
    {45, 45},
    {46, irs::doc_limits::eof()}};

  auto root = MakeBucket(docs, irs::Occur::Must);

  tests::PreparedFilter prep{root, segment};
  auto it = prep.Execute(0);
  ASSERT_TRUE(bool(it));
  for (const auto& target : expected) {
    ASSERT_EQ(target.expected, it->Seek(target.target))
      << " for target " << target.target;
  }
}

TEST(boolean_conjunction, seek_next) {
  detail::SegmentReaderMock segment{2048};

  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                     {1, 5, 6, 9, 12, 29, 45}};

  auto root = MakeBucket(docs, irs::Occur::Must);

  tests::PreparedFilter prep{root, segment};
  auto it = prep.Execute(0);
  ASSERT_TRUE(bool(it));

  ASSERT_EQ(5, it->Seek(3));
  ASSERT_EQ(9, it->Advance());
  ASSERT_EQ(45, it->Seek(10));
  ASSERT_TRUE(irs::doc_limits::eof(it->Advance()));
}

TEST(boolean_conjunction, scored) {
  const irs::score_t value = 5;

  tests::sort::Boost sort;
  detail::SegmentReaderMock segment{2048};

  irs::BooleanFilter root;
  AddDocs(root, irs::Occur::Must, {1, 2, 5, 7}, value);
  AddDocs(root, irs::Occur::Must, {1, 5, 6, 7}, value);

  tests::PreparedFilter prep{root, segment, &sort};

  irs::ColumnArgsFetcher fetcher;
  auto it = prep.ExecuteScored(0, fetcher);
  ASSERT_TRUE(bool(it));
  const auto scr = it->PrepareScore();
  ASSERT_FALSE(scr.IsDefault());

  for (const irs::doc_id_t expected : {1, 5, 7}) {
    ASSERT_EQ(expected, it->Advance());
    it->FetchScoreArgs(0);
    ASSERT_EQ(2 * value, scr.Score());
  }
  ASSERT_TRUE(irs::doc_limits::eof(it->Advance()));
}

TEST(boolean_exclusion, next) {
  detail::SegmentReaderMock segment{2048};

  struct Case {
    std::vector<irs::doc_id_t> included;
    std::vector<irs::doc_id_t> excluded;
    std::vector<irs::doc_id_t> expected;
  };

  const std::vector<Case> cases{
    {{1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {2, 7, 9, 11, 45}},
    {{1, 2, 5, 7, 9, 11, 45}, {}, {1, 2, 5, 7, 9, 11, 45}},
    {{}, {1, 5, 6, 12, 29}, {}},
    {{1, 2, 5, 7, 9, 11, 45}, {1, 2, 5, 7, 9, 11, 45}, {}},
    {{24}, {}, {24}},
    {{}, {}, {}},
  };

  for (const auto& test : cases) {
    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, test.included);
    AddDocs(root, irs::Occur::MustNot, test.excluded);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
    ASSERT_EQ(test.expected, Collect(*it));
  }
}

TEST(boolean_exclusion, seek) {
  detail::SegmentReaderMock segment{2048};

  // 2, 7, 9, 11, 45
  const std::vector<irs::doc_id_t> included{1, 2, 5, 7, 9, 11, 29, 45};
  const std::vector<irs::doc_id_t> excluded{1, 5, 6, 12, 29};

  {
    const std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 2},
      {5, 7},
      {irs::doc_limits::invalid(), 7},
      {9, 9},
      {45, 45},
      {43, 45},
      {57, irs::doc_limits::eof()}};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, included);
    AddDocs(root, irs::Occur::MustNot, excluded);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it->Seek(target.target))
        << " for target " << target.target;
    }
  }

  {
    const std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {100000, irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, included);
    AddDocs(root, irs::Occur::MustNot, excluded);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it->Seek(target.target))
        << " for target " << target.target;
    }
  }

  {
    const std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {7, 7},
      {11, 11},
      {irs::doc_limits::invalid(), 11},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    irs::BooleanFilter root;
    AddDocs(root, irs::Occur::Must, included);
    AddDocs(root, irs::Occur::MustNot, excluded);

    tests::PreparedFilter prep{root, segment};
    auto it = prep.Execute(0);
    ASSERT_TRUE(bool(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it->Seek(target.target))
        << " for target " << target.target;
    }
  }
}

TEST(BooleanFilter_test, ctor) {
  irs::BooleanFilter q;
  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), q.type());
  for (const auto occur : irs::kAllOccur) {
    ASSERT_EQ(0, q.Size(occur));
    ASSERT_TRUE(q.Terms(occur).empty());
    ASSERT_TRUE(q.Filters(occur).empty());
  }
  ASSERT_EQ(0, q.MinShouldMatch());
  ASSERT_EQ(irs::ScoreMergeType::Sum, q.MergeType());
  ASSERT_EQ(irs::kNoBoost, q.GetBoost());
  ASSERT_FALSE(q.Valid());
}

TEST(BooleanFilter_test, term_decomposes) {
  irs::BooleanFilter q;
  q.Add(
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>(kFieldAbc, "def")),
    irs::Occur::Must);

  ASSERT_EQ(1, q.Size(irs::Occur::Must));
  ASSERT_TRUE(q.Filters(irs::Occur::Must).empty());
  const auto terms = q.Terms(irs::Occur::Must);
  ASSERT_EQ(1, terms.size());
  ASSERT_EQ(kFieldAbc, terms[0].field);
  ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("def")),
            irs::bytes_view{terms[0].term});
  ASSERT_EQ(irs::kNoBoost, terms[0].boost);
  ASSERT_EQ(nullptr, terms[0].scorer);
}

TEST(BooleanFilter_test, non_term_stays_a_filter) {
  irs::BooleanFilter q;
  Append<irs::ByPrefix>(q, irs::Occur::Should, kFieldAbc, "de");
  AsDisjunction(q);

  ASSERT_EQ(1, q.Size(irs::Occur::Should));
  ASSERT_TRUE(q.Terms(irs::Occur::Should).empty());
  ASSERT_EQ(1, q.Filters(irs::Occur::Should).size());
  ASSERT_EQ(irs::Type<irs::ByPrefix>::id(),
            q.Filters(irs::Occur::Should)[0]->type());
  ASSERT_EQ(1, CountChildren(q));
}

TEST(BooleanFilter_test, duplicate_term_merges_boost) {
  // `Add` keeps both copies: how two clauses' scores meet is the node's merge
  // type, and a threshold counts how many clauses matched -- neither is
  // settled while the node is still filling, so `boolean_dedup` folds them.
  {
    irs::BooleanFilter q;
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 2.f);
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 3.f);
    ASSERT_EQ(2, q.Size(irs::Occur::Must));

    const auto optimized = tests::Optimized(std::move(q));
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), optimized->type());
    EXPECT_EQ(5.f, optimized->GetBoost());
  }

  {
    irs::BooleanFilter q;
    q.SetMergeType(irs::ScoreMergeType::Max);
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 2.f);
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 3.f);
    ASSERT_EQ(2, q.Size(irs::Occur::Must));

    const auto optimized = tests::Optimized(std::move(q));
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), optimized->type());
    EXPECT_EQ(3.f, optimized->GetBoost());
  }

  {
    tests::sort::Boost sort;
    irs::BooleanFilter q;
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 2.f);
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 3.f, &sort);

    const auto optimized = tests::Optimized(std::move(q), &sort);
    ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), optimized->type());
    const auto& node = sdb::basics::downCast<irs::BooleanFilter>(*optimized);
    EXPECT_EQ(2, node.Size(irs::Occur::Must));
  }

  {
    tests::sort::Boost sort;
    irs::BooleanFilter q;
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 2.f);
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 3.f, &sort);

    const auto optimized = tests::Optimized(std::move(q));
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), optimized->type());
  }

  // A required clause is met by every document the node returns, so the
  // optional copy is dropped and its boost goes to the survivor.
  {
    irs::BooleanFilter q;
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def", 2.f);
    AddTerm(q, irs::Occur::Should, kFieldAbc, "def", 3.f);
    AsDisjunction(q);
    ASSERT_EQ(1, q.Size(irs::Occur::Must));
    ASSERT_EQ(1, q.Size(irs::Occur::Should));

    const auto optimized = tests::Optimized(std::move(q));
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), optimized->type());
  }
}

TEST(BooleanFilter_test, negated_term_carries_no_score) {
  tests::sort::Boost sort;
  irs::BooleanFilter q;
  AddTerm(q, irs::Occur::MustNot, kFieldAbc, "def", 2.f, &sort);

  ASSERT_EQ(1, q.Size(irs::Occur::MustNot));
  const auto terms = q.Terms(irs::Occur::MustNot);
  ASSERT_EQ(irs::kNoBoost, terms[0].boost);
  ASSERT_EQ(nullptr, terms[0].scorer);

  AddTerm(q, irs::Occur::MustNot, kFieldAbc, "def", 5.f);
  ASSERT_EQ(1, q.Size(irs::Occur::MustNot));
}

TEST(BooleanFilter_test, terms_are_sorted) {
  irs::BooleanFilter q;
  AddTerm(q, irs::Occur::Must, kFieldAbcd, "b");
  AddTerm(q, irs::Occur::Must, kFieldAbc, "z");
  AddTerm(q, irs::Occur::Must, kFieldAbc, "a");

  const auto terms = q.Terms(irs::Occur::Must);
  ASSERT_EQ(3, terms.size());
  ASSERT_TRUE(absl::c_is_sorted(terms, irs::TermClauseLess{}));
}

TEST(BooleanFilter_test, valid) {
  {
    irs::BooleanFilter q;
    ASSERT_FALSE(q.Valid());
  }

  {
    irs::BooleanFilter q;
    AddTerm(q, irs::Occur::Should, kFieldAbc, "def");
    ASSERT_FALSE(q.Valid());
    q.SetMinShouldMatch(1);
    ASSERT_TRUE(q.Valid());
  }

  {
    irs::BooleanFilter q;
    AddTerm(q, irs::Occur::Must, kFieldAbc, "def");
    AddTerm(q, irs::Occur::Should, kFieldAbcd, "def");
    ASSERT_TRUE(q.Valid());
  }

  {
    irs::BooleanFilter q;
    AddTerm(q, irs::Occur::MustNot, kFieldAbc, "def");
    ASSERT_TRUE(q.Valid());
  }
}

TEST(BooleanFilter_test, equal) {
  const auto build = [](irs::field_id extra) {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldField, "term");
    AddTerm(root, irs::Occur::Must, kFieldField1, "term1");
    auto& subq = AddBool(root, irs::Occur::Must);
    AddTerm(subq, irs::Occur::Must, kFieldField123, "dfterm");
    AddTerm(subq, irs::Occur::Must, kFieldFieasfdld1, "term1");
    if (extra != irs::field_limits::invalid()) {
      AddTerm(subq, irs::Occur::Must, extra, "term1");
    }
    return root;
  };

  const auto lhs = build(irs::field_limits::invalid());
  ASSERT_EQ(lhs, build(irs::field_limits::invalid()));
  ASSERT_NE(lhs, build(kFieldField));

  {
    auto rhs = build(irs::field_limits::invalid());
    rhs.SetMergeType(irs::ScoreMergeType::Max);
    ASSERT_NE(lhs, rhs);
  }
}

TEST(BooleanFilter_test, visit_children_skips_terms) {
  irs::BooleanFilter root;
  AddTerm(root, irs::Occur::Must, kFieldField, "term");
  Append<irs::ByPrefix>(root, irs::Occur::Must, kFieldField, "te");
  AddTerm(root, irs::Occur::MustNot, kFieldField1, "term1");
  Append<irs::ByPrefix>(root, irs::Occur::MustNot, kFieldField1, "te");

  ASSERT_EQ(2, CountChildren(root));
}

// A node is what it includes, less what it excludes, so the two query
// surfaces differ only in the tree they build: Lucene's `-x` leaves the
// include side empty, SQL's `NOT x` includes `All`.
TEST(BooleanFilter_test, optimize_double_negation) {
  // Lucene `-(-x)`. An empty include has nothing to exclude from, so `-x` is
  // already nothing and negating it again does not bring the term back.
  {
    auto root = std::make_unique<irs::BooleanFilter>();
    auto& inner = AddBool(*root, irs::Occur::MustNot);
    AddTerm(inner, irs::Occur::MustNot, kFieldTestField, "test_term");

    irs::Filter::ptr filter = std::move(root);
    irs::Optimize(filter);

    ASSERT_EQ(irs::Type<irs::Empty>::id(), filter->type());
  }

  // SQL `NOT NOT x`. Each negation includes `All`, so the two complements
  // cancel and the term is what is left.
  {
    auto root = std::make_unique<irs::BooleanFilter>();
    AddChild<irs::All>(*root, irs::Occur::Must);
    auto& inner = AddBool(*root, irs::Occur::MustNot);
    AddChild<irs::All>(inner, irs::Occur::Must);
    AddTerm(inner, irs::Occur::MustNot, kFieldTestField, "test_term");

    irs::Filter::ptr filter = std::move(root);
    irs::Optimize(filter);

    ASSERT_NE(irs::Type<irs::Empty>::id(), filter->type());
  }
}

TEST(BooleanFilter_test, optimize_single_node) {
  auto expect_lone_term = [](const irs::Filter& filter) {
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), filter.type());
    const auto& term = sdb::basics::downCast<irs::ByTerm>(filter);
    EXPECT_EQ(kFieldTestField, term.field_id());
    EXPECT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("test_term")),
              term.options().term);
  };

  // simple hierarchy
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldTestField, "test_term");

    expect_lone_term(*tests::Optimized(std::move(root)));
  }

  // complex hierarchy
  {
    irs::BooleanFilter root;
    auto& sub = AddBool(root, irs::Occur::Must);
    auto& subsub = AddBool(sub, irs::Occur::Must);
    AddTerm(subsub, irs::Occur::Must, kFieldTestField, "test_term");

    expect_lone_term(*tests::Optimized(std::move(root)));
  }

  {
    irs::BooleanFilter root;
    auto& sub = AddBool(root, irs::Occur::Should);
    auto& subsub = AddBool(sub, irs::Occur::Should);
    AddTerm(subsub, irs::Occur::Should, kFieldTestField, "test_term");
    AsDisjunction(subsub);
    AsDisjunction(sub);
    AsDisjunction(root);

    expect_lone_term(*tests::Optimized(std::move(root)));
  }
}

TEST(BooleanFilter_test, optimize_all_filters) {
  // single `all` filter
  {
    irs::BooleanFilter root;
    AddChild<irs::All>(root, irs::Occur::Must).SetBoost(5.f);

    tests::sort::Boost sort{};
    tests::PreparedFilter prepared{*tests::Optimized(std::move(root), &sort),
                                   irs::SubReader::empty(), &sort};
    const irs::All all;
    tests::PreparedFilter all_prepared{all, irs::SubReader::empty()};
    ASSERT_EQ(typeid(all_prepared.Query(0)), typeid(prepared.Query(0)));
    ASSERT_EQ(5.f, prepared.Query(0)->Boost());
  }

  {
    irs::BooleanFilter root;
    AddChild<irs::All>(root, irs::Occur::Must).SetBoost(5.f);

    tests::PreparedFilter prepared{*tests::Optimized(std::move(root)),
                                   irs::SubReader::empty()};
    const irs::All all;
    tests::PreparedFilter all_prepared{all, irs::SubReader::empty()};
    ASSERT_EQ(typeid(all_prepared.Query(0)), typeid(prepared.Query(0)));
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  // multiple `all` filters
  {
    auto root = std::make_unique<irs::BooleanFilter>();
    AddChild<irs::All>(*root, irs::Occur::Must).SetBoost(5.f);
    AddChild<irs::All>(*root, irs::Occur::Must).SetBoost(2.f);
    AddChild<irs::All>(*root, irs::Occur::Must).SetBoost(3.f);
    irs::Filter::ptr f = std::move(root);
    irs::Optimize(f, {.scored = true});

    ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
    const auto& node = sdb::basics::downCast<irs::BooleanFilter>(*f);
    const auto filters = node.Filters(irs::Occur::Must);
    ASSERT_EQ(3, filters.size());
    irs::score_t total = 0;
    for (const auto& child : filters) {
      ASSERT_EQ(irs::Type<irs::All>::id(), child->type());
      total += child->GetBoost();
    }
    ASSERT_EQ(10.f, total);
  }

  // multiple `all` filters + term filter
  {
    auto root = std::make_unique<irs::BooleanFilter>();
    AddChild<irs::All>(*root, irs::Occur::Must).SetBoost(5.f);
    AddChild<irs::All>(*root, irs::Occur::Must).SetBoost(2.f);
    AddTerm(*root, irs::Occur::Must, kFieldTestField, "test_term");
    irs::Filter::ptr f = std::move(root);
    irs::Optimize(f, {.scored = true});

    ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
    const auto& node = sdb::basics::downCast<irs::BooleanFilter>(*f);
    ASSERT_EQ(1, node.Terms(irs::Occur::Must).size());
    const auto filters = node.Filters(irs::Occur::Must);
    ASSERT_EQ(2, filters.size());
    irs::score_t total = 0;
    for (const auto& child : filters) {
      ASSERT_EQ(irs::Type<irs::All>::id(), child->type());
      total += child->GetBoost();
    }
    ASSERT_EQ(7.f, total);
  }
}

TEST(BooleanFilter_test, not_boosted) {
  tests::sort::Boost sort;
  detail::SegmentReaderMock segment{8};

  irs::BooleanFilter root;
  AddDocs(root, irs::Occur::Must, {1}, 5.f);
  AddDocs(root, irs::Occur::MustNot, {5, 6}, 4.f);

  tests::PreparedFilter prep{*tests::Optimized(std::move(root), &sort), segment,
                             &sort};
  irs::ColumnArgsFetcher fetcher;
  auto docs = prep.ExecuteScored(0, fetcher);
  const auto scr = docs->PrepareScore();
  ASSERT_FALSE(scr.IsDefault());

  ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
  docs->FetchScoreArgs(0);
  const auto doc_boost = scr.Score();
  ASSERT_EQ(5., doc_boost);  // FIXME: should be 9 if we will boost negation
  ASSERT_EQ(1, docs->Value());

  ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
}

TEST(BooleanFilter_test, optimize_all_unscored) {
  auto root = std::make_unique<irs::BooleanFilter>();
  detail::Boosted::gExecuteCount = 0;
  AddDocs(*root, irs::Occur::Should, {1});
  AddDocs(*root, irs::Occur::Should, {2});
  AddDocs(*root, irs::Occur::Should, {3});
  AddChild<irs::All>(*root, irs::Occur::Should);
  AddChild<irs::Empty>(*root, irs::Occur::Should);
  AddChild<irs::All>(*root, irs::Occur::Should);
  AddChild<irs::Empty>(*root, irs::Occur::Should);
  AsDisjunction(*root);
  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  detail::SegmentReaderMock segment{8};
  tests::PreparedFilter prep{*filter, segment};

  prep.Execute(0);
  // specific filters should be opt out
  ASSERT_EQ(0, detail::Boosted::gExecuteCount);
}

TEST(BooleanFilter_test, optimize_all_scored) {
  auto root = std::make_unique<irs::BooleanFilter>();
  detail::Boosted::gExecuteCount = 0;
  AddDocs(*root, irs::Occur::Should, {1});
  AddDocs(*root, irs::Occur::Should, {2});
  AddDocs(*root, irs::Occur::Should, {3});
  AddChild<irs::All>(*root, irs::Occur::Should);
  AddChild<irs::Empty>(*root, irs::Occur::Should);
  AddChild<irs::All>(*root, irs::Occur::Should);
  AddChild<irs::Empty>(*root, irs::Occur::Should);
  AsDisjunction(*root);
  tests::sort::Boost sort{};
  detail::SegmentReaderMock segment{8};
  tests::PreparedFilter prep{*root, segment, &sort};

  irs::ColumnArgsFetcher fetcher;
  prep.ExecuteScored(0, fetcher);
  // specific filters should be executed as score needs them
  ASSERT_EQ(3, detail::Boosted::gExecuteCount);
}

TEST(BooleanFilter_test, optimize_only_all_boosted) {
  tests::sort::Boost sort{};
  auto root = std::make_unique<irs::BooleanFilter>();
  root->SetBoost(2);
  AddChild<irs::All>(*root, irs::Occur::Should).SetBoost(3);
  AddChild<irs::All>(*root, irs::Occur::Should).SetBoost(5);
  AsDisjunction(*root);

  irs::Filter::ptr f = std::move(root);
  irs::Optimize(f, {.scored = true});
  tests::PreparedFilter prep{*f, irs::SubReader::empty(), &sort};

  irs::ColumnArgsFetcher fetcher;
  prep.ExecuteScored(0, fetcher);
  ASSERT_NE(nullptr, dynamic_cast<const irs::BooleanQuery*>(prep.Query(0)));
  auto& node = sdb::basics::downCast<irs::BooleanQuery>(*prep.Query(0));
  const auto& should = node.Bucket(irs::Occur::Should);
  ASSERT_EQ(2, should.all_docs.size());
  irs::score_t total = 0;
  for (const auto& clause : should.all_docs) {
    total += clause.boost;
  }
  ASSERT_EQ(16.f, total);
}

TEST(BooleanFilter_test, boosted_not) {
  tests::sort::Boost sort{};
  detail::SegmentReaderMock segment{8};

  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& neg = AddBool(*root, irs::Occur::Should);
    AddDocs(neg, irs::Occur::MustNot, {5, 6}, 4.f);
  }
  AddDocs(*root, irs::Occur::Should, {1}, 5.f);
  AsDisjunction(*root);

  irs::Filter::ptr f = std::move(root);
  irs::Optimize(f, {.scored = true});
  tests::PreparedFilter prep{*f, segment, &sort};

  irs::ColumnArgsFetcher fetcher;
  auto docs = prep.ExecuteScored(0, fetcher);
  const auto scr = docs->PrepareScore();
  ASSERT_FALSE(scr.IsDefault());

  ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
  docs->FetchScoreArgs(0);
  const auto doc_boost = scr.Score();
  ASSERT_EQ(5., doc_boost);  // FIXME: should be 9 if we will boost negation
  ASSERT_EQ(1, docs->Value());
  ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
}

namespace {

irs::bytes_view B(std::string_view value) {
  return irs::ViewCast<irs::byte_type>(value);
}

const irs::AutomatonFilter* FusedOf(const irs::Filter::ptr& filter) {
  return dynamic_cast<const irs::AutomatonFilter*>(filter.get());
}

bool FusedAccepts(const irs::AutomatonFilter& fused, std::string_view term) {
  return bool(irs::Accept(fused.options().compiled->acceptor, B(term)));
}

irs::ByRange& AddRange(irs::BooleanFilter& root, irs::Occur occur,
                       irs::field_id field) {
  auto& range = AddChild<irs::ByRange>(root, occur);
  *range.mutable_field_id() = field;
  return range;
}

irs::ByGranularRange& AddGranularRange(irs::BooleanFilter& root,
                                       irs::Occur occur, irs::field_id field) {
  auto& range = AddChild<irs::ByGranularRange>(root, occur);
  *range.mutable_field_id() = field;
  return range;
}

}  // namespace

TEST(AndRangeMerge_test, inverted_bounds_merge_to_empty) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("m")};
    lo.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("d")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), filter->type());
}

TEST(RangeDegenerate_test, inverted_bounds_become_empty) {
  auto range = std::make_unique<irs::ByRange>();
  *range->mutable_field_id() = kFieldTestField;
  range->mutable_options()->range.min = irs::bstring{B("z")};
  range->mutable_options()->range.min_type = irs::BoundType::Inclusive;
  range->mutable_options()->range.max = irs::bstring{B("a")};
  range->mutable_options()->range.max_type = irs::BoundType::Inclusive;

  irs::Filter::ptr filter = std::move(range);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), filter->type());
}

TEST(GranularRangeDegenerate_test, inverted_bounds_become_empty) {
  auto range = std::make_unique<irs::ByGranularRange>();
  *range->mutable_field_id() = kFieldTestField;
  range->mutable_options()->range.min.emplace_back(irs::bstring{B("z")});
  range->mutable_options()->range.min_type = irs::BoundType::Inclusive;
  range->mutable_options()->range.max.emplace_back(irs::bstring{B("a")});
  range->mutable_options()->range.max_type = irs::BoundType::Inclusive;

  irs::Filter::ptr filter = std::move(range);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), filter->type());
}

TEST(AndRangeMerge_test, merges_complementary_bounds) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    lo.SetBoost(2.f);
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("m")};
    hi.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    hi.SetBoost(3.f);
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = true});

  ASSERT_EQ(irs::Type<irs::ByRange>::id(), filter->type());
  const auto& merged = sdb::basics::downCast<irs::ByRange>(*filter);
  EXPECT_EQ(kFieldTestField, merged.field_id());
  EXPECT_EQ(irs::bstring{B("b")}, merged.options().range.min);
  EXPECT_EQ(irs::BoundType::Inclusive, merged.options().range.min_type);
  EXPECT_EQ(irs::bstring{B("m")}, merged.options().range.max);
  EXPECT_EQ(irs::BoundType::Exclusive, merged.options().range.max_type);
  EXPECT_EQ(5.f, merged.GetBoost());
}

TEST(AndRangeMerge_test, max_merge_type_takes_max_boost) {
  auto root = std::make_unique<irs::BooleanFilter>();
  root->SetMergeType(irs::ScoreMergeType::Max);
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    lo.SetBoost(2.f);
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("m")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    hi.SetBoost(3.f);
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = true});

  ASSERT_EQ(irs::Type<irs::ByRange>::id(), filter->type());
  EXPECT_EQ(3.f, sdb::basics::downCast<irs::ByRange>(*filter).GetBoost());
}

TEST(AndRangeMerge_test, noop_merge_type_drops_boost) {
  auto root = std::make_unique<irs::BooleanFilter>();
  root->SetMergeType(irs::ScoreMergeType::Noop);
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    lo.SetBoost(2.f);
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("m")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    hi.SetBoost(3.f);
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = true});

  ASSERT_EQ(irs::Type<irs::ByRange>::id(), filter->type());
  EXPECT_EQ(irs::kNoBoost,
            sdb::basics::downCast<irs::ByRange>(*filter).GetBoost());
}

TEST(AndRangeMerge_test, equal_bounds_inclusive_become_term) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("b")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), filter->type());
  const auto& term = sdb::basics::downCast<irs::ByTerm>(*filter);
  EXPECT_EQ(kFieldTestField, term.field_id());
  EXPECT_EQ(irs::bstring{B("b")}, term.options().term);
}

TEST(AndRangeMerge_test, equal_bounds_exclusive_become_empty) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("b")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), filter->type());
}

TEST(AndRangeMerge_test, keeps_different_fields) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField + 1);
    hi.mutable_options()->range.max = irs::bstring{B("m")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
  EXPECT_EQ(2, CountChildren(*filter));
}

TEST(AndRangeMerge_test, keeps_analyzed_fields) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("m")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.analyzed_fields = {kFieldTestField}});

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
  EXPECT_EQ(2, CountChildren(*filter));
}

TEST(AndRangeMerge_test, keeps_scored) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min = irs::bstring{B("b")};
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  }
  {
    auto& hi = AddRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max = irs::bstring{B("m")};
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = true, .analyzed_fields = {kFieldTestField}});

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
  EXPECT_EQ(2, CountChildren(*filter));
}

TEST(AndRangeMerge_test, granular_merges_complementary_bounds) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddGranularRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min.emplace_back(irs::bstring{B("b")});
    lo.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  }
  {
    auto& hi = AddGranularRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max.emplace_back(irs::bstring{B("m")});
    hi.mutable_options()->range.max_type = irs::BoundType::Exclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::ByGranularRange>::id(), filter->type());
  const auto& merged = sdb::basics::downCast<irs::ByGranularRange>(*filter);
  ASSERT_EQ(1, merged.options().range.min.size());
  ASSERT_EQ(1, merged.options().range.max.size());
  EXPECT_EQ(irs::bstring{B("b")}, merged.options().range.min.front());
  EXPECT_EQ(irs::BoundType::Inclusive, merged.options().range.min_type);
  EXPECT_EQ(irs::bstring{B("m")}, merged.options().range.max.front());
  EXPECT_EQ(irs::BoundType::Exclusive, merged.options().range.max_type);
}

TEST(AndRangeMerge_test, granular_inverted_bounds_merge_to_empty) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& lo = AddGranularRange(*root, irs::Occur::Must, kFieldTestField);
    lo.mutable_options()->range.min.emplace_back(irs::bstring{B("m")});
    lo.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  }
  {
    auto& hi = AddGranularRange(*root, irs::Occur::Must, kFieldTestField);
    hi.mutable_options()->range.max.emplace_back(irs::bstring{B("d")});
    hi.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), filter->type());
}

TEST(RangeDegenerate_test, equal_inclusive_bounds_become_term) {
  auto range = std::make_unique<irs::ByRange>();
  *range->mutable_field_id() = kFieldTestField;
  range->mutable_options()->range.min = irs::bstring{B("b")};
  range->mutable_options()->range.min_type = irs::BoundType::Inclusive;
  range->mutable_options()->range.max = irs::bstring{B("b")};
  range->mutable_options()->range.max_type = irs::BoundType::Inclusive;
  range->SetBoost(2.f);

  irs::Filter::ptr filter = std::move(range);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), filter->type());
  const auto& term = sdb::basics::downCast<irs::ByTerm>(*filter);
  EXPECT_EQ(kFieldTestField, term.field_id());
  EXPECT_EQ(irs::bstring{B("b")}, term.options().term);
  EXPECT_EQ(2.f, term.GetBoost());
}

TEST(GranularRangeDegenerate_test, equal_inclusive_bounds_become_term) {
  auto range = std::make_unique<irs::ByGranularRange>();
  *range->mutable_field_id() = kFieldTestField;
  range->mutable_options()->range.min.emplace_back(irs::bstring{B("b")});
  range->mutable_options()->range.min_type = irs::BoundType::Inclusive;
  range->mutable_options()->range.max.emplace_back(irs::bstring{B("b")});
  range->mutable_options()->range.max_type = irs::BoundType::Inclusive;

  irs::Filter::ptr filter = std::move(range);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), filter->type());
  EXPECT_EQ(irs::bstring{B("b")},
            sdb::basics::downCast<irs::ByTerm>(*filter).options().term);
}

TEST(OrAcceptorFusion_test, fuses_mixed_acceptors) {
  auto root = std::make_unique<irs::BooleanFilter>();
  AddTerm(*root, irs::Occur::Should, kFieldTestField, "kiwi");
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ax");
  Append<irs::ByWildcard>(*root, irs::Occur::Should, kFieldTestField, "b_n%");
  {
    auto& re = AddChild<irs::ByRegexp>(*root, irs::Occur::Should);
    *re.mutable_field_id() = kFieldTestField;
    re.mutable_options()->pattern = irs::bstring{B("a.*e")};
  }
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_EQ(kFieldTestField, fused->field_id());
  EXPECT_TRUE(FusedAccepts(*fused, "kiwi"));
  EXPECT_TRUE(FusedAccepts(*fused, "axle"));
  EXPECT_TRUE(FusedAccepts(*fused, "banana"));
  EXPECT_TRUE(FusedAccepts(*fused, "apple"));
  EXPECT_FALSE(FusedAccepts(*fused, "kiwis"));
  EXPECT_FALSE(FusedAccepts(*fused, "cherry"));
}

TEST(OrAcceptorFusion_test, keeps_contiguous_only_by_default) {
  auto root = std::make_unique<irs::BooleanFilter>();
  AddTerm(*root, irs::Occur::Should, kFieldTestField, "kiwi");
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ax");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(OrAcceptorFusion_test, quotes_term_metacharacters) {
  auto root = std::make_unique<irs::BooleanFilter>();
  AddTerm(*root, irs::Occur::Should, kFieldTestField, "a.e");
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "b(");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_seekable_acceptors = true});

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_TRUE(FusedAccepts(*fused, "a.e"));
  EXPECT_FALSE(FusedAccepts(*fused, "axe"));
  EXPECT_TRUE(FusedAccepts(*fused, "b(x"));
  EXPECT_FALSE(FusedAccepts(*fused, "bx"));
}

TEST(OrAcceptorFusion_test, pure_terms_stay_clauses) {
  auto root = std::make_unique<irs::BooleanFilter>();
  AddTerm(*root, irs::Occur::Should, kFieldTestField, "apple");
  AddTerm(*root, irs::Occur::Should, kFieldTestField, "banana");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
  const auto& node = sdb::basics::downCast<irs::BooleanFilter>(*filter);
  ASSERT_EQ(2, node.Terms(irs::Occur::Should).size());
  EXPECT_TRUE(node.Filters(irs::Occur::Should).empty());
  EXPECT_EQ(1, node.MinShouldMatch());
}

TEST(OrAcceptorFusion_test, keeps_min_match_or) {
  auto root = std::make_unique<irs::BooleanFilter>();
  AddTerm(*root, irs::Occur::Should, kFieldTestField, "kiwi");
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ax");
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ban");
  root->SetMinShouldMatch(2);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(OrAcceptorFusion_test, keeps_mixed_fields) {
  auto root = std::make_unique<irs::BooleanFilter>();
  AddTerm(*root, irs::Occur::Should, kFieldTestField, "kiwi");
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldName, "ax");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(OrAcceptorFusion_test, keeps_range_children) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ax");
  {
    auto& range = AddRange(*root, irs::Occur::Should, kFieldTestField);
    range.mutable_options()->range.min = irs::bstring{B("b")};
    range.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    range.mutable_options()->range.max = irs::bstring{B("d")};
    range.mutable_options()->range.max_type = irs::BoundType::Exclusive;
  }
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(OrAcceptorFusion_test, scored_requires_uniform_boosts) {
  {
    auto root = std::make_unique<irs::BooleanFilter>();
    AddTerm(*root, irs::Occur::Should, kFieldTestField, "kiwi", 2.f);
    Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ax")
      .SetBoost(3.f);
    AsDisjunction(*root);

    irs::Filter::ptr filter = std::move(root);
    irs::Optimize(filter, {.scored = true, .fuse_seekable_acceptors = true});

    EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
  }
  {
    auto root = std::make_unique<irs::BooleanFilter>();
    Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ax")
      .SetBoost(2.f);
    Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ban")
      .SetBoost(2.f);
    root->SetBoost(3.f);
    AsDisjunction(*root);

    irs::Filter::ptr filter = std::move(root);
    irs::Optimize(filter, {.scored = true, .fuse_seekable_acceptors = true});

    const auto* fused = FusedOf(filter);
    ASSERT_NE(nullptr, fused);
    EXPECT_EQ(6.f, fused->GetBoost());
  }
}

TEST(OrAcceptorFusion_test, flattens_nested_or_on_search_path) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& inner = AddBool(*root, irs::Occur::Should);
    Append<irs::ByPrefix>(inner, irs::Occur::Should, kFieldTestField, "ax");
    Append<irs::ByPrefix>(inner, irs::Occur::Should, kFieldTestField, "ban");
    AsDisjunction(inner);
  }
  Append<irs::ByWildcard>(*root, irs::Occur::Should, kFieldTestField, "%le");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_TRUE(FusedAccepts(*fused, "axle"));
  EXPECT_TRUE(FusedAccepts(*fused, "banana"));
  EXPECT_TRUE(FusedAccepts(*fused, "apple"));
  EXPECT_FALSE(FusedAccepts(*fused, "kiwi"));
}

TEST(OrAcceptorFusion_test, fuses_nested_fused_or) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& inner = AddBool(*root, irs::Occur::Should);
    Append<irs::ByPrefix>(inner, irs::Occur::Should, kFieldTestField, "ax");
    Append<irs::ByPrefix>(inner, irs::Occur::Should, kFieldTestField, "ban");
    AsDisjunction(inner);
  }
  Append<irs::ByWildcard>(*root, irs::Occur::Should, kFieldTestField, "%le");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_seekable_acceptors = true});

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_TRUE(FusedAccepts(*fused, "axle"));
  EXPECT_TRUE(FusedAccepts(*fused, "banana"));
  EXPECT_TRUE(FusedAccepts(*fused, "apple"));
  EXPECT_FALSE(FusedAccepts(*fused, "kiwi"));
}

TEST(OrAcceptorFusion_test, keeps_unfusable_nested_or) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& inner = AddBool(*root, irs::Occur::Should);
    Append<irs::ByPrefix>(inner, irs::Occur::Should, kFieldTestField, "ax");
    Append<irs::ByPrefix>(inner, irs::Occur::Should, kFieldTestField, "ban");
    inner.SetMinShouldMatch(2);
  }
  Append<irs::ByWildcard>(*root, irs::Occur::Should, kFieldTestField, "%le");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_seekable_acceptors = true});

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(OrAcceptorFusion_test, translates_wildcard_escapes) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByWildcard>(*root, irs::Occur::Should, kFieldTestField, "a_c%");
  Append<irs::ByWildcard>(*root, irs::Occur::Should, kFieldTestField, "x\\%y");
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_TRUE(FusedAccepts(*fused, "abc"));
  EXPECT_TRUE(FusedAccepts(*fused, "abcdef"));
  EXPECT_FALSE(FusedAccepts(*fused, "ac"));
  EXPECT_TRUE(FusedAccepts(*fused, "x%y"));
  EXPECT_FALSE(FusedAccepts(*fused, "xzy"));
}

TEST(OrAcceptorFusion_test, keeps_non_perl_regexp) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByPrefix>(*root, irs::Occur::Should, kFieldTestField, "ax");
  {
    auto& re = AddChild<irs::ByRegexp>(*root, irs::Occur::Should);
    *re.mutable_field_id() = kFieldTestField;
    re.mutable_options()->pattern = irs::bstring{B("a.*e")};
    re.mutable_options()->syntax = irs::RegexpSyntax::PosixEre;
  }
  AsDisjunction(*root);

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_seekable_acceptors = true});

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(AndAcceptorFusion_test, fuses_same_field_acceptors) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByPrefix>(*root, irs::Occur::Must, kFieldTestField, "ax");
  Append<irs::ByWildcard>(*root, irs::Occur::Must, kFieldTestField, "%le");

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_acceptor_intersections = true});

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_EQ(kFieldTestField, fused->field_id());
  EXPECT_EQ(irs::bstring{B("ax%&%le")}, fused->options().pattern);
  EXPECT_TRUE(FusedAccepts(*fused, "axle"));
  EXPECT_TRUE(FusedAccepts(*fused, "axolotle"));
  EXPECT_FALSE(FusedAccepts(*fused, "apple"));
  EXPECT_FALSE(FusedAccepts(*fused, "axis"));
}

TEST(AndAcceptorFusion_test, noop_without_flag) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByPrefix>(*root, irs::Occur::Must, kFieldTestField, "ax");
  Append<irs::ByWildcard>(*root, irs::Occur::Must, kFieldTestField, "%le");

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(AndAcceptorFusion_test, noop_when_scored) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByPrefix>(*root, irs::Occur::Must, kFieldTestField, "ax");
  Append<irs::ByWildcard>(*root, irs::Occur::Must, kFieldTestField, "%le");

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = true, .fuse_acceptor_intersections = true});

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(AndAcceptorFusion_test, renders_range_pattern) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByPrefix>(*root, irs::Occur::Must, kFieldTestField, "b");
  {
    auto& range = AddRange(*root, irs::Occur::Must, kFieldTestField);
    range.mutable_options()->range.min = irs::bstring{B("b")};
    range.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    range.mutable_options()->range.max = irs::bstring{B("d")};
    range.mutable_options()->range.max_type = irs::BoundType::Exclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_acceptor_intersections = true});

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_EQ(irs::bstring{B("b%&[b..d)")}, fused->options().pattern);
  EXPECT_TRUE(FusedAccepts(*fused, "bar"));
  EXPECT_FALSE(FusedAccepts(*fused, "dog"));
  EXPECT_FALSE(FusedAccepts(*fused, "ax"));
}

TEST(AndAcceptorFusion_test, levenshtein_driver_bails) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByEditDistance>(*root, irs::Occur::Must, kFieldTestField, "apple")
    .mutable_options()
    ->max_distance = 1;
  Append<irs::ByWildcard>(*root, irs::Occur::Must, kFieldTestField, "%le");

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_acceptor_intersections = true});

  EXPECT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
}

TEST(AndAcceptorFusion_test, levenshtein_predicate_fuses) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& range = AddRange(*root, irs::Occur::Must, kFieldTestField);
    range.mutable_options()->range.min = irs::bstring{B("a")};
    range.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    range.mutable_options()->range.max = irs::bstring{B("b")};
    range.mutable_options()->range.max_type = irs::BoundType::Exclusive;
  }
  Append<irs::ByEditDistance>(*root, irs::Occur::Must, kFieldTestField, "apple")
    .mutable_options()
    ->max_distance = 1;

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_acceptor_intersections = true});

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_EQ(irs::bstring{B("[a..b)&apple~")}, fused->options().pattern);
  EXPECT_TRUE(FusedAccepts(*fused, "apple"));
  EXPECT_TRUE(FusedAccepts(*fused, "aplle"));
  EXPECT_FALSE(FusedAccepts(*fused, "banana"));
  EXPECT_FALSE(FusedAccepts(*fused, "axxxx"));
}

TEST(AndAcceptorFusion_test, nested_disjunction_driver_bails) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& inner = AddBool(*root, irs::Occur::Must);
    AddTerm(inner, irs::Occur::Should, kFieldTestField, "apple");
    AddTerm(inner, irs::Occur::Should, kFieldTestField, "banana");
    AsDisjunction(inner);
  }
  Append<irs::ByPrefix>(*root, irs::Occur::Must, kFieldTestField, "ap");

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_acceptor_intersections = true});

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
  const auto& node = sdb::basics::downCast<irs::BooleanFilter>(*filter);
  ASSERT_EQ(2, node.Size(irs::Occur::Must));
  const auto filters = node.Filters(irs::Occur::Must);
  ASSERT_EQ(2, filters.size());
  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filters[0]->type());
  const auto& inner = sdb::basics::downCast<irs::BooleanFilter>(*filters[0]);
  EXPECT_EQ(2, inner.Terms(irs::Occur::Should).size());
}

TEST(AndAcceptorFusion_test, keeps_other_field_children) {
  auto root = std::make_unique<irs::BooleanFilter>();
  Append<irs::ByPrefix>(*root, irs::Occur::Must, kFieldTestField, "ax");
  Append<irs::ByWildcard>(*root, irs::Occur::Must, kFieldTestField, "%le");
  Append<irs::ByPrefix>(*root, irs::Occur::Must, kFieldName, "b");

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_acceptor_intersections = true});

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filter->type());
  const auto& node = sdb::basics::downCast<irs::BooleanFilter>(*filter);
  ASSERT_EQ(2, node.Size(irs::Occur::Must));
}

TEST(AndAcceptorFusion_test, fuses_or_product) {
  auto root = std::make_unique<irs::BooleanFilter>();
  {
    auto& inner = AddBool(*root, irs::Occur::Must);
    Append<irs::ByPrefix>(inner, irs::Occur::Should, kFieldTestField, "ax");
    Append<irs::ByWildcard>(inner, irs::Occur::Should, kFieldTestField, "%le");
    AsDisjunction(inner);
  }
  {
    auto& range = AddRange(*root, irs::Occur::Must, kFieldTestField);
    range.mutable_options()->range.min = irs::bstring{B("a")};
    range.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    range.mutable_options()->range.max = irs::bstring{B("b")};
    range.mutable_options()->range.max_type = irs::BoundType::Exclusive;
  }

  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.fuse_acceptor_intersections = true});

  const auto* fused = FusedOf(filter);
  ASSERT_NE(nullptr, fused);
  EXPECT_TRUE(FusedAccepts(*fused, "axle"));
  EXPECT_TRUE(FusedAccepts(*fused, "ale"));
  EXPECT_FALSE(FusedAccepts(*fused, "ble"));
  EXPECT_FALSE(FusedAccepts(*fused, "amp"));
}

class BooleanFilterTestCase : public FilterTestCaseBase {};

TEST_P(BooleanFilterTestCase, or_sequential_multiple_segments) {
  // populate index
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);

    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();
    const tests::Document* doc3 = gen.next();
    const tests::Document* doc4 = gen.next();
    const tests::Document* doc5 = gen.next();
    const tests::Document* doc6 = gen.next();
    const tests::Document* doc7 = gen.next();
    const tests::Document* doc8 = gen.next();
    const tests::Document* doc9 = gen.next();

    auto writer = open_writer();

    ASSERT_TRUE(
      Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));  // A
    ASSERT_TRUE(
      Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));  // B
    ASSERT_TRUE(
      Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));  // C
    ASSERT_TRUE(
      Insert(*writer, doc4->indexed.begin(), doc4->indexed.end()));  // D
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(
      Insert(*writer, doc5->indexed.begin(), doc5->indexed.end()));  // E
    ASSERT_TRUE(
      Insert(*writer, doc6->indexed.begin(), doc6->indexed.end()));  // F
    ASSERT_TRUE(
      Insert(*writer, doc7->indexed.begin(), doc7->indexed.end()));  // G
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(
      Insert(*writer, doc8->indexed.begin(), doc8->indexed.end()));  // H
    ASSERT_TRUE(
      Insert(*writer, doc9->indexed.begin(), doc9->indexed.end()));  // I
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  auto rdr = open_reader();
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "B");
    AddTerm(root, irs::Occur::Should, kFieldName, "F");
    AddTerm(root, irs::Occur::Should, kFieldName, "I");
    AsDisjunction(root);

    tests::PreparedFilter prep{root, rdr};
    {
      auto docs = prep.Execute(0);
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(2, docs->Value());
      ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    }

    {
      auto docs = prep.Execute(1);
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(2, docs->Value());
      ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    }

    {
      auto docs = prep.Execute(2);
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(2, docs->Value());
      ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    }
  }
}

TEST_P(BooleanFilterTestCase, or_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "V");  // 22
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{22}, rdr);
  }

  // name=W OR name=Z
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "W");  // 23
    AddTerm(root, irs::Occur::Should, kFieldName, "C");  // 3
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{3, 23}, rdr);
  }

  // name=A OR name=Q OR name=Z
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");  // 1
    AddTerm(root, irs::Occur::Should, kFieldName, "Q");  // 17
    AddTerm(root, irs::Occur::Should, kFieldName, "Z");  // 26
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 17, 26}, rdr);
  }

  // name=A OR name=Q OR same!=xyz
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");  // 1
    AddTerm(root, irs::Occur::Should, kFieldName, "Q");  // 17
    {
      auto& sub = AddBool(root, irs::Occur::Should);
      AddTerm(sub, irs::Occur::MustNot, kFieldSame, "xyz");
    }
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 17}, rdr);
  }

  // (name=A OR name=Q) OR same!=xyz
  {
    irs::BooleanFilter root;
    {
      auto& sub = AddBool(root, irs::Occur::Should);
      AddTerm(sub, irs::Occur::Should, kFieldName, "A");  // 1
      AddTerm(sub, irs::Occur::Should, kFieldName, "Q");  // 17
      AsDisjunction(sub);
    }
    {
      auto& sub = AddBool(root, irs::Occur::Should);
      AddTerm(sub, irs::Occur::MustNot, kFieldSame, "xyz");
    }
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 17}, rdr);
  }

  // name=A OR name=Q OR name=Z OR same=invalid_term OR invalid_field=V
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");  // 1
    AddTerm(root, irs::Occur::Should, kFieldName, "Q");  // 17
    AddTerm(root, irs::Occur::Should, kFieldName, "Z");  // 26
    AddTerm(root, irs::Occur::Should, kFieldSame, "invalid_term");
    AddTerm(root, irs::Occur::Should, kFieldInvalid, "V");
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 17, 26}, rdr);
  }

  // search : all terms
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");    // 1
    AddTerm(root, irs::Occur::Should, kFieldName, "Q");    // 17
    AddTerm(root, irs::Occur::Should, kFieldName, "Z");    // 26
    AddTerm(root, irs::Occur::Should, kFieldSame, "xyz");  // 1..32
    AddTerm(root, irs::Occur::Should, kFieldSame, "invalid_term");
    AsDisjunction(root);

    CheckQuery(
      *tests::Optimized(std::move(root)),
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");    // 1
    AddTerm(root, irs::Occur::Should, kFieldName, "Q");    // 17
    AddTerm(root, irs::Occur::Should, kFieldName, "Z");    // 26
    AddTerm(root, irs::Occur::Should, kFieldSame, "xyz");  // 1..32
    AddTerm(root, irs::Occur::Should, kFieldSame, "invalid_term");
    root.SetMinShouldMatch(
      static_cast<uint32_t>(root.Size(irs::Occur::Should)));

    CheckQuery(*tests::Optimized(std::move(root)), Docs{}, rdr);
  }

  // name=A OR false
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");  // 1
    AddChild<irs::Empty>(root, irs::Occur::Should);
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{1}, rdr);
  }

  // A sub-node holding nothing but a negation has no include side, so it
  // matches nothing and the disjunction is left with only `false`.
  {
    irs::BooleanFilter root;
    {
      auto& sub = AddBool(root, irs::Occur::Should);
      AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");  // 1
    }
    AddChild<irs::Empty>(root, irs::Occur::Should);
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{}, rdr);
  }

  // The same, said the way SQL says it: an explicit include side makes the
  // sub-node the complement of `name=A`.
  {
    irs::BooleanFilter root;
    {
      auto& sub = AddBool(root, irs::Occur::Should);
      AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");  // 1
      AsComplement(sub);
    }
    AddChild<irs::Empty>(root, irs::Occur::Should);
    AsDisjunction(root);

    CheckQuery(
      *tests::Optimized(std::move(root)),
      Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
           18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // Not with impossible name!=A OR same="NOT POSSIBLE"
  {
    irs::BooleanFilter root;
    {
      auto& sub = AddBool(root, irs::Occur::Should);
      AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");  // 1
    }
    AddTerm(root, irs::Occur::Should, kFieldSame, "NOT POSSIBLE");
    AsDisjunction(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{}, rdr);
  }

  // optimization should adjust min_match
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");
    AddChild<irs::All>(root, irs::Occur::Should);
    AddChild<irs::All>(root, irs::Occur::Should);
    AddChild<irs::All>(root, irs::Occur::Should);
    AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
    root.SetMinShouldMatch(5);
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1}, rdr);
  }

  // optimization should adjust min_match same but with score to check scored
  // optimization
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");
    AddChild<irs::All>(root, irs::Occur::Should);
    AddChild<irs::All>(root, irs::Occur::Should);
    AddChild<irs::All>(root, irs::Occur::Should);
    AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
    root.SetMinShouldMatch(5);
    irs::Scorer::ptr sort{std::make_unique<sort::CustomSort>()};
    CheckQuery(*tests::Optimized(std::move(root)), std::span{&sort, 1}, Docs{1},
               rdr);
  }

  // optimization should adjust min_match
  // case where it should be dropped to 1
  // as optimized more filters than min_match
  // unscored
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");
    for (size_t i = 0; i != 8; ++i) {
      AddChild<irs::All>(root, irs::Occur::Should);
    }
    AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
    root.SetMinShouldMatch(3);
    CheckQuery(
      *tests::Optimized(std::move(root)),
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // scored
  {
    irs::BooleanFilter root;
    root.SetMergeType(irs::ScoreMergeType::Max);
    AddTerm(root, irs::Occur::Should, kFieldName, "A");
    for (size_t i = 0; i != 8; ++i) {
      AddChild<irs::All>(root, irs::Occur::Should);
    }
    AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
    root.SetMinShouldMatch(3);
    irs::Scorer::ptr sort{std::make_unique<sort::CustomSort>()};

    Docs expected{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                  12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                  23, 24, 25, 26, 27, 28, 29, 30, 31, 32};

    CheckQuery(*tests::Optimized(std::move(root)), std::span{&sort, 1},
               expected, rdr);
  }
}

TEST_P(BooleanFilterTestCase, and_schemas) {
  // write segments
  {
    auto writer = open_writer(irs::kOmCreate);

    std::vector<DocGeneratorBase::ptr> gens;

    gens.emplace_back(new tests::JsonDocGenerator(
      resource("AdventureWorks2014.json"), &tests::GenericJsonFieldFactory));
    gens.emplace_back(
      new tests::JsonDocGenerator(resource("AdventureWorks2014Edges.json"),
                                  &tests::GenericJsonFieldFactory));
    gens.emplace_back(new tests::JsonDocGenerator(
      resource("Northwnd.json"), &tests::GenericJsonFieldFactory));
    gens.emplace_back(new tests::JsonDocGenerator(
      resource("NorthwndEdges.json"), &tests::GenericJsonFieldFactory));

    add_segments(*writer, gens);
  }

  auto rdr = open_reader();

  // Name = Product AND source=AdventureWor3ks2014
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldNameUpper, "Product");
    AddTerm(root, irs::Occur::Must, kFieldSource, "AdventureWor3ks2014");
    CheckQuery(*tests::Optimized(std::move(root)), Docs{}, rdr);
  }
}

TEST_P(BooleanFilterTestCase, and_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // name=V
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldName, "V");  // 22

    CheckQuery(*tests::Optimized(std::move(root)), Docs{22}, rdr);
  }

  // duplicated=abcd AND same=xyz
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated,
            "abcd");                                     // 1,5,11,21,27,31
    AddTerm(root, irs::Occur::Must, kFieldSame, "xyz");  // 1..32
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 5, 11, 21, 27, 31},
               rdr);
  }

  // duplicated=abcd AND same=xyz AND name=A
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    AddTerm(root, irs::Occur::Must, kFieldSame, "xyz");
    AddTerm(root, irs::Occur::Must, kFieldName, "A");  // 1
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1}, rdr);
  }

  // duplicated=abcd AND same=xyz AND name=B
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    AddTerm(root, irs::Occur::Must, kFieldSame, "xyz");
    AddTerm(root, irs::Occur::Must, kFieldName, "B");  // 2
    CheckQuery(*tests::Optimized(std::move(root)), Docs{}, rdr);
  }
}

TEST_P(BooleanFilterTestCase, not_standalone_sequential_ordered) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Lucene `-duplicated=abcd`: nothing included, so nothing to exclude from
  {
    irs::BooleanFilter not_node;
    AddTerm(not_node, irs::Occur::MustNot, kFieldDuplicated, "abcd");

    auto optimized = tests::Optimized(std::move(not_node));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT duplicated=abcd`: the complement, in reverse order
  {
    const auto column_name = kFieldDuplicated;

    std::vector<irs::doc_id_t> expected = {2,  3,  4,  6,  7,  8,  9,  10, 12,
                                           13, 14, 15, 16, 17, 18, 19, 20, 22,
                                           23, 24, 25, 26, 28, 29, 30, 32};

    irs::BooleanFilter not_node;
    AddTerm(not_node, irs::Occur::MustNot, column_name, "abcd");
    AsComplement(not_node);

    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;
    irs::doc_id_t cur_doc = 0;

    sort::CustomSort sort;

    sort.collectors_collect = [&collector_finish_count](
                                irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // negated branch must not feed field/term collectors
      ASSERT_EQ(nullptr, field);
      ASSERT_EQ(nullptr, term);
    };
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_EQ(1, n);
      ++scorer_score_count;
      *score = cur_doc;
    };

    tests::PreparedFilter prepared_filter{
      *tests::Optimized(std::move(not_node), &sort), *rdr, &sort};
    std::multimap<irs::score_t, irs::doc_id_t, std::greater<>> scored_result;

    ASSERT_EQ(1, rdr->size());

    ASSERT_EQ(26, prepared_filter.Estimate(0));

    irs::ColumnArgsFetcher fetcher;
    auto filter_itr = prepared_filter.ExecuteScored(0, fetcher);

    auto score = filter_itr->PrepareScore();

    size_t docs_count = 0;

    while (!irs::doc_limits::eof(filter_itr->Advance())) {
      cur_doc = filter_itr->Value();
      filter_itr->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);
      scored_result.emplace(score_value, filter_itr->Value());
      ++docs_count;
    }

    ASSERT_EQ(expected.size(), docs_count);

    ASSERT_EQ(1, collector_finish_count);
    ASSERT_EQ(1, scorer_score_count);

    std::vector<irs::doc_id_t> actual;

    for (auto& entry : scored_result) {
      actual.emplace_back(entry.second);
    }

    ASSERT_EQ(expected, actual);
  }
}

TEST_P(BooleanFilterTestCase, not_sequential_ordered) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Lucene `+(-duplicated=abcd)`: the required child includes nothing
  {
    irs::BooleanFilter root;
    {
      auto& sub = AddBool(root, irs::Occur::Must);
      AddTerm(sub, irs::Occur::MustNot, kFieldDuplicated, "abcd");
    }

    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT duplicated=abcd` nested under a conjunction, in reverse order
  {
    const auto column_name = kFieldDuplicated;

    std::vector<irs::doc_id_t> expected = {2,  3,  4,  6,  7,  8,  9,  10, 12,
                                           13, 14, 15, 16, 17, 18, 19, 20, 22,
                                           23, 24, 25, 26, 28, 29, 30, 32};

    irs::BooleanFilter root;
    {
      auto& sub = AddBool(root, irs::Occur::Must);
      AddTerm(sub, irs::Occur::MustNot, column_name, "abcd");
      AsComplement(sub);
    }

    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;
    irs::doc_id_t cur_doc = 0;

    sort::CustomSort sort;

    sort.collectors_collect = [&collector_finish_count](
                                irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // negated branch must not feed field/term collectors
      ASSERT_EQ(nullptr, field);
      ASSERT_EQ(nullptr, term);
    };
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_EQ(1, n);
      ++scorer_score_count;
      *score = cur_doc;
    };

    tests::PreparedFilter prepared_filter{
      *tests::Optimized(std::move(root), &sort), *rdr, &sort};
    std::multimap<irs::score_t, irs::doc_id_t, std::greater<>> scored_result;

    ASSERT_EQ(1, rdr->size());

    ASSERT_EQ(26, prepared_filter.Estimate(0));

    irs::ColumnArgsFetcher fetcher;
    auto filter_itr = prepared_filter.ExecuteScored(0, fetcher);

    auto score = filter_itr->PrepareScore();

    size_t docs_count = 0;

    while (!irs::doc_limits::eof(filter_itr->Advance())) {
      cur_doc = filter_itr->Value();
      filter_itr->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);
      scored_result.emplace(score_value, filter_itr->Value());
      ++docs_count;
    }

    ASSERT_EQ(expected.size(), docs_count);

    ASSERT_EQ(1, collector_finish_count);
    ASSERT_EQ(1, scorer_score_count);

    std::vector<irs::doc_id_t> actual;

    for (auto& entry : scored_result) {
      actual.emplace_back(entry.second);
    }

    ASSERT_EQ(expected, actual);
  }
}

TEST_P(BooleanFilterTestCase, not_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Lucene `-same=xyz`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::MustNot, kFieldSame, "xyz");

    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT same=xyz`: the term spans the segment
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::MustNot, kFieldSame, "xyz");
    AsComplement(root);

    CheckQuery(*tests::Optimized(std::move(root)), Docs{}, rdr);
  }

  // Lucene `+duplicated=abcd +(-(-name=A))`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    {
      auto& outer = AddBool(root, irs::Occur::Must);
      auto& inner = AddBool(outer, irs::Occur::MustNot);
      AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");
    }

    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `duplicated=abcd AND (NOT (NOT name=A))`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    {
      auto& outer = AddBool(root, irs::Occur::Must);
      auto& inner = AddBool(outer, irs::Occur::MustNot);
      AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");
      AsComplement(inner);
      AsComplement(outer);
    }
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1}, rdr);
  }

  // Lucene `+duplicated=abcd +(-(-(-(-(-name=A)))))`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    {
      auto* node = &AddBool(root, irs::Occur::Must);
      for (size_t i = 0; i != 4; ++i) {
        node = &AddBool(*node, irs::Occur::MustNot);
      }
      AddTerm(*node, irs::Occur::MustNot, kFieldName, "A");
    }

    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `duplicated=abcd AND (NOT (NOT (NOT (NOT (NOT name=A)))))`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    {
      auto* node = &AddBool(root, irs::Occur::Must);
      for (size_t i = 0; i != 4; ++i) {
        auto& next = AddBool(*node, irs::Occur::MustNot);
        AsComplement(*node);
        node = &next;
      }
      AddTerm(*node, irs::Occur::MustNot, kFieldName, "A");
      AsComplement(*node);
    }
    CheckQuery(*tests::Optimized(std::move(root)), Docs{5, 11, 21, 27, 31},
               rdr);
  }

  // * AND NOT *
  {
    {
      irs::BooleanFilter root;
      AddChild<irs::All>(root, irs::Occur::Must);
      AddChild<irs::All>(root, irs::Occur::MustNot);
      CheckQuery(*tests::Optimized(std::move(root)), Docs{}, rdr);
    }

    {
      irs::BooleanFilter root;
      AddChild<irs::All>(root, irs::Occur::Should);
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddChild<irs::All>(sub, irs::Occur::MustNot);
      }
      AsDisjunction(root);
      CheckQuery(
        *tests::Optimized(std::move(root)),
        Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
        rdr);
    }
  }

  // duplicated=abcd AND NOT name=A
  {
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
      AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
      CheckQuery(*tests::Optimized(std::move(root)), Docs{5, 11, 21, 27, 31},
                 rdr);
    }

    // Lucene `duplicated=abcd (-name=A)`: the optional negation is nothing,
    // so only the term can match
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
      }
      AsDisjunction(root);
      CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 5, 11, 21, 27, 31},
                 rdr);
    }

    // SQL `duplicated=abcd OR NOT name=A`
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
        AsComplement(sub);
      }
      AsDisjunction(root);
      CheckQuery(
        *tests::Optimized(std::move(root)),
        Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
        rdr);
    }
  }

  // duplicated=abcd AND NOT name=A AND NOT name=A
  {
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
      AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
      AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
      ASSERT_EQ(1, root.Size(irs::Occur::MustNot));
      CheckQuery(*tests::Optimized(std::move(root)), Docs{5, 11, 21, 27, 31},
                 rdr);
    }

    // Lucene: both optional negations are nothing
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
      }
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
      }
      AsDisjunction(root);
      CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 5, 11, 21, 27, 31},
                 rdr);
    }

    // SQL `duplicated=abcd OR NOT name=A OR NOT name=A`
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
        AsComplement(sub);
      }
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
        AsComplement(sub);
      }
      AsDisjunction(root);
      CheckQuery(
        *tests::Optimized(std::move(root)),
        Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
        rdr);
    }
  }

  // duplicated=abcd AND NOT name=A AND NOT name=E
  {
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
      AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
      AddTerm(root, irs::Occur::MustNot, kFieldName, "E");
      CheckQuery(*tests::Optimized(std::move(root)), Docs{11, 21, 27, 31}, rdr);
    }

    // Lucene: both optional negations are nothing
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
      }
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldPrefix, "abcd");
      }
      AsDisjunction(root);
      CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 5, 11, 21, 27, 31},
                 rdr);
    }

    // SQL `duplicated=abcd OR NOT name=A OR NOT prefix=abcd`
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Should, kFieldDuplicated, "abcd");
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldName, "A");
        AsComplement(sub);
      }
      {
        auto& sub = AddBool(root, irs::Occur::Should);
        AddTerm(sub, irs::Occur::MustNot, kFieldPrefix, "abcd");
        AsComplement(sub);
      }
      AsDisjunction(root);
      CheckQuery(
        *tests::Optimized(std::move(root)),
        Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
        rdr);
    }
  }
}

TEST_P(BooleanFilterTestCase, not_and_conjunction_regression) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  // Lucene `-(+same=xyz +duplicated=abcd)`: nothing to exclude from
  {
    irs::BooleanFilter root;
    {
      auto& conj = AddBool(root, irs::Occur::MustNot);
      AddTerm(conj, irs::Occur::Must, kFieldSame, "xyz");
      AddTerm(conj, irs::Occur::Must, kFieldDuplicated, "abcd");
    }

    auto empty = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), empty->type());
    CheckQuery(*empty, Docs{}, rdr);
  }

  // SQL `NOT (same=xyz AND duplicated=abcd)`
  irs::BooleanFilter root;
  {
    auto& conj = AddBool(root, irs::Occur::MustNot);
    AddTerm(conj, irs::Occur::Must, kFieldSame, "xyz");
    AddTerm(conj, irs::Occur::Must, kFieldDuplicated, "abcd");
  }
  AsComplement(root);

  irs::Filter::ptr optimized = tests::Optimized(std::move(root));
  CheckQuery(*optimized,
             Docs{2,  3,  4,  6,  7,  8,  9,  10, 12, 13, 14, 15, 16,
                  17, 18, 19, 20, 22, 23, 24, 25, 26, 28, 29, 30, 32},
             rdr);

  tests::PreparedFilter prepared{*optimized, rdr};
  for (size_t i = 0, n = prepared.size(); i < n; ++i) {
    auto docs = prepared.Execute(i);
    for (irs::doc_id_t target : {2, 5, 6, 11, 12, 21, 22, 27, 28, 31, 32}) {
      const auto landed = docs->Seek(target);
      EXPECT_GE(landed, target);
      if (irs::doc_limits::eof(landed)) {
        break;
      }
    }
  }
}

TEST_P(BooleanFilterTestCase, not_standalone_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Lucene `-same=xyz`: nothing, because nothing is included
  {
    irs::BooleanFilter not_node;
    AddTerm(not_node, irs::Occur::MustNot, kFieldSame, "xyz");

    auto optimized = tests::Optimized(std::move(not_node));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT same=xyz`: the term spans the segment, so the complement is
  // empty for a reason of its own
  {
    irs::BooleanFilter not_node;
    AddTerm(not_node, irs::Occur::MustNot, kFieldSame, "xyz");
    AsComplement(not_node);

    CheckQuery(*tests::Optimized(std::move(not_node)), Docs{}, rdr);
  }

  // Lucene `-same=invalid_term`: still nothing, even though the excluded
  // term matches no document
  {
    irs::BooleanFilter not_node;
    AddTerm(not_node, irs::Occur::MustNot, kFieldSame, "invalid_term");

    auto optimized = tests::Optimized(std::move(not_node));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT same=invalid_term`: all docs
  {
    irs::BooleanFilter not_node;
    AddTerm(not_node, irs::Occur::MustNot, kFieldSame, "invalid_term");
    AsComplement(not_node);

    CheckQuery(
      *tests::Optimized(std::move(not_node)),
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // Lucene `-(-name=A)`: the inner negation is already nothing
  {
    irs::BooleanFilter not_node;
    auto& inner = AddBool(not_node, irs::Occur::MustNot);
    AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");

    auto optimized = tests::Optimized(std::move(not_node));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT NOT name=A`: the two complements cancel
  {
    irs::BooleanFilter not_node;
    auto& inner = AddBool(not_node, irs::Occur::MustNot);
    AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");
    AsComplement(inner);
    AsComplement(not_node);

    CheckQuery(*tests::Optimized(std::move(not_node)), Docs{1}, rdr);
  }

  // Lucene `-(-(-(-(-name=A))))`
  {
    irs::BooleanFilter not_node;
    auto* node = &not_node;
    for (size_t i = 0; i != 4; ++i) {
      node = &AddBool(*node, irs::Occur::MustNot);
    }
    AddTerm(*node, irs::Occur::MustNot, kFieldName, "A");

    auto optimized = tests::Optimized(std::move(not_node));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT NOT NOT NOT NOT name=A`: an odd number of complements
  {
    irs::BooleanFilter not_node;
    auto* node = &not_node;
    for (size_t i = 0; i != 4; ++i) {
      auto& next = AddBool(*node, irs::Occur::MustNot);
      AsComplement(*node);
      node = &next;
    }
    AddTerm(*node, irs::Occur::MustNot, kFieldName, "A");
    AsComplement(*node);

    CheckQuery(
      *tests::Optimized(std::move(not_node)),
      Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
           18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }
}

TEST_P(BooleanFilterTestCase, lowered_wildcard_dedups_against_a_literal_term) {
  // `fox` spelled as a wildcard lowers to the term it already is, and the
  // lowering runs after the rule pass -- so the fold has to happen there or
  // the bucket carries the same posting twice.
  irs::BooleanFilter root;
  AddTerm(root, irs::Occur::Should, kFieldName, "fox");
  auto wildcard = std::make_unique<irs::ByWildcard>();
  *wildcard->mutable_field_id() = kFieldName;
  wildcard->mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  root.Add(std::move(wildcard), irs::Occur::Should);
  root.SetMinShouldMatch(1);

  // One posting, so the node is that term rather than a disjunction of it
  // with itself.
  auto optimized = tests::Optimized(std::move(root));
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), optimized->type());
  const auto& term = sdb::basics::downCast<irs::ByTerm>(*optimized);
  EXPECT_EQ(kFieldName, term.field_id());
  EXPECT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"fox"}),
            irs::bytes_view{term.options().term});
}

TEST_P(BooleanFilterTestCase, exclusion_all_docs_in_optional_bucket) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // The optional bucket's threshold is met by the all-docs clause alone, so
  // the node includes every document and excludes `A`.
  irs::BooleanFilter root;
  AddChild<irs::All>(root, irs::Occur::Should);
  AddTerm(root, irs::Occur::Should, kFieldName, "B");
  AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
  root.SetMinShouldMatch(1);

  const Docs expected{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                      13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                      24, 25, 26, 27, 28, 29, 30, 31, 32};
  CheckQuery(*tests::Optimized(std::move(root)), expected, rdr);

  // Scored, the all-docs clause is kept rather than erased, so the same node
  // reaches the query layer with it still in the optional bucket.
  irs::Scorer::ptr impl{std::make_unique<tests::sort::CustomSort>()};
  irs::BooleanFilter scored;
  AddChild<irs::All>(scored, irs::Occur::Should);
  AddTerm(scored, irs::Occur::Should, kFieldName, "B");
  AddTerm(scored, irs::Occur::MustNot, kFieldName, "A");
  scored.SetMinShouldMatch(1);
  CheckQuery(*tests::Optimized(std::move(scored), impl.get()), expected, rdr);
}

TEST_P(BooleanFilterTestCase, exclusion_sequential) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Lucene `-name=A`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::MustNot, kFieldName, "A");

    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT name=A`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
    AsComplement(root);
    CheckQuery(
      *tests::Optimized(std::move(root)),
      Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
           18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 5, 11, 21, 27, 31},
               rdr);
  }

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
    CheckQuery(*tests::Optimized(std::move(root)), Docs{5, 11, 21, 27, 31},
               rdr);
  }

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
    AddTerm(root, irs::Occur::MustNot, kFieldName, "E");
    CheckQuery(*tests::Optimized(std::move(root)), Docs{11, 21, 27, 31}, rdr);
  }

  // Lucene `-(-name=A)`
  {
    irs::BooleanFilter root;
    auto& inner = AddBool(root, irs::Occur::MustNot);
    AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");

    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::Empty>::id(), optimized->type());
    CheckQuery(*optimized, Docs{}, rdr);
  }

  // SQL `NOT NOT name=A`
  {
    irs::BooleanFilter root;
    auto& inner = AddBool(root, irs::Occur::MustNot);
    AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");
    AsComplement(inner);
    AsComplement(root);
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1}, rdr);
  }

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldSame, "xyz");
    AddTerm(root, irs::Occur::MustNot, kFieldName, "A");
    CheckQuery(
      *tests::Optimized(std::move(root)),
      Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
           18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // Lucene `+duplicated=abcd -(-name=A)`: the excluded child is nothing, so
  // it removes nothing
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    auto& inner = AddBool(root, irs::Occur::MustNot);
    AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1, 5, 11, 21, 27, 31},
               rdr);
  }

  // SQL `duplicated=abcd AND NOT NOT name=A`
  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldDuplicated, "abcd");
    auto& inner = AddBool(root, irs::Occur::MustNot);
    AddTerm(inner, irs::Occur::MustNot, kFieldName, "A");
    AsComplement(inner);
    CheckQuery(*tests::Optimized(std::move(root)), Docs{1}, rdr);
  }
}

TEST_P(BooleanFilterTestCase, mixed) {
  {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc)
    {
      irs::BooleanFilter root;

      // same=xyz AND duplicated=abcd
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddTerm(child, irs::Occur::Must, kFieldSame, "xyz");
        AddTerm(child, irs::Occur::Must, kFieldDuplicated, "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddTerm(child, irs::Occur::Must, kFieldSame, "xyz");
        AddTerm(child, irs::Occur::Must, kFieldDuplicated, "vczc");
      }
      AsDisjunction(root);

      CheckQuery(*tests::Optimized(std::move(root)),
                 Docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31}, rdr);
    }

    // ((same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc)) AND
    // name=X
    {
      irs::BooleanFilter root;
      AddTerm(root, irs::Occur::Must, kFieldName, "X");

      // ( same = xyz AND duplicated = abcd ) OR( same = xyz AND duplicated =
      // vczc )
      {
        auto& child = AddBool(root, irs::Occur::Must);

        // same=xyz AND duplicated=abcd
        {
          auto& subchild = AddBool(child, irs::Occur::Should);
          AddTerm(subchild, irs::Occur::Must, kFieldSame, "xyz");
          AddTerm(subchild, irs::Occur::Must, kFieldDuplicated, "abcd");
        }

        // same=xyz AND duplicated=vczc
        {
          auto& subchild = AddBool(child, irs::Occur::Should);
          AddTerm(subchild, irs::Occur::Must, kFieldSame, "xyz");
          AddTerm(subchild, irs::Occur::Must, kFieldDuplicated, "vczc");
        }
        AsDisjunction(child);
      }

      CheckQuery(*tests::Optimized(std::move(root)), Docs{24}, rdr);
    }

    // ((same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or
    // name=U or name=X)) OR (same=xyz AND (duplicated=vczc OR (name=A OR
    // name=C OR NAME=P OR name=U OR name=X)) ) 1, 2, 3, 4, 5, 8, 11, 14, 16,
    // 17, 19, 21, 24, 27, 31
    {
      irs::BooleanFilter root;

      // (same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or
      // name=U or name=X) 1, 3, 5,11, 16, 21, 24, 27, 31
      {
        auto& child = AddBool(root, irs::Occur::Should);

        // ( same = xyz AND duplicated = abcd )
        {
          auto& subchild = AddBool(root, irs::Occur::Should);
          AddTerm(subchild, irs::Occur::Must, kFieldSame, "xyz");
          AddTerm(subchild, irs::Occur::Must, kFieldDuplicated, "abcd");
        }

        AddTerm(child, irs::Occur::Should, kFieldName, "A");
        AddTerm(child, irs::Occur::Should, kFieldName, "C");
        AddTerm(child, irs::Occur::Should, kFieldName, "P");
        AddTerm(child, irs::Occur::Should, kFieldName, "X");
        AsDisjunction(child);
      }

      // (same=xyz AND (duplicated=vczc OR (name=A OR name=C OR NAME=P OR
      // name=U OR name=X)) 1, 2, 3, 8, 14, 16, 17, 19, 21, 24
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddTerm(child, irs::Occur::Must, kFieldSame, "xyz");

        // (duplicated=vczc OR (name=A OR name=C OR NAME=P OR name=U OR
        // name=X)
        {
          auto& subchild = AddBool(child, irs::Occur::Must);
          AddTerm(subchild, irs::Occur::Should, kFieldDuplicated, "vczc");

          // name=A OR name=C OR NAME=P OR name=U OR name=X
          {
            auto& subsubchild = AddBool(subchild, irs::Occur::Should);
            AddTerm(subsubchild, irs::Occur::Should, kFieldName, "A");
            AddTerm(subsubchild, irs::Occur::Should, kFieldName, "C");
            AddTerm(subsubchild, irs::Occur::Should, kFieldName, "P");
            AddTerm(subsubchild, irs::Occur::Should, kFieldName, "X");
            AsDisjunction(subsubchild);
          }
          AsDisjunction(subchild);
        }
      }
      AsDisjunction(root);

      CheckQuery(*tests::Optimized(std::move(root)),
                 Docs{1, 2, 3, 5, 8, 11, 14, 16, 17, 19, 21, 24, 27, 31}, rdr);
    }

    {
      irs::BooleanFilter root;

      // *
      AddChild<irs::All>(root, irs::Occur::Should);

      // same=xyz AND duplicated=abcd
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddTerm(child, irs::Occur::Must, kFieldSame, "xyz");
        AddTerm(child, irs::Occur::Must, kFieldDuplicated, "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddTerm(child, irs::Occur::Must, kFieldSame, "xyz");
        AddTerm(child, irs::Occur::Must, kFieldDuplicated, "vczc");
      }
      AsDisjunction(root);

      CheckQuery(
        *tests::Optimized(std::move(root)),
        Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
        rdr);
    }

    // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc) OR NOT
    // *
    {
      irs::BooleanFilter root;

      // NOT *
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddChild<irs::All>(child, irs::Occur::MustNot);
      }

      // same=xyz AND duplicated=abcd
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddTerm(child, irs::Occur::Must, kFieldSame, "xyz");
        AddTerm(child, irs::Occur::Must, kFieldDuplicated, "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        auto& child = AddBool(root, irs::Occur::Should);
        AddTerm(child, irs::Occur::Must, kFieldSame, "xyz");
        AddTerm(child, irs::Occur::Must, kFieldDuplicated, "vczc");
      }
      AsDisjunction(root);

      CheckQuery(*tests::Optimized(std::move(root)),
                 Docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31}, rdr);
    }
  }
}

TEST_P(BooleanFilterTestCase, mixed_ordered) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();
  ASSERT_TRUE(bool(rdr));

  {
    irs::BooleanFilter root;
    auto& sub = AddBool(root, irs::Occur::Should);
    {
      auto& filter = AddChild<irs::ByRange>(sub, irs::Occur::Must);
      *filter.mutable_field_id() = kFieldName;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("!"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    }
    {
      auto& filter = AddChild<irs::ByRange>(sub, irs::Occur::Must);
      *filter.mutable_field_id() = kFieldName;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("~"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    }
    AsDisjunction(root);

    irs::TFIDF tfidf_scorer;

    tests::PreparedFilter prepared{
      *tests::Optimized(std::move(root), &tfidf_scorer), *rdr, &tfidf_scorer};
    ASSERT_NE(nullptr, prepared.Query(0));

    std::vector<irs::doc_id_t> expected_docs{
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
      16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32};

    auto expected_doc = expected_docs.begin();
    for (size_t i = 0, n = prepared.size(); i < n; ++i) {
      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(i, fetcher);

      const auto scr = docs->PrepareScore();

      std::vector<irs::bstring> scores;
      while (!irs::doc_limits::eof(docs->Advance())) {
        EXPECT_EQ(*expected_doc, docs->Value());
        ++expected_doc;

        docs->FetchScoreArgs(0);
        irs::bstring score_value(sizeof(irs::score_t), 0);
        *reinterpret_cast<irs::score_t*>(score_value.data()) = scr.Score();
        scores.emplace_back(std::move(score_value));
      }

      ASSERT_EQ(expected_docs.end(), expected_doc);
      ASSERT_TRUE(irs::irstd::AllEqual(scores.begin(), scores.end()));
    }
  }
}

TEST_P(BooleanFilterTestCase, and_or_no_collector) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  const auto collect = [&](const irs::Filter& filter,
                           PreparedFilter::CollectMode mode) {
    PreparedFilter prepared{filter,  rdr, nullptr, irs::IResourceManager::gNoop,
                            nullptr, mode};
    Docs docs;
    for (size_t i = 0, n = prepared.size(); i < n; ++i) {
      auto it = prepared.Execute(i);
      while (!irs::doc_limits::eof(it->Advance())) {
        docs.push_back(it->Value());
      }
    }
    return docs;
  };

  const auto check = [&](const irs::Filter& filter) {
    const auto with_collector =
      collect(filter, PreparedFilter::CollectMode::Single);
    const auto without_collector =
      collect(filter, PreparedFilter::CollectMode::NoCollector);
    ASSERT_FALSE(without_collector.empty());
    ASSERT_EQ(with_collector, without_collector);
  };

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Must, kFieldSame, "xyz");
    AddTerm(root, irs::Occur::Must, kFieldName, "A");
    check(root);
  }

  {
    irs::BooleanFilter root;
    AddTerm(root, irs::Occur::Should, kFieldName, "A");
    AddTerm(root, irs::Occur::Should, kFieldName, "B");
    AsDisjunction(root);
    check(root);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(boolean_filter_test, BooleanFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         BooleanFilterTestCase::to_string);

}  // namespace tests
