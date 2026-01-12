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

#pragma once

#include <basics/singleton.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/cost.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/filter_visitor.hpp>
#include <iresearch/search/score.hpp>
#include <iresearch/search/tfidf.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <variant>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace tests {
namespace sort {

struct Boost : public irs::ScorerBase<Boost, void> {
  struct ScoreCtx final : public irs::ScoreCtx {
   public:
    explicit ScoreCtx(irs::score_t boost) noexcept : boost(boost) {}

    irs::score_t boost;
  };

  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ColumnProvider&,
                                   const irs::FieldProperties& /*features*/,
                                   const irs::byte_type* /*query_attrs*/,
                                   const irs::AttributeProvider& /*doc_attrs*/,
                                   irs::score_t boost) const final {
    return irs::ScoreFunction::Make<Boost::ScoreCtx>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        const auto& state = *reinterpret_cast<ScoreCtx*>(ctx);

        *res = state.boost;
      },
      irs::ScoreFunction::DefaultMin, boost);
  }
};

struct CustomSort : public irs::ScorerBase<CustomSort, void> {
  class FieldCollector : public irs::FieldCollector {
   public:
    FieldCollector(const CustomSort& sort) : _sort(sort) {}

    void collect(const irs::SubReader& segment,
                 const irs::TermReader& field) final {
      if (_sort.collector_collect_field) {
        _sort.collector_collect_field(segment, field);
      }
    }

    void reset() final {
      if (_sort.field_reset) {
        _sort.field_reset();
      }
    }

    void collect(irs::bytes_view /*in*/) final {
      // NOOP
    }

    void write(irs::DataOutput& /*out*/) const final {
      // NOOP
    }

   private:
    const CustomSort& _sort;
  };

  class TermCollector : public irs::TermCollector {
   public:
    TermCollector(const CustomSort& sort) : _sort(sort) {}

    void collect(const irs::SubReader& segment, const irs::TermReader& field,
                 const irs::AttributeProvider& term_attrs) final {
      if (_sort.collector_collect_term) {
        _sort.collector_collect_term(segment, field, term_attrs);
      }
    }

    void reset() final {
      if (_sort.term_reset) {
        _sort.term_reset();
      }
    }

    void collect(irs::bytes_view /*in*/) final {
      // NOOP
    }

    void write(irs::DataOutput& /*out*/) const final {
      // NOOP
    }

   private:
    const CustomSort& _sort;
  };

  struct Scorer final : public irs::ScoreCtx {
    Scorer(const CustomSort& sort, const irs::ColumnProvider& segment_reader,
           const irs::FieldProperties& term_reader,
           const irs::byte_type* filter_node_attrs,
           const irs::AttributeProvider& document_attrs)
      : document_attrs(document_attrs),
        filter_node_attrs(filter_node_attrs),
        segment_reader(segment_reader),
        sort(sort),
        term_reader(term_reader) {}

    const irs::AttributeProvider& document_attrs;
    const irs::byte_type* filter_node_attrs;
    const irs::ColumnProvider& segment_reader;
    const CustomSort& sort;
    const irs::FieldProperties& term_reader;
  };

  void collect(irs::byte_type* filter_attrs, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final {
    if (collectors_collect) {
      collectors_collect(filter_attrs, field, term);
    }
  }

  irs::IndexFeatures GetIndexFeatures() const override {
    return irs::IndexFeatures::None;
  }

  irs::FieldCollector::ptr PrepareFieldCollector() const final {
    if (prepare_field_collector) {
      return prepare_field_collector();
    }

    return std::make_unique<FieldCollector>(*this);
  }

  irs::ScoreFunction PrepareScorer(const irs::ColumnProvider& segment_reader,
                                   const irs::FieldProperties& term_reader,
                                   const irs::byte_type* filter_node_attrs,
                                   const irs::AttributeProvider& document_attrs,
                                   irs::score_t boost) const final {
    if (prepare_scorer) {
      return prepare_scorer(segment_reader, term_reader, filter_node_attrs,
                            document_attrs, boost);
    }

    return irs::ScoreFunction::Make<CustomSort::Scorer>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        const auto& state = *reinterpret_cast<Scorer*>(ctx);

        if (state.sort.scorer_score) {
          state.sort.scorer_score(
            irs::get<irs::DocAttr>(state.document_attrs)->value, res);
        }
      },
      irs::ScoreFunction::DefaultMin, *this, segment_reader, term_reader,
      filter_node_attrs, document_attrs);
  }

  irs::TermCollector::ptr PrepareTermCollector() const final {
    if (prepare_term_collector) {
      return prepare_term_collector();
    }

    return std::make_unique<TermCollector>(*this);
  }

  std::function<void(const irs::SubReader&, const irs::TermReader&)>
    collector_collect_field;
  std::function<void(const irs::SubReader&, const irs::TermReader&,
                     const irs::AttributeProvider&)>
    collector_collect_term;
  std::function<void(irs::byte_type*, const irs::FieldCollector*,
                     const irs::TermCollector*)>
    collectors_collect;
  std::function<irs::FieldCollector::ptr()> prepare_field_collector;
  std::function<irs::ScoreFunction(
    const irs::ColumnProvider&, const irs::FieldProperties&,
    const irs::byte_type*, const irs::AttributeProvider&, irs::score_t)>
    prepare_scorer;
  std::function<irs::TermCollector::ptr()> prepare_term_collector;
  std::function<void(irs::doc_id_t, irs::score_t*)> scorer_score;
  std::function<void()> term_reset;
  std::function<void()> field_reset;
};

struct StatsT {
  irs::doc_id_t count;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief order by frequency, then if equal order by doc_id_t
//////////////////////////////////////////////////////////////////////////////
struct FrequencySort : public irs::ScorerBase<FrequencySort, StatsT> {
  struct TermCollector : public irs::TermCollector {
    size_t docs_count{};
    const irs::TermMeta* meta_attr;

    void collect(const irs::SubReader& /*segment*/,
                 const irs::TermReader& /*field*/,
                 const irs::AttributeProvider& term_attrs) final {
      meta_attr = irs::get<irs::TermMeta>(term_attrs);
      ASSERT_NE(nullptr, meta_attr);
      docs_count += meta_attr->docs_count;
    }

    void reset() noexcept final { docs_count = 0; }

    void collect(irs::bytes_view /*in*/) final {
      // NOOP
    }

    void write(irs::DataOutput& /*out*/) const final {
      // NOOP
    }
  };

  struct Scorer final : public irs::ScoreCtx {
    Scorer(const irs::doc_id_t* docs_count, const irs::DocAttr* doc)
      : doc(doc), docs_count(docs_count) {}

    const irs::DocAttr* doc;
    const irs::doc_id_t* docs_count;
  };

  void collect(irs::byte_type* stats_buf, const irs::FieldCollector* /*field*/,
               const irs::TermCollector* term) const final {
    auto* term_ptr = dynamic_cast<const TermCollector*>(term);
    if (term_ptr) {  // may be null e.g. 'all' filter
      stats_cast(stats_buf)->count =
        static_cast<irs::doc_id_t>(term_ptr->docs_count);
      const_cast<TermCollector*>(term_ptr)->docs_count = 0;
    }
  }

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::FieldCollector::ptr PrepareFieldCollector() const final {
    return nullptr;  // do not need to collect stats
  }

  irs::ScoreFunction PrepareScorer(const irs::ColumnProvider&,
                                   const irs::FieldProperties&,
                                   const irs::byte_type* stats_buf,
                                   const irs::AttributeProvider& doc_attrs,
                                   irs::score_t /*boost*/) const final {
    auto* doc = irs::get<irs::DocAttr>(doc_attrs);
    auto* stats = stats_cast(stats_buf);
    const irs::doc_id_t* docs_count = &stats->count;
    return irs::ScoreFunction::Make<FrequencySort::Scorer>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        const auto& state = *reinterpret_cast<Scorer*>(ctx);

        // docs_count may be nullptr if no collector called,
        // e.g. by range_query for BitsetDocIterator
        if (state.docs_count) {
          *res = 1.f / (*state.docs_count);
        } else {
          *res = std::numeric_limits<irs::score_t>::infinity();
        }
      },
      irs::ScoreFunction::DefaultMin, docs_count, doc);
  }

  irs::TermCollector::ptr PrepareTermCollector() const final {
    return std::make_unique<TermCollector>();
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @brief Report FreqAttr frequency as score
//////////////////////////////////////////////////////////////////////////////
struct FrequencyScore : public irs::ScorerBase<FrequencyScore, StatsT> {
  struct Scorer final : public irs::ScoreCtx {
    Scorer(const irs::FreqAttr* fr) : freq(fr) {}

    const irs::FreqAttr* freq;
  };

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ColumnProvider&,
                                   const irs::FieldProperties&,
                                   const irs::byte_type* /*stats_buf*/,
                                   const irs::AttributeProvider& doc_attrs,
                                   irs::score_t /*boost*/) const final {
    auto* freq = irs::get<irs::FreqAttr>(doc_attrs);
    return irs::ScoreFunction::Make<FrequencyScore::Scorer>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        const auto& state = *reinterpret_cast<Scorer*>(ctx);
        if (state.freq) {
          *res = state.freq->value;
        } else {
          *res = 0;
        }
      },
      irs::ScoreFunction::DefaultMin, freq);
  }
};

}  // namespace sort

class FilterTestCaseBase : public IndexTestBase {
 protected:
  using Docs = std::vector<irs::doc_id_t>;
  using ScoredDocs =
    std::vector<std::pair<irs::doc_id_t, std::vector<irs::score_t>>>;
  using Costs = std::vector<irs::CostAttr::Type>;

  struct Seek {
    irs::doc_id_t target;
  };

  struct Skip {
    irs::doc_id_t count;
  };

  struct Next {};

  using Action = std::variant<Seek, Skip, Next>;

  struct Test {
    Action action;
    irs::doc_id_t expected;
    std::vector<irs::score_t> score{};
  };

  using Tests = std::vector<Test>;

  // Validate matched documents and query cost
  static void CheckQuery(const irs::Filter& filter, const Docs& expected,
                         const Costs& expected_costs,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate matched documents
  static void CheckQuery(const irs::Filter& filter, const Docs& expected,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate documents and its scores
  static void CheckQuery(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const ScoredDocs& expected,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate documents and its scores with test cases
  static void CheckQuery(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const std::vector<Tests>& tests,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  static void MakeResult(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const irs::IndexReader& rdr,
                         std::vector<irs::doc_id_t>& result,
                         bool score_must_be_present = true,
                         bool reverse = false);

  // Validate document order
  static void CheckQuery(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const std::vector<irs::doc_id_t>& expected,
                         const irs::IndexReader& index,
                         bool score_must_be_present = true,
                         bool reverse = false);

 private:
  static void GetQueryResult(const irs::Filter::Query::ptr& q,
                             const irs::IndexReader& index, Docs& result,
                             Costs& result_costs,
                             std::string_view source_location);

  static void GetQueryResult(const irs::Filter::Query::ptr& q,
                             const irs::IndexReader& index,
                             const irs::Scorers& ord, ScoredDocs& result,
                             Costs& result_costs,
                             std::string_view source_location);
};

struct EmptyTermReader : irs::Singleton<EmptyTermReader>, irs::TermReader {
  irs::SeekTermIterator::ptr iterator(irs::SeekMode) const final {
    return irs::SeekTermIterator::empty();
  }

  irs::SeekTermIterator::ptr iterator(
    irs::automaton_table_matcher&) const final {
    return irs::SeekTermIterator::empty();
  }

  const irs::FieldMeta& meta() const final {
    static irs::FieldMeta gEmpty;
    return gEmpty;
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

  // total number of terms
  size_t size() const final { return 0; }

  // total number of documents
  uint64_t docs_count() const final { return 0; }

  // less significant term
  irs::bytes_view(min)() const final { return {}; }

  // most significant term
  irs::bytes_view(max)() const final { return {}; }
};

class EmptyFilterVisitor : public irs::FilterVisitor {
 public:
  void Prepare(const irs::SubReader& /*segment*/,
               const irs::TermReader& /*field*/,
               const irs::SeekTermIterator& terms) noexcept final {
    _it = &terms;
    ++_prepare_calls_counter;
  }

  void Visit(irs::score_t boost) noexcept final {
    ASSERT_NE(nullptr, _it);
    _terms.emplace_back(_it->value(), boost);
    ++_visit_calls_counter;
  }

  void reset() noexcept {
    _prepare_calls_counter = 0;
    _visit_calls_counter = 0;
    _terms.clear();
    _it = nullptr;
  }

  size_t prepare_calls_counter() const noexcept {
    return _prepare_calls_counter;
  }

  size_t visit_calls_counter() const noexcept { return _visit_calls_counter; }

  const std::vector<std::pair<irs::bstring, irs::score_t>>& terms()
    const noexcept {
    return _terms;
  }

  template<typename Char>
  std::vector<std::pair<irs::basic_string_view<Char>, irs::score_t>> term_refs()
    const {
    std::vector<std::pair<irs::basic_string_view<Char>, irs::score_t>> refs(
      _terms.size());
    auto begin = refs.begin();
    for (auto& term : _terms) {
      begin->first = irs::ViewCast<Char>(irs::bytes_view{term.first});
      begin->second = term.second;
      ++begin;
    }
    return refs;
  }

  virtual void assert_boost(irs::score_t boost) {
    ASSERT_EQ(irs::kNoBoost, boost);
  }

 private:
  const irs::SeekTermIterator* _it{};
  std::vector<std::pair<irs::bstring, irs::score_t>> _terms;
  size_t _prepare_calls_counter = 0;
  size_t _visit_calls_counter = 0;
};

}  // namespace tests
