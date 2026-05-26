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

#include "range_filter.hpp"

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_collector.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

template<typename Visitor, typename Comparer>
void CollectTerms(const SubReader& segment, const TermReader& field,
                  SeekTermIterator& terms, Visitor& visitor, Comparer cmp) {
  auto* term = irs::get<TermAttr>(terms);

  if (!term) [[unlikely]] {
    return;
  }

  if (cmp(term->value)) {
    // read attributes
    terms.read();

    visitor.Prepare(segment, field, terms);

    do {
      visitor.Visit(kNoBoost);

      if (!terms.next()) {
        break;
      }

      terms.read();
    } while (cmp(term->value));
  }
}

template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& reader,
               const ByRangeOptions::range_type& rng, Visitor& visitor) {
  auto terms = reader.iterator(SeekMode::NORMAL);

  if (!terms) [[unlikely]] {
    return;
  }

  auto res = false;

  // seek to min
  switch (rng.min_type) {
    case BoundType::Unbounded:
      res = terms->next();
      break;
    case BoundType::Inclusive:
      res = seek_min<true>(*terms, rng.min);
      break;
    case BoundType::Exclusive:
      res = seek_min<false>(*terms, rng.min);
      break;
  }

  if (!res) {
    // reached the end, nothing to collect
    return;
  }

  // now we are on the target or the next term
  const bytes_view max = rng.max;

  switch (rng.max_type) {
    case BoundType::Unbounded:
      CollectTerms(segment, reader, *terms, visitor,
                   [](bytes_view) { return true; });
      break;
    case BoundType::Inclusive:
      CollectTerms(segment, reader, *terms, visitor,
                   [max](bytes_view term) { return term <= max; });
      break;
    case BoundType::Exclusive:
      CollectTerms(segment, reader, *terms, visitor,
                   [max](bytes_view term) { return term < max; });
      break;
  }
}

}  // namespace
namespace {

class Buffer final : public SampledMultiTermBuffer {
 public:
  Buffer(const PrepareContext& ctx, std::string_view field,
         const ByRangeOptions::range_type& rng, size_t scored_terms_limit,
         score_t boost = kNoBoost)
    : SampledMultiTermBuffer{ctx, scored_terms_limit, boost},
      _field{field},
      _rng{&rng} {}

  void PrepareSegment(const SubReader& segment) final {
    if (const auto* reader = segment.field(_field); reader) {
      VisitImpl(segment, *reader, *_rng, _visitor);
    }
  }

 private:
  std::string_view _field;
  const ByRangeOptions::range_type* _rng;
};

}  // namespace

Filter::Query::ptr ByRange::Prepare(const PrepareContext& ctx,
                                    std::string_view field,
                                    const options_type::range_type& rng,
                                    size_t scored_terms_limit) {
  // TODO: optimize unordered case
  //  - seek to min
  //  - get ordinal position of the term
  //  - seek to max
  //  - get ordinal position of the term

  if (rng.min_type != BoundType::Unbounded &&
      rng.max_type != BoundType::Unbounded && rng.min == rng.max) {
    if (rng.min_type == rng.max_type && rng.min_type == BoundType::Inclusive) {
      // degenerated case
      return ByTerm::prepare(ctx, field, rng.min);
    }

    // can't satisfy conditon
    return Query::empty();
  }

  Buffer buf{ctx, field, rng, scored_terms_limit};
  return Filter::PrepareWithBuffer<Buffer>(buf, ctx);
}

std::unique_ptr<Filter::PrepareBuffer> ByRange::CreateBuffer(
  const PrepareContext& ctx) const {
  const auto& rng = options().range;
  if (rng.min_type != BoundType::Unbounded &&
      rng.max_type != BoundType::Unbounded && rng.min == rng.max) {
    if (rng.min_type == BoundType::Inclusive &&
        rng.max_type == BoundType::Inclusive) {
      return std::make_unique<ByTerm::Buffer>(ctx, field(), rng.min, Boost());
    }
    return std::make_unique<EmptyBuffer>();
  }
  return std::make_unique<Buffer>(ctx, field(), options().range,
                                  options().scored_terms_limit, Boost());
}

void ByRange::visit(const SubReader& segment, const TermReader& reader,
                    const options_type::range_type& rng,
                    FilterVisitor& visitor) {
  VisitImpl(segment, reader, rng, visitor);
}

}  // namespace irs
