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

#include <utility>

#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/lazy_bitset.hpp"
#include "iresearch/search/common/posting_probe.hpp"
#include "iresearch/search/count/term_counts.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

using search::CountAgainst;
using search::LazyBitset;
using search::PostingProbe;
using search::PostingReader;
using search::ReadPosting;

template<typename Input>
class TermCountsOf : public TermCounts {
 public:
  TermCountsOf(LazyBitset& set, const IndexInput& doc, IndexFeatures layout,
               bool bounds) noexcept
    : _set{set}, _reader{doc}, _doc{&doc}, _layout{layout}, _bounds{bounds} {}

  uint64_t Count(const PostingMeta& term) final {
    SDB_ASSERT(term.docs_count != 0);
    CountAgainst sink{_set};
    if (term.docs_count == 1) {
      sink.Doc(doc_limits::min() + term.doc_delta);
    } else {
      ReadPosting(term, _reader.In(), _reader.Enc(), _reader.Docs(), _bounds,
                  FeaturesHaveFreq(_layout), sink);
    }
    return sink.Total();
  }

  bool Any(const PostingMeta& term) final {
    SDB_ASSERT(term.docs_count != 0);
    if (term.docs_count == 1) {
      return _set.Contains(doc_limits::min() + term.doc_delta);
    }
    PostingProbe<Input> posting{term, *_doc, _layout, _bounds};
    auto doc = doc_limits::min();
    for (;;) {
      doc = posting.Probe(doc);
      if (doc_limits::eof(doc)) {
        return false;
      }
      const auto next = _set.Probe(doc);
      if (next == doc) {
        return true;
      }
      if (doc_limits::eof(next)) {
        return false;
      }
      doc = next;
    }
  }

 private:
  LazyBitset& _set;
  PostingReader<Input> _reader;
  const IndexInput* _doc;
  IndexFeatures _layout;
  bool _bounds;
};

}  // namespace irs::count
