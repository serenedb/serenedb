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

#include "iresearch/index/iterators.hpp"

#include <absl/algorithm/container.h>

#include <functional>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/singleton.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// A term whose posting list holds nothing
struct EmptyTermPostings : TermPostings {
  uint32_t NextDocs(doc_id_t* /*docs*/, uint32_t* /*freqs*/) noexcept final {
    return 0;
  }
};

EmptyTermPostings gEmptyTermPostings;

// Represents an iterator without terms
struct EmptySeekTermIterator : SeekTermIterator {
  bytes_view value() const noexcept final { return {}; }

  TermPostings::ptr postings(IndexFeatures /*features*/) const noexcept final {
    return TermPostings::empty();
  }

  bool next() noexcept final { return false; }

  Attribute* GetMutable(TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }

  SeekResult seek_ge(bytes_view /*value*/) noexcept final {
    return SeekResult::End;
  }

  bool seek(bytes_view /*value*/) noexcept final { return false; }

  const PostingMeta& cookie() const noexcept final { return kNoPosting; }
};

EmptySeekTermIterator gEmptySeekIterator;

}  // namespace

void LoserScoreCollector::SortHits() noexcept {
  std::span keys{_tree.data(), _size};
  if (_size == _k) {
    keys[0] = _root;
  } else {
    for (uint32_t i = 0; i != _size; ++i) {
      keys[i] = MakeNode(_hits[i].score, i);
    }
  }
  absl::c_sort(keys, std::greater<>{});
  for (size_t i = 0; i != _size; ++i) {
    if (Leaf(keys[i]) == kNone) {
      continue;
    }
    const auto first = _hits[i];
    for (auto at = i;;) {
      const auto from = Leaf(keys[at]);
      keys[at] = kNone;
      if (from == i) {
        _hits[at] = first;
        break;
      }
      _hits[at] = _hits[from];
      at = from;
    }
  }
}

SeekTermIterator::ptr SeekTermIterator::empty() noexcept {
  return memory::to_managed<SeekTermIterator>(gEmptySeekIterator);
}

TermPostings::ptr TermPostings::empty() noexcept {
  return memory::to_managed<TermPostings>(gEmptyTermPostings);
}

}  // namespace irs
