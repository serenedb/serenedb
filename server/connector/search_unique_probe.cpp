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

#include "connector/search_unique_probe.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <span>
#include <vector>

#include "connector/primary_key.h"
#include "connector/search_remove_filter.hpp"
#include "connector/term_dict.h"

namespace sdb::connector {

UniqueProbe::UniqueProbe(
  const irs::DirectoryReader& reader,
  std::span<const irs::SubReader* const> unpublished_segments,
  std::span<const std::vector<irs::bstring>* const> unpublished_keys,
  std::span<const irs::field_id> key_fields,
  std::span<const search::RowRange> deleted_ranges) {
  SDB_ASSERT(!key_fields.empty());
  _segments.reserve(reader.size() + unpublished_segments.size());

  for (size_t i = 0; i != reader.size(); ++i) {
    AddSegment(reader[i], key_fields, deleted_ranges);
  }
  for (const auto* sub : unpublished_segments) {
    AddSegment(*sub, key_fields, deleted_ranges);
  }

  _keys.assign(unpublished_keys.begin(), unpublished_keys.end());
  absl::c_sort(_keys,
               [](const auto* lhs, const auto* rhs) { return *lhs < *rhs; });
  _column_terms.resize(key_fields.size());
  _column_indexed.assign(key_fields.size(), false);
}

void UniqueProbe::AddSegment(const irs::SubReader& sub,
                             std::span<const irs::field_id> key_fields,
                             std::span<const search::RowRange> deleted_ranges) {
  Segment segment{.sub = &sub, .mask = sub.MaskedDocs()};

  segment.fields.reserve(key_fields.size());
  for (const auto field : key_fields) {
    const auto* term_reader = sub.field(field);
    if (term_reader == nullptr) {
      break;
    }
    segment.fields.push_back(term_reader);
  }
  if (segment.fields.size() != key_fields.size()) {
    return;
  }

  if (!deleted_ranges.empty()) {
    const auto* pk_field = sub.field(term_dict::kPKFieldId);
    SDB_ASSERT(pk_field);
    for (const auto& range : deleted_ranges) {
      for (auto rowid = range.begin; rowid != range.end; ++rowid) {
        const auto term =
          primary_key::GeneratedKeyTerm(static_cast<uint64_t>(rowid));
        const auto doc =
          FirstLiveDoc(*pk_field,
                       irs::ViewCast<irs::byte_type>(
                         std::string_view{term.GetData(), term.GetSize()}),
                       segment.mask, nullptr);
        if (!irs::doc_limits::eof(doc)) {
          segment.deleted.Add(doc);
        }
      }
    }
  }
  _segments.push_back(std::move(segment));
}

std::span<const irs::bytes_view> UniqueProbe::ColumnTerms(size_t field) {
  SDB_ASSERT(field < _column_terms.size());
  auto& terms = _column_terms[field];
  if (!_column_indexed[field]) {
    _column_indexed[field] = true;
    terms.reserve(_keys.size());
    for (const auto* key : _keys) {
      SDB_ASSERT(field < key->size());
      terms.push_back((*key)[field]);
    }
    absl::c_sort(terms);
    terms.erase(std::ranges::unique(terms).begin(), terms.end());
  }
  return terms;
}

bool UniqueProbe::MayHold(size_t field, irs::bytes_view term) {
  for (const auto& segment : _segments) {
    SDB_ASSERT(field < segment.fields.size());
    if (segment.fields[field]->Lookup(term).docs_count != 0) {
      return true;
    }
  }
  return !_keys.empty() && absl::c_binary_search(ColumnTerms(field), term);
}

bool UniqueProbe::Exists(std::span<const irs::bstring> key_terms) {
  for (auto& segment : _segments) {
    SDB_ASSERT(segment.fields.size() == key_terms.size());

    bool absent = false;
    size_t seed = 0;
    auto rarest = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0; i != key_terms.size(); ++i) {
      const uint64_t docs = segment.fields[i]->Lookup(key_terms[i]).docs_count;
      if (docs == 0) {
        absent = true;
        break;
      }
      if (docs < rarest) {
        rarest = docs;
        seed = i;
      }
    }
    if (absent) {
      continue;
    }

    _hits.clear();
    ForEachLiveDoc(*segment.fields[seed], key_terms[seed], segment.mask,
                   &segment.deleted, [&](irs::doc_id_t doc) {
                     _hits.push_back(doc);
                     return true;
                   });

    for (size_t i = 0; i != key_terms.size() && !_hits.empty(); ++i) {
      if (i == seed) {
        continue;
      }
      // Posting lists ascend by doc id, so _hits does too -- relied on by the
      // lower_bound below, both for membership and as the stop signal.
      SDB_ASSERT(absl::c_is_sorted(_hits));

      // TODO(dronplane): seek to _hits.front() instead of walking from the
      // start of the posting list. ReadDocs is push-only and TermPostings has
      // no seek; only lead::Node does, which means preparing a filter. Revisit
      // when a doc-level seek reaches the TermReader surface, or alongside the
      // sequential dictionary scan of the parallel-path merge.
      _next.clear();
      ForEachLiveDoc(*segment.fields[i], key_terms[i], segment.mask,
                     &segment.deleted, [&](irs::doc_id_t doc) {
                       const auto it = absl::c_lower_bound(_hits, doc);
                       if (it == _hits.end()) {
                         return false;
                       }
                       if (*it == doc) {
                         _next.push_back(doc);
                       }
                       return _next.size() != _hits.size();
                     });
      _hits.swap(_next);
    }

    if (!_hits.empty()) {
      return true;
    }
  }
  // An unpublished key needs no delete filtering: a generation holds the keys
  // of one committed transaction's live rows, and a later removal of one of
  // them is a rowid in the delete set of whichever generation recorded it,
  // which only ever masks a doc the reader still shows.
  const auto it =
    absl::c_lower_bound(_keys, key_terms,
                        [](const std::vector<irs::bstring>* lhs,
                           std::span<const irs::bstring> rhs) {
                          return std::lexicographical_compare(
                            lhs->begin(), lhs->end(), rhs.begin(), rhs.end());
                        });
  return it != _keys.end() && absl::c_equal(**it, key_terms);
}

}  // namespace sdb::connector
