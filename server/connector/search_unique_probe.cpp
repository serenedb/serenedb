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
#include <absl/functional/function_ref.h>

#include <algorithm>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/key_removal.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <utility>

#include "connector/term_dict.h"

namespace sdb::connector {
namespace {

// A field's postings are partitioned by dictionary before they are partitioned
// by term, so the reader a key reads from is the one its own value was written
// under. A field written under one dictionary only -- every type but a boolean
// -- settles on the first.
const irs::TermReader* DictReader(
  std::span<const irs::TermReader* const> readers, irs::DictRef dict) {
  for (const auto* reader : readers) {
    if (reader->meta().dict == dict) {
      return reader;
    }
  }
  return nullptr;
}

void ForEachLiveDoc(const irs::TermReader& field, irs::bytes_view term,
                    const irs::DocumentMask::Iterator& mask,
                    const irs::DocumentMaskBuilder& deleted,
                    absl::FunctionRef<bool(irs::doc_id_t)> visit) {
  field.ReadDocs(term, [&](irs::doc_id_t doc) {
    if (mask.Contains(doc) || deleted.Contains(doc)) {
      return true;
    }
    return visit(doc);
  });
}

}  // namespace

UniqueProbe::UniqueProbe(
  const irs::DirectoryReader* reader,
  std::span<const irs::SubReader* const> unpublished_segments,
  std::vector<const KeyTerms*> unpublished_keys,
  std::span<const irs::field_id> key_fields, const PendingRemovals& removed)
  : _keys{std::move(unpublished_keys)} {
  SDB_ASSERT(!key_fields.empty());
  const size_t published = reader ? reader->size() : 0;
  _segments.reserve(published + unpublished_segments.size());

  for (size_t i = 0; i != published; ++i) {
    AddSegment((*reader)[i], key_fields);
  }
  for (const auto* sub : unpublished_segments) {
    AddSegment(*sub, key_fields);
  }
  ApplyRemovals(removed);

  absl::c_sort(_keys,
               [](const auto* lhs, const auto* rhs) { return *lhs < *rhs; });
  _column_terms.resize(key_fields.size());
  _column_indexed.assign(key_fields.size(), false);
}

void UniqueProbe::AddSegment(const irs::SubReader& sub,
                             std::span<const irs::field_id> key_fields) {
  Segment segment{.sub = &sub, .mask = sub.MaskedDocs()};

  segment.fields.reserve(key_fields.size());
  for (const auto field : key_fields) {
    const auto readers = sub.fields(field);
    if (readers.empty()) {
      // No document here has that key column indexed, so none carries a whole
      // key either.
      return;
    }
    segment.fields.push_back(readers);
  }
  _segments.push_back(std::move(segment));
}

void UniqueProbe::ApplyRemovals(const PendingRemovals& removed) {
  if (removed.docs != nullptr) {
    irs::RemovalTargets targets;
    targets.reserve(_segments.size());
    for (auto& segment : _segments) {
      uint64_t id = 0;
      if (irs::ParseSegmentId(segment.sub->Meta().name, id)) {
        // `ResolveRemoval` lands the docs; the reader it offers alongside is
        // for a caller that carries removals onward, which this one does not.
        targets.emplace(id, irs::RemovalTarget{.reader = nullptr,
                                               .removed = &segment.deleted});
      }
    }
    irs::ResolveRemoval(*removed.docs, targets);
  }
  if (removed.keys.empty()) {
    return;
  }
  SDB_ASSERT(absl::c_is_sorted(removed.keys));
  for (auto& segment : _segments) {
    irs::RemoveKeys(
      removed.keys, term_dict::kPKFieldId, *segment.sub,
      [&](const int64_t&, irs::doc_id_t doc) { segment.deleted.Add(doc); });
  }
}

std::span<const UniqueProbe::ColumnTerm> UniqueProbe::ColumnTerms(
  size_t field) {
  SDB_ASSERT(field < _column_terms.size());
  auto& terms = _column_terms[field];
  if (!_column_indexed[field]) {
    _column_indexed[field] = true;
    terms.reserve(_keys.size());
    for (const auto* key : _keys) {
      SDB_ASSERT(field < key->size());
      const auto& component = (*key)[field];
      terms.push_back({.dict = component.dict, .term = component.term});
    }
    absl::c_sort(terms);
    // TODO(Dronplane): can be assert for single column unique constraint.
    // But that is small tail so might be good as is. Recheck if perf shows this.
    terms.erase(std::ranges::unique(terms).begin(), terms.end());
  }
  return terms;
}

bool UniqueProbe::MayHold(size_t field, const KeyTerm& term) {
  for (const auto& segment : _segments) {
    SDB_ASSERT(field < segment.fields.size());
    const auto* reader = DictReader(segment.fields[field], term.dict);
    if (reader != nullptr && reader->Lookup(term.term).docs_count != 0) {
      return true;
    }
  }
  return !_keys.empty() && absl::c_binary_search(
                             ColumnTerms(field),
                             ColumnTerm{.dict = term.dict, .term = term.term});
}

bool UniqueProbe::Exists(std::span<const KeyTerm> key) {
  for (auto& segment : _segments) {
    SDB_ASSERT(segment.fields.size() == key.size());

    bool absent = false;
    size_t seed = 0;
    auto rarest = std::numeric_limits<uint64_t>::max();
    _readers.clear();
    for (size_t i = 0; i != key.size(); ++i) {
      const auto* reader = DictReader(segment.fields[i], key[i].dict);
      const uint64_t docs =
        reader != nullptr ? reader->Lookup(key[i].term).docs_count : 0;
      if (docs == 0) {
        absent = true;
        break;
      }
      _readers.push_back(reader);
      if (docs < rarest) {
        rarest = docs;
        seed = i;
      }
    }
    if (absent) {
      continue;
    }

    _hits.clear();
    ForEachLiveDoc(*_readers[seed], key[seed].term, segment.mask,
                   segment.deleted, [&](irs::doc_id_t doc) {
                     _hits.push_back(doc);
                     return true;
                   });

    for (size_t i = 0; i != key.size() && !_hits.empty(); ++i) {
      if (i == seed) {
        continue;
      }
      // Posting lists ascend by doc id, so _hits does too -- relied on by the
      // lower_bound below, both for membership and as the stop signal.
      SDB_ASSERT(absl::c_is_sorted(_hits));

      // TODO(Dronplane): seek to _hits.front() instead of walking from the
      // start of the posting list. ReadDocs is push-only and TermPostings has
      // no seek; only lead::Node does, which means preparing a filter. Revisit
      // when a doc-level seek reaches the TermReader surface, or alongside the
      // sequential dictionary scan of the parallel-path merge.
      _next.clear();
      ForEachLiveDoc(*_readers[i], key[i].term, segment.mask, segment.deleted,
                     [&](irs::doc_id_t doc) {
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
  const auto it = absl::c_lower_bound(
    _keys, key, [](const KeyTerms* lhs, std::span<const KeyTerm> rhs) {
      return std::lexicographical_compare(lhs->begin(), lhs->end(), rhs.begin(),
                                          rhs.end());
    });
  return it != _keys.end() && absl::c_equal(**it, key);
}

}  // namespace sdb::connector
