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

#include <iresearch/utils/assert.hpp>
#include <limits>

#include "connector/primary_key.h"
#include "connector/search_remove_filter.hpp"
#include "connector/term_dict.h"

namespace sdb::connector {

UniqueProbe::UniqueProbe(const irs::DirectoryReader& reader,
                         std::span<const irs::field_id> key_fields,
                         std::span<const int64_t> deleted_rowids) {
  SDB_ASSERT(!key_fields.empty());
  _segments.reserve(reader.size());

  for (size_t i = 0; i != reader.size(); ++i) {
    const auto& sub = reader[i];
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
      continue;
    }

    if (!deleted_rowids.empty()) {
      const auto* pk_field = sub.field(term_dict::kPKFieldId);
      SDB_ASSERT(pk_field);
      for (const auto rowid : deleted_rowids) {
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
    _segments.push_back(std::move(segment));
  }
}

bool UniqueProbe::MayHold(size_t field, irs::bytes_view term) const {
  for (const auto& segment : _segments) {
    SDB_ASSERT(field < segment.fields.size());
    if (segment.fields[field]->Lookup(term).docs_count != 0) {
      return true;
    }
  }
  return false;
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
  return false;
}

}  // namespace sdb::connector
