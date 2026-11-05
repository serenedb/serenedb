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

#include <vector>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/index/term_dict_reader.hpp"

namespace irs::term_dict {
namespace {

struct OrderedEntry {
  uint32_t key;
  uint32_t size;
  PostingMeta meta;
};

}  // namespace

void TermReaderImpl::VisitDocsInOrder(bytes_view from, bytes_view to,
                                      bool descending, TermDoc visit) const {
  if (_terms == 0 || (!from.empty() && from > max()) ||
      (!to.empty() && to <= min())) {
    return;
  }
  bool going = true;
  const auto read = [&](bytes_view term, const PostingMeta& meta) {
    ReadPostingDocs(*this, meta, [&](doc_id_t doc) {
      going = visit(term, doc);
      return going;
    });
    return going;
  };
  Cursor cursor{*this};
  if (!descending) {
    if (from.empty()) {
      cursor.First();
    } else if (cursor.SeekGe(from, false) == SeekResult::End) {
      return;
    }
    do {
      const auto term = cursor.Term();
      if (!to.empty() && term >= to) {
        return;
      }
      if (!read(term, cursor.Meta())) {
        return;
      }
    } while (cursor.Advance());
    return;
  }
  PaddedKey probe;
  const uint32_t last =
    to.empty() ? _blocks - 1 : BlockOf(probe.Assign(KeyPrefix(), to));
  bstring keys;
  std::vector<OrderedEntry> entries;
  for (uint32_t block = last + 1; block-- != 0;) {
    if (block == 0) {
      cursor.First();
    } else {
      cursor.Load(block);
      cursor.Next();
    }
    keys.clear();
    entries.clear();
    bool below = false;
    for (;;) {
      const auto term = cursor.Term();
      if (!to.empty() && term >= to) {
        break;
      }
      if (!from.empty() && term < from) {
        below = true;
      } else {
        entries.emplace_back(static_cast<uint32_t>(keys.size()),
                             static_cast<uint32_t>(term.size()), cursor.Meta());
        keys.append(term);
      }
      if (cursor.AtBlockEnd()) {
        break;
      }
      cursor.Next();
    }
    for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
      if (!read(bytes_view{keys}.substr(it->key, it->size), it->meta)) {
        return;
      }
    }
    if (below) {
      return;
    }
  }
}

}  // namespace irs::term_dict
