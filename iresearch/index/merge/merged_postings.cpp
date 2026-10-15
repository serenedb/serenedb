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

#include "iresearch/index/merge/merged_postings.hpp"

#include <algorithm>

namespace irs {

uint32_t CompoundPostings::NextDocs(doc_id_t* docs, uint32_t* freqs) {
  for (; _current_itr < _iterators.size(); ++_current_itr) {
    auto& [it, remap, source] = _iterators[_current_itr];
    uint32_t n = 0;
    if (remap->id_map.empty()) {
      n = it->NextDocs(docs, freqs);
      const auto shift = remap->base_id - doc_limits::min();
      for (uint32_t i = 0; i != n; ++i) {
        docs[i] += shift;
      }
    } else {
      n = NextLive(*it, *remap, docs, freqs);
    }
    if (n != 0) {
      if (!_progress(n)) {
        _iterators.clear();
        return 0;
      }
      return n;
    }
  }
  return 0;
}

uint32_t CompoundPostings::NextLive(TermPostings& it, const DocRemap& remap,
                                    doc_id_t* docs, uint32_t* freqs) {
  while (true) {
    if (_at == _size) {
      _size = it.NextDocs(_docs, _freqs);
      _at = 0;
      if (_size == 0) {
        return 0;
      }
    }
    uint64_t skip = 0;
    for (; _at != _size && remap.IsMasked(_docs[_at]); ++_at) {
      if (_has_pos) {
        skip += _freqs[_at];
      }
    }
    if (skip != 0) {
      it.SkipPositions(skip);
    }
    uint32_t n = 0;
    for (; _at != _size && !remap.IsMasked(_docs[_at]); ++_at, ++n) {
      docs[n] = remap.Remap(_docs[_at]);
      if (_has_freq) {
        freqs[n] = _freqs[_at];
      }
    }
    if (n != 0) {
      return n;
    }
  }
}

void SortedPostings::Settle(size_t i) {
  auto& c = _cursors[i];
  auto& [it, remap, source] = _iterators[i];
  while (true) {
    if (c.at == c.size) {
      c.size = it->NextDocs(c.docs, c.freqs);
      c.at = 0;
      if (c.size == 0) {
        c.head = doc_limits::eof();
        return;
      }
    }
    const auto doc = c.docs[c.at];
    if (!remap->IsMasked(doc)) {
      c.head = remap->Remap(doc);
      return;
    }
    if (_has_pos) {
      c.skip += c.freqs[c.at];
    }
    ++c.at;
  }
}

uint32_t SortedPostings::NextDocs(doc_id_t* docs, uint32_t* freqs) {
  uint32_t n = 0;
  while (n != doc_limits::kBlockSize) {
    size_t best = 0;
    doc_id_t best_head = doc_limits::eof();
    doc_id_t rival_head = doc_limits::eof();
    for (size_t i = 0; i != _cursors.size(); ++i) {
      const auto head = _cursors[i].head;
      if (head < best_head) {
        rival_head = best_head;
        best_head = head;
        best = i;
      } else if (head < rival_head) {
        rival_head = head;
      }
    }
    if (doc_limits::eof(best_head)) {
      break;
    }
    auto& c = _cursors[best];
    while (n != doc_limits::kBlockSize && c.head < rival_head) {
      docs[n] = c.head;
      if (_has_freq) {
        freqs[n] = c.freqs[c.at];
      }
      _input[n] = best;
      _freq[n] = _has_pos ? c.freqs[c.at] : 0;
      _skip[n] = c.skip;
      c.skip = 0;
      ++n;
      ++c.at;
      Settle(best);
    }
  }
  _recorded = n;
  _walked = 0;
  _left = 0;
  if (n != 0 && !_progress(n)) {
    Clear();
    return 0;
  }
  return n;
}

void SortedPostings::NextPositions(uint32_t* pos, uint32_t* offs_start,
                                   uint32_t* offs_len, uint32_t n) {
  while (n != 0) {
    if (_left == 0) {
      SDB_ASSERT(_walked < _recorded);
      _current = _input[_walked];
      if (_skip[_walked] != 0) {
        _iterators[_current].it->SkipPositions(_skip[_walked]);
      }
      _left = _freq[_walked++];
      while (_walked != _recorded && _input[_walked] == _current &&
             _skip[_walked] == 0) {
        _left += _freq[_walked++];
      }
    }
    const auto take = static_cast<uint32_t>(std::min<uint64_t>(n, _left));
    _iterators[_current].it->NextPositions(pos, offs_start, offs_len, take);
    pos += take;
    if (offs_start != nullptr) {
      offs_start += take;
      offs_len += take;
    }
    n -= take;
    _left -= take;
  }
}

}  // namespace irs
