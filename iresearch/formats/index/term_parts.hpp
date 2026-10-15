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

#include <cstdint>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class IndexInput;

struct TermPart {
  IndexInput* doc = nullptr;
  IndexInput* pos = nullptr;
  IndexInput* pay = nullptr;
  bytes_view terms;
  uint64_t term_count = 0;
  uint64_t total_doc_freq = 0;
  uint64_t total_term_freq = 0;
};

class TermPartWriter {
 public:
  explicit TermPartWriter(IndexFeatures features) noexcept;

  void Add(bytes_view term, const PostingMeta& meta);

  bytes_view Terms() const noexcept { return _terms; }
  uint64_t Count() const noexcept { return _count; }
  uint64_t TotalDocFreq() const noexcept { return _total_doc_freq; }
  uint64_t TotalTermFreq() const noexcept { return _total_term_freq; }

 private:
  bstring _terms;
  BytesOutput _out{_terms};
  bstring _last;
  PostingMeta _prev;
  uint64_t _count = 0;
  uint64_t _total_doc_freq = 0;
  uint64_t _total_term_freq = 0;
  bool _has_freq;
  bool _has_pos;
  bool _has_offs;
};

class TermPartReader {
 public:
  TermPartReader(bytes_view terms, IndexFeatures features) noexcept;

  bool Next();

  bytes_view Term() const noexcept { return _term; }
  const PostingMeta& Meta() const noexcept { return _meta; }

 private:
  BytesViewInput _in;
  bstring _term;
  PostingMeta _meta;
  bool _has_freq;
  bool _has_pos;
  bool _has_offs;
};

}  // namespace irs
