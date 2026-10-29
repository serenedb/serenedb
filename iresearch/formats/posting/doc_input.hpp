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

#include <array>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

template<typename Metas>
void PrefetchDocExtents(const IndexInput& in, Metas&& metas) {
  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  for (const PostingMeta& meta : metas) {
    if (const auto extent = DocExtent(meta); extent != 0) {
      ranges.emplace_back(meta.doc_start, meta.doc_start + extent);
    }
  }
  PrefetchRanges(in, ranges);
}

class InlineInput final : public BytesViewInput {
 public:
  InlineInput() = default;
  explicit InlineInput(bytes_view data) noexcept { Assign(data); }
  InlineInput(const InlineInput&) = delete;
  InlineInput& operator=(const InlineInput&) = delete;

  void Assign(bytes_view data) noexcept {
    SDB_ASSERT(data.size() <= PostingMeta::kInlineBytes);
    std::memcpy(_bytes.data(), data.data(), data.size());
    reset(_bytes.data(), data.size());
  }

  ptr Dup() const final {
    auto in =
      std::make_unique<InlineInput>(bytes_view{_bytes.data(), Length()});
    in->Seek(Position());
    return in;
  }

 private:
  std::array<byte_type, PostingMeta::kInlineBytes + block_codec::kInSlack>
    _bytes{};
};

inline IndexInput::ptr OpenDocInput(const PostingMeta& meta,
                                    const IndexInput& doc_in) {
  if (meta.inline_size != 0) {
    return std::make_unique<InlineInput>(meta.Inline());
  }
  auto in = doc_in.Reopen();
  SDB_ASSERT(in);
  in->Seek(meta.doc_start);
  return in;
}

}  // namespace irs
