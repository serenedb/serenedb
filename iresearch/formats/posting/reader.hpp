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

#include <iresearch/utils/assert.hpp>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/stream.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

inline duckdb::shared_ptr<duckdb::InputFile> PrepareInput(
  const ReaderState& state, std::string_view ext) {
  const auto name = irs::FileName(state.meta.name, ext);
  auto file = state.fs.OpenInputFile(state.fs.JoinPath(state.root, name),
                                     duckdb::MappedFileAccess::NORMAL);
  format_utils::ReadFooter(*file, name);
  return file;
}

inline constexpr IndexFeatures kPos = IndexFeatures::Freq | IndexFeatures::Pos;

class PostingsReader final {
 public:
  PostingsHandles Handles() const noexcept {
    return {
      .doc = _doc_file.get(), .pos = _pos_file.get(), .pay = _pay_file.get()};
  }

  // features - the set of features available for segment
  void prepare(const ReaderState& state, IndexFeatures features);

  static size_t decode(const byte_type* in, IndexFeatures field_features,
                       PostingMeta& state);

  TermPostings::ptr Postings(IndexFeatures field_features,
                             IndexFeatures required_features,
                             const PostingMeta& meta, bool has_score_bounds,
                             TermPostings::ptr reuse = {}) const;

 private:
  template<typename FieldTraits, typename Factory>
  static auto IteratorImpl(IndexFeatures enabled, Factory&& factory);

  template<typename Factory>
  static auto IteratorImpl(IndexFeatures field_features,
                           IndexFeatures required_features, Factory&& factory);

  duckdb::shared_ptr<duckdb::InputFile> _doc_file;
  duckdb::shared_ptr<duckdb::InputFile> _pos_file;
  duckdb::shared_ptr<duckdb::InputFile> _pay_file;
};

inline void PostingsReader::prepare(const ReaderState& state,
                                    IndexFeatures features) {
  const bool needs_pay =
    IndexFeatures::None !=
    (features & (IndexFeatures::Offs | IndexFeatures::Vec));

  if (state.postings) {
    _doc_file = std::move(state.postings->doc);
    SDB_ASSERT(_doc_file);
    if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
      _pos_file = std::move(state.postings->pos);
      SDB_ASSERT(_pos_file);
    }
    if (needs_pay) {
      _pay_file = std::move(state.postings->pay);
      SDB_ASSERT(_pay_file);
    }
    return;
  }

  // prepare document input
  _doc_file = PrepareInput(state, PostingsWriter::kDocExt);

  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    _pos_file = PrepareInput(state, PostingsWriter::kPosExt);
  }

  if (needs_pay) {
    _pay_file = PrepareInput(state, PostingsWriter::kPayExt);
  }
}

inline size_t PostingsReader::decode(const byte_type* in,
                                     IndexFeatures features,
                                     PostingMeta& posting_meta) {
  const auto* p = in;

  SDB_ASSERT(IndexFeatures::None == (features & IndexFeatures::Vec) ||
             IndexFeatures::None ==
               (features & (IndexFeatures::Pos | IndexFeatures::Offs)));

  const bool has_freq = IsSubsetOf(IndexFeatures::Freq, features);
  const bool has_pos = IsSubsetOf(IndexFeatures::Pos, features);
  const uint64_t next = uint64_t{posting_meta.pos_offset} + posting_meta.freq;
  const auto layout = RecordLayoutOf(has_freq, has_pos);
  const auto head = vread<uint64_t>(p);
  const bool single = (head & 1) != 0;
  const bool follows = has_pos && (head >> layout.follows & 1) != 0;
  const auto payload = head >> layout.payload;
  if (single) {
    posting_meta.docs_count = 1;
    posting_meta.doc_delta = static_cast<uint32_t>(payload);
  } else {
    posting_meta.docs_count = static_cast<uint32_t>(payload);
  }
  if (has_freq) {
    posting_meta.freq =
      posting_meta.docs_count + ((head & 2) != 0 ? 0 : 1 + vread<uint32_t>(p));
  }

  if (!single) {
    posting_meta.doc_start += vread<uint64_t>(p);
  }
  if (has_pos) {
    if (!follows || next >= PosGroup::kPositions) {
      posting_meta.pos_start += vread<uint64_t>(p);
      if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
        posting_meta.pay_start += vread<uint64_t>(p);
      }
    }
    posting_meta.pos_offset = static_cast<uint16_t>(
      follows ? next % PosGroup::kPositions : vread<uint32_t>(p));
  } else if (IndexFeatures::None != (features & IndexFeatures::Vec)) {
    posting_meta.pay_start += vread<uint64_t>(p);
    posting_meta.pos_offset = static_cast<uint16_t>(vread<uint32_t>(p));
  }

  if (doc_limits::kBlockSize < posting_meta.docs_count) {
    posting_meta.doc_delta = vread<uint32_t>(p);
  }

  if (has_pos && pos_limits::kBlockSize < posting_meta.freq) {
    posting_meta.pos_extent = vread<uint32_t>(p);
    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      posting_meta.pay_extent = vread<uint32_t>(p);
    }
  }

  SDB_ASSERT(p >= in);
  return size_t(std::distance(in, p));
}

template<typename FieldTraits, typename Factory>
auto PostingsReader::IteratorImpl(IndexFeatures enabled, Factory&& factory) {
  switch (ToIndex(enabled)) {
    case kPosOffs: {
      using Traits = IteratorTraitsImpl<true, true, true>;
      if constexpr ((FieldTraits::Features() & Traits::Features()) ==
                    Traits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<Traits, FieldTraits>();
      }
    } break;
    case kPos: {
      using Traits = IteratorTraitsImpl<true, true, false>;
      if constexpr ((FieldTraits::Features() & Traits::Features()) ==
                    Traits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<Traits, FieldTraits>();
      }
    } break;
    case IndexFeatures::Freq: {
      using Traits = IteratorTraitsImpl<true, false, false>;
      if constexpr ((FieldTraits::Features() & Traits::Features()) ==
                    Traits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<Traits, FieldTraits>();
      }
    } break;
    default:
      break;
  }
  using Traits = IteratorTraitsImpl<false, false, false>;
  return std::forward<Factory>(factory)
    .template operator()<Traits, FieldTraits>();
}

template<typename Factory>
auto PostingsReader::IteratorImpl(IndexFeatures field_features,
                                  IndexFeatures required_features,
                                  Factory&& factory) {
  // get enabled features as the intersection
  // between requested and available features
  const auto enabled = field_features & required_features;

  switch (ToIndex(field_features)) {
    case kPosOffs: {
      using FieldTraits = IteratorTraitsImpl<true, true, true>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case kPos: {
      using FieldTraits = IteratorTraitsImpl<true, true, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case IndexFeatures::Freq: {
      using FieldTraits = IteratorTraitsImpl<true, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    default: {
      using FieldTraits = IteratorTraitsImpl<false, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
  }
}

inline TermPostings::ptr PostingsReader::Postings(
  IndexFeatures field_features, IndexFeatures required_features,
  const PostingMeta& meta, bool has_score_bounds,
  TermPostings::ptr reuse) const {
  if (meta.docs_count == 0) {
    return TermPostings::empty();
  }

  return IteratorImpl(
    field_features, required_features,
    [&]<typename IteratorTraits, typename FieldTraits> -> TermPostings::ptr {
      return Resolve(*_doc_file, [&]<typename InputType> -> TermPostings::ptr {
        using Stream = PostingsStream<IteratorTraits, FieldTraits, InputType>;
        if (!reuse) {
          reuse = memory::make_managed<Stream>();
        }
        SDB_ASSERT(dynamic_cast<Stream*>(reuse.get()) != nullptr);
        static_cast<Stream&>(*reuse).Prepare(meta, *_doc_file, _pos_file.get(),
                                             _pay_file.get(), has_score_bounds);
        return std::move(reuse);
      });
    });
}

}  // namespace irs
