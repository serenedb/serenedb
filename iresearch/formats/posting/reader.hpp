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

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/stream.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/debugging.hpp"

namespace irs {

inline void PrepareInput(std::string& str, IndexInput::ptr& in, IOAdvice advice,
                         const ReaderState& state, std::string_view ext) {
  SDB_ASSERT(!in);
  irs::FileName(str, state.meta->name, ext);
  in = state.dir->open(str, advice);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", str)};
  }

  format_utils::ReadFooter(*in, str);
}

inline constexpr IndexFeatures kPos = IndexFeatures::Freq | IndexFeatures::Pos;

class PostingsReader final {
 public:
  template<bool Freq, bool Pos, bool Offs>
  using IteratorTraits = IteratorTraitsImpl<FormatTraits128, Freq, Pos, Offs>;

  PostingsHandles Handles() const noexcept {
    return {.doc = _doc_in.get(), .pos = _pos_in.get(), .pay = _pay_in.get()};
  }

  uint64_t CountMappedMemory() const {
    uint64_t bytes = 0;
    if (_doc_in != nullptr) {
      bytes += _doc_in->CountMappedMemory();
    }
    if (_pos_in != nullptr) {
      bytes += _pos_in->CountMappedMemory();
    }
    if (_pay_in != nullptr) {
      bytes += _pay_in->CountMappedMemory();
    }
    return bytes;
  }

  // features - the set of features available for segment
  void prepare(const ReaderState& state, IndexFeatures features);

  size_t decode(const byte_type* in, IndexFeatures field_features,
                PostingMeta& state);

  // One term's whole posting list as the write side reads it: front to back,
  // with the frequency and the positions the field stores. Nothing here
  // seeks, so no skip list is parsed. `required_features` narrows what is
  // decoded; what the field carries beyond that is stepped over.
  TermPostings::ptr Postings(IndexFeatures field_features,
                             IndexFeatures required_features,
                             const PostingMeta& meta,
                             bool has_score_bounds) const;

  std::unique_ptr<IndexInput> ReopenPayload() const {
    return _pay_in ? _pay_in->Reopen() : nullptr;
  }

 private:
  template<typename FieldTraits, typename Factory>
  static auto IteratorImpl(IndexFeatures enabled, Factory&& factory);

  template<typename Factory>
  static auto IteratorImpl(IndexFeatures field_features,
                           IndexFeatures required_features, Factory&& factory);

  IndexInput::ptr _doc_in;
  IndexInput::ptr _pos_in;
  IndexInput::ptr _pay_in;
};

inline void PostingsReader::prepare(const ReaderState& state,
                                    IndexFeatures features) {
  std::string buf;

  const bool needs_pay =
    IndexFeatures::None !=
    (features & (IndexFeatures::Offs | IndexFeatures::Vec));

  // prepare document input
  PrepareInput(buf, _doc_in, IOAdvice::RANDOM, state, PostingsWriter::kDocExt);
  _doc_in->EnableReadahead();

  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    PrepareInput(buf, _pos_in, IOAdvice::RANDOM, state,
                 PostingsWriter::kPosExt);
    _pos_in->EnableReadahead();
  }

  if (needs_pay) {
    PrepareInput(buf, _pay_in, IOAdvice::RANDOM, state,
                 PostingsWriter::kPayExt);
    _pay_in->EnableReadahead();
  }
}

inline size_t PostingsReader::decode(const byte_type* in,
                                     IndexFeatures features,
                                     PostingMeta& posting_meta) {
  const auto* p = in;

  SDB_ASSERT(IndexFeatures::None == (features & IndexFeatures::Vec) ||
             IndexFeatures::None ==
               (features & (IndexFeatures::Pos | IndexFeatures::Offs)));

  const auto head = vread<uint32_t>(p);
  posting_meta.docs_count = head >> 1;
  if (IndexFeatures::None != (features & IndexFeatures::Freq)) {
    posting_meta.freq = posting_meta.docs_count + vread<uint32_t>(p);
  }

  if ((head & 1) != 0) {
    const auto size = *p++;
    SDB_ASSERT(size != 0 && size <= PostingMeta::kInlineBytes);
    posting_meta.inline_size = size;
    std::memcpy(posting_meta.inline_data, p, size);
    p += size;
  } else {
    posting_meta.inline_size = 0;
    posting_meta.doc_start += vread<uint64_t>(p);
  }
  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    const auto pos_delta = vread<uint64_t>(p);
    posting_meta.pos_start += pos_delta;
    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      posting_meta.pay_start += vread<uint64_t>(p);
    }
    const auto pos_offset = vread<uint32_t>(p);
    posting_meta.pos_offset =
      pos_delta == 0 ? posting_meta.pos_offset + pos_offset : pos_offset;
  } else if (IndexFeatures::None != (features & IndexFeatures::Vec)) {
    posting_meta.pay_start += vread<uint64_t>(p);
    posting_meta.pos_offset = *p++;
  }

  if (1 == posting_meta.docs_count ||
      doc_limits::kBlockSize < posting_meta.docs_count) {
    posting_meta.doc_delta = vread<uint32_t>(p);
  }

  SDB_ASSERT(p >= in);
  return size_t(std::distance(in, p));
}

template<typename FieldTraits, typename Factory>
auto PostingsReader::IteratorImpl(IndexFeatures enabled, Factory&& factory) {
  switch (ToIndex(enabled)) {
    case kPosOffs: {
      using IteratorTraits = IteratorTraits<true, true, true>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case kPos: {
      using IteratorTraits = IteratorTraits<true, true, false>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case IndexFeatures::Freq: {
      using IteratorTraits = IteratorTraits<true, false, false>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    default:
      break;
  }
  using IteratorTraits = IteratorTraits<false, false, false>;
  return std::forward<Factory>(factory)
    .template operator()<IteratorTraits, FieldTraits>();
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
      using FieldTraits = IteratorTraits<true, true, true>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case kPos: {
      using FieldTraits = IteratorTraits<true, true, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case IndexFeatures::Freq: {
      using FieldTraits = IteratorTraits<true, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    default: {
      using FieldTraits = IteratorTraits<false, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
  }
}

auto ResolveInputType(DataInput::Type type, auto&& f) {
  if (type == DataInput::Type::BytesViewInput) {
    return f.template operator()<BytesViewInput>();
  } else {
    return f.template operator()<IndexInput>();
  }
}

inline TermPostings::ptr PostingsReader::Postings(
  IndexFeatures field_features, IndexFeatures required_features,
  const PostingMeta& meta, bool has_score_bounds) const {
  if (meta.docs_count == 0) {
    return TermPostings::empty();
  }

  return IteratorImpl(
    field_features, required_features,
    [&]<typename IteratorTraits, typename FieldTraits> -> TermPostings::ptr {
      return ResolveInputType(
        _doc_in->GetType(), [&]<typename InputType> -> TermPostings::ptr {
          auto it = memory::make_managed<
            PostingsStream<IteratorTraits, FieldTraits, InputType>>();
          it->Prepare(meta, *_doc_in, _pos_in.get(), _pay_in.get(),
                      has_score_bounds);
          return it;
        });
    });
}

}  // namespace irs
