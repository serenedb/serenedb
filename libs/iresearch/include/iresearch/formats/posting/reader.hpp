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

#include "basics/debugging.h"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/stream.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

inline void PrepareInput(std::string& str, IndexInput::ptr& in, IOAdvice advice,
                         const ReaderState& state, std::string_view ext,
                         std::string_view format) {
  SDB_ASSERT(!in);
  irs::FileName(str, state.meta->name, ext);
  in = state.dir->open(str, advice);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", str)};
  }

  format_utils::CheckHeader(*in, format);
}

inline constexpr IndexFeatures kPos = IndexFeatures::Freq | IndexFeatures::Pos;

class PostingsReaderBase : public PostingsReader {
 public:
  PostingsHandles Handles() const noexcept final {
    return {.doc = _doc_in.get(), .pos = _pos_in.get(), .pay = _pay_in.get()};
  }

  uint64_t CountMappedMemory() const final {
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

  void prepare(DataInput& in, const ReaderState& state,
               IndexFeatures features) final;

  size_t decode(const byte_type* in, IndexFeatures field_features,
                PostingMeta& state) final;

  std::unique_ptr<IndexInput> ReopenPayload() const final {
    return _pay_in ? _pay_in->Reopen() : nullptr;
  }

 protected:
  explicit PostingsReaderBase(size_t block_size) noexcept
    : _block_size{block_size} {}

  ScorerPtr _scorer;
  IndexInput::ptr _doc_in;
  IndexInput::ptr _pos_in;
  IndexInput::ptr _pay_in;
  size_t _block_size;
  doc_id_t _docs_count = 0;
};

inline void PostingsReaderBase::prepare(DataInput& in, const ReaderState& state,
                                        IndexFeatures features) {
  std::string buf;

  const bool needs_pay =
    IndexFeatures::None !=
    (features & (IndexFeatures::Offs | IndexFeatures::Vec));

  // prepare document input
  PrepareInput(buf, _doc_in, IOAdvice::RANDOM, state,
               PostingsWriterBase::kDocExt, PostingsWriterBase::kDocFormatName);

  // Since terms doc postings too large
  //  it is too costly to verify checksum of
  //  the entire file. Here we perform cheap
  //  error detection which could recognize
  //  some forms of corruption.
  format_utils::ReadChecksum(*_doc_in);

  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    /* prepare positions input */
    PrepareInput(buf, _pos_in, IOAdvice::RANDOM, state,
                 PostingsWriterBase::kPosExt,
                 PostingsWriterBase::kPosFormatName);

    // Since terms pos postings too large
    // it is too costly to verify checksum of
    // the entire file. Here we perform cheap
    // error detection which could recognize
    // some forms of corruption.
    format_utils::ReadChecksum(*_pos_in);
  }

  if (needs_pay) {
    PrepareInput(buf, _pay_in, IOAdvice::RANDOM, state,
                 PostingsWriterBase::kPayExt,
                 PostingsWriterBase::kPayFormatName);

    // Since terms pos postings too large
    // it is too costly to verify checksum of
    // the entire file. Here we perform cheap
    // error detection which could recognize
    // some forms of corruption.
    format_utils::ReadChecksum(*_pay_in);
  }

  const uint64_t block_size = in.ReadV32();

  if (block_size != _block_size) {
    throw IndexError{
      absl::StrCat("while preparing postings_reader, error: "
                   "invalid block size '",
                   block_size, "', expected '", _block_size, "'")};
  }

  _scorer = state.scorer;
  _docs_count = state.meta->docs_count;
}

inline size_t PostingsReaderBase::decode(const byte_type* in,
                                         IndexFeatures features,
                                         PostingMeta& posting_meta) {
  const auto* p = in;

  SDB_ASSERT(IndexFeatures::None == (features & IndexFeatures::Vec) ||
             IndexFeatures::None ==
               (features & (IndexFeatures::Pos | IndexFeatures::Offs)));

  posting_meta.docs_count = vread<uint32_t>(p);
  if (IndexFeatures::None != (features & IndexFeatures::Freq)) {
    posting_meta.freq = posting_meta.docs_count + vread<uint32_t>(p);
  }

  posting_meta.doc_start += vread<uint64_t>(p);
  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    posting_meta.pos_start += vread<uint64_t>(p);
    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      posting_meta.pay_start += vread<uint64_t>(p);
    }
    posting_meta.pos_offset = *p++;
  } else if (IndexFeatures::None != (features & IndexFeatures::Vec)) {
    posting_meta.pay_start += vread<uint64_t>(p);
    posting_meta.pos_offset = *p++;
  }

  if (1 == posting_meta.docs_count || _block_size < posting_meta.docs_count) {
    posting_meta.doc_delta = vread<uint32_t>(p);
  }

  SDB_ASSERT(p >= in);
  return size_t(std::distance(in, p));
}

template<typename FormatTraits>
class PostingsReaderImpl final : public PostingsReaderBase {
 public:
  template<bool Freq, bool Pos, bool Offs>
  using IteratorTraits = IteratorTraitsImpl<FormatTraits, Freq, Pos, Offs>;

  PostingsReaderImpl() noexcept : PostingsReaderBase{doc_limits::kBlockSize} {}

  size_t BitUnion(IndexFeatures field, TermProvider provider, uint64_t* set,
                  bool has_score_bounds) final;

  TermPostings::ptr Postings(IndexFeatures field_features,
                             IndexFeatures required_features,
                             const PostingMeta& meta,
                             bool has_score_bounds) const final;

 private:
  template<typename FieldTraits, typename Factory>
  static auto IteratorImpl(IndexFeatures enabled, Factory&& factory);

  template<typename Factory>
  static auto IteratorImpl(IndexFeatures field_features,
                           IndexFeatures required_features, Factory&& factory);
};

template<typename FieldTraits>
void BitUnionImpl(DataInput& doc_in, doc_id_t docs_count, doc_id_t* docs,
                  uint32_t* enc_buf, uint64_t* words) {
  auto read_leaf = [&]<size_t N>(uint32_t len, doc_id_t prev) IRS_FORCE_INLINE {
    const auto leaf =
      FieldTraits::ReadTailForFill(len, doc_in, enc_buf, docs, prev);
    if (leaf.IsRun()) {
      const uint64_t first = uint64_t{prev} + 1;
      SetBitRange(words, first, first + len);
    } else if (leaf.IsBitset()) {
      OrBitsetAt(words, prev, leaf.bitset, leaf.words);
    } else {
      static constexpr auto kBits = BitsRequired<uint64_t>();
      const auto* const data = docs + doc_limits::kBlockSize - len;
      VisitDocs<N>(len, [&](uint32_t i) IRS_FORCE_INLINE {
        const size_t offset = data[i];
        SetBit(words[offset / kBits], offset % kBits);
      });
    }
    if constexpr (FieldTraits::Frequency()) {
      if (len == doc_limits::kBlockSize) {
        FieldTraits::SkipBlock(doc_in);
      }
    }
    return leaf.max;
  };

  auto prev_doc = doc_limits::invalid();
  for (auto blocks = docs_count / doc_limits::kBlockSize; blocks--;) {
    prev_doc = read_leaf.template operator()<doc_limits::kBlockSize>(
      doc_limits::kBlockSize, prev_doc);
  }

  if (const auto tail = docs_count % doc_limits::kBlockSize; tail != 0) {
    read_leaf.template operator()<std::dynamic_extent>(tail, prev_doc);
  }
}

template<typename FormatTraits>
size_t PostingsReaderImpl<FormatTraits>::BitUnion(
  const IndexFeatures field_features, TermProvider provider, uint64_t* set,
  bool has_score_bounds) {
  constexpr auto kBits{BitsRequired<std::remove_pointer_t<decltype(set)>>()};
  uint32_t enc_buf[doc_limits::kBlockSize];
  doc_id_t docs[doc_limits::kBlockSize
#ifdef __AVX2__
                + 8  // placeholder for bitset materialize
#endif
  ];
  const bool has_freq =
    IndexFeatures::None != (field_features & IndexFeatures::Freq);

  SDB_ASSERT(_doc_in);
  auto doc_in = _doc_in->Reopen();

  if (!doc_in) {
    // implementation returned wrong pointer
    SDB_ERROR(IRESEARCH, "Failed to reopen document input");

    throw IoError("failed to reopen document input");
  }

  size_t count = 0;
  while (const PostingMeta* meta = provider()) {
    const auto& term_state = *meta;

    if (term_state.docs_count > 1) {
      doc_in->Seek(term_state.doc_start);
      SDB_ASSERT(!doc_in->IsEOF());
      if (term_state.docs_count < doc_limits::kBlockSize) {
        SkipScoreBounds(has_score_bounds, *doc_in);
      }
      SDB_ASSERT(!doc_in->IsEOF());

      if (has_freq) {
        using FieldTraits = IteratorTraits<true, false, false>;
        BitUnionImpl<FieldTraits>(*doc_in, term_state.docs_count, docs, enc_buf,
                                  set);
      } else {
        using FieldTraits = IteratorTraits<false, false, false>;
        BitUnionImpl<FieldTraits>(*doc_in, term_state.docs_count, docs, enc_buf,
                                  set);
      }

      count += term_state.docs_count;
    } else {
      const doc_id_t doc = doc_limits::min() + term_state.doc_delta;
      SetBit(set[doc / kBits], doc % kBits);

      ++count;
    }
  }

  return count;
}

template<typename FormatTraits>
template<typename FieldTraits, typename Factory>
auto PostingsReaderImpl<FormatTraits>::IteratorImpl(IndexFeatures enabled,
                                                    Factory&& factory) {
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

template<typename FormatTraits>
template<typename Factory>
auto PostingsReaderImpl<FormatTraits>::IteratorImpl(
  IndexFeatures field_features, IndexFeatures required_features,
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

template<typename FormatTraits>
TermPostings::ptr PostingsReaderImpl<FormatTraits>::Postings(
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
