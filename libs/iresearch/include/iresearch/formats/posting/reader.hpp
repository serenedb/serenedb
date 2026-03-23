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
#include "iresearch/formats/posting/iterator_doc.hpp"
#include "iresearch/formats/posting/iterator_score.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/search/block_disjunction.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/max_score_iterator.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

inline void PrepareInput(std::string& str, IndexInput::ptr& in, IOAdvice advice,
                         const ReaderState& state, std::string_view ext,
                         std::string_view format, const int32_t min_ver,
                         const int32_t max_ver) {
  SDB_ASSERT(!in);
  irs::FileName(str, state.meta->name, ext);
  in = state.dir->open(str, advice);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", str)};
  }

  format_utils::CheckHeader(*in, format, min_ver, max_ver);
}

inline constexpr IndexFeatures kPos = IndexFeatures::Freq | IndexFeatures::Pos;

static_assert(kMaxScorers < WandContext::kDisable);

template<uint8_t Value>
struct Extent {
  IRS_FORCE_INLINE static constexpr uint8_t GetExtent() noexcept {
    return Value;
  }
};

template<>
struct Extent<WandContext::kDisable> {
  Extent(uint8_t value) noexcept : value{value} {}

  IRS_FORCE_INLINE constexpr uint8_t GetExtent() const noexcept {
    return value;
  }

  uint8_t value;
};

using DynamicExtent = Extent<WandContext::kDisable>;

template<uint8_t PossibleMin, typename Func>
auto ResolveExtent(uint8_t extent, Func&& func) {
  if constexpr (PossibleMin == WandContext::kDisable) {
    return std::forward<Func>(func)(Extent<0>{});
  } else {
    switch (extent) {
      case 1:
        return std::forward<Func>(func)(Extent<1>{});
      case 0:
        if constexpr (PossibleMin <= 0) {
          return std::forward<Func>(func)(Extent<0>{});
        }
        [[fallthrough]];
      default:
        return std::forward<Func>(func)(DynamicExtent{extent});
    }
  }
}

template<typename PostingImpl>
struct WandPostingAdapter : PostingAdapter<PostingImpl> {
  using PostingAdapter<PostingImpl>::PostingAdapter;

  IRS_FORCE_INLINE doc_id_t SeekToBlock(doc_id_t doc) {
    return this->self().ShallowSeekToBlock(doc);
  }

  IRS_FORCE_INLINE score_t GetMaxScore(doc_id_t doc) {
    return this->self().GetMaxScore(doc);
  }

  template<typename... Args>
  IRS_FORCE_INLINE void CollectRange(Args&&... args) {
    this->self().CollectRange(std::forward<Args>(args)...);
  }

  template<typename... Args>
  IRS_FORCE_INLINE auto ScoreCandidates(Args&&... args) {
    return this->self().ScoreCandidates(std::forward<Args>(args)...);
  }

  void SetSkipWandBelow(doc_id_t max) noexcept {
    this->self().SetSkipWandBelow(max);
  }
};

class PostingsReaderBase : public PostingsReader {
 public:
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
                TermMeta& state) final;

 protected:
  explicit PostingsReaderBase(size_t block_size) noexcept
    : _block_size{block_size} {}

  ScorersView _scorers;
  IndexInput::ptr _doc_in;
  IndexInput::ptr _pos_in;
  IndexInput::ptr _pay_in;
  size_t _block_size;
  doc_id_t _docs_count = 0;
};

inline void PostingsReaderBase::prepare(DataInput& in, const ReaderState& state,
                                        IndexFeatures features) {
  std::string buf;

  // prepare document input
  PrepareInput(buf, _doc_in, IOAdvice::RANDOM, state,
               PostingsWriterBase::kDocExt, PostingsWriterBase::kDocFormatName,
               static_cast<int32_t>(PostingsFormat::Min),
               static_cast<int32_t>(PostingsFormat::Max));

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
                 PostingsWriterBase::kPosFormatName,
                 static_cast<int32_t>(PostingsFormat::Min),
                 static_cast<int32_t>(PostingsFormat::Max));

    // Since terms pos postings too large
    // it is too costly to verify checksum of
    // the entire file. Here we perform cheap
    // error detection which could recognize
    // some forms of corruption.
    format_utils::ReadChecksum(*_pos_in);

    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      // prepare positions input
      PrepareInput(buf, _pay_in, IOAdvice::RANDOM, state,
                   PostingsWriterBase::kPayExt,
                   PostingsWriterBase::kPayFormatName,
                   static_cast<int32_t>(PostingsFormat::Min),
                   static_cast<int32_t>(PostingsFormat::Max));

      // Since terms pos postings too large
      // it is too costly to verify checksum of
      // the entire file. Here we perform cheap
      // error detection which could recognize
      // some forms of corruption.
      format_utils::ReadChecksum(*_pay_in);
    }
  }

  // check postings format
  format_utils::CheckHeader(in, PostingsWriterBase::kTermsFormatName,
                            static_cast<int32_t>(TermsFormat::Min),
                            static_cast<int32_t>(TermsFormat::Max));

  const uint64_t block_size = in.ReadV32();

  if (block_size != _block_size) {
    throw IndexError{
      absl::StrCat("while preparing postings_reader, error: "
                   "invalid block size '",
                   block_size, "', expected '", _block_size, "'")};
  }

  _scorers =
    state.scorers.subspan(0, std::min(state.scorers.size(), kMaxScorers));
  _docs_count = state.meta->docs_count;
}

inline size_t PostingsReaderBase::decode(const byte_type* in,
                                         IndexFeatures features,
                                         TermMeta& state) {
  auto& term_meta = static_cast<TermMetaImpl&>(state);
  const auto* p = in;

  term_meta.docs_count = vread<uint32_t>(p);
  if (IndexFeatures::None != (features & IndexFeatures::Freq)) {
    term_meta.freq = term_meta.docs_count + vread<uint32_t>(p);
  }

  term_meta.doc_start += vread<uint64_t>(p);
  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    term_meta.pos_start += vread<uint64_t>(p);
    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      term_meta.pay_start += vread<uint64_t>(p);
    }
    term_meta.pos_offset = *p++;
  }

  if (1 == term_meta.docs_count) {
    term_meta.e_single_doc = vread<uint32_t>(p);
  } else if (_block_size < term_meta.docs_count) {
    term_meta.e_skip_start = vread<uint64_t>(p);
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

  size_t BitUnion(IndexFeatures field, const term_provider_f& provider,
                  size_t* set, uint8_t wand_count) final;

  DocIterator::ptr Iterator(IndexFeatures field_features,
                            IndexFeatures required_features,
                            std::span<const PostingCookie> metas,
                            IteratorFieldOptions options, size_t min_match,
                            ScoreMergeType type) const final;

 private:
  DocIterator::ptr WandIterator(IndexFeatures field_features,
                                std::span<const PostingCookie> metas,
                                IteratorFieldOptions options,
                                ScoreMergeType type) const;

  template<typename FieldTraits, typename Factory>
  static DocIterator::ptr IteratorImpl(IndexFeatures enabled,
                                       Factory&& factory);

  template<typename Factory>
  static DocIterator::ptr IteratorImpl(IndexFeatures field_features,
                                       IndexFeatures required_features,
                                       Factory&& factory);
};

template<typename FieldTraits>
void BitUnionImpl(DataInput& doc_in, doc_id_t docs_count, doc_id_t* docs,
                  uint32_t* enc_buf, size_t* set) {
  constexpr auto kBits{BitsRequired<std::remove_pointer_t<decltype(set)>>()};
  size_t num_blocks = docs_count / doc_limits::kBlockSize;

  auto prev_doc = doc_limits::invalid();
  while (num_blocks--) {
    FieldTraits::ReadBlockDelta(doc_in, enc_buf, docs, prev_doc);
    if constexpr (FieldTraits::Frequency()) {
      FieldTraits::SkipBlock(doc_in);
    }

    // FIXME optimize
    for (const auto doc : std::span{docs, doc_limits::kBlockSize}) {
      SetBit(set[doc / kBits], doc % kBits);
    }
    prev_doc = docs[doc_limits::kBlockSize - 1];
  }

  const auto tail = docs_count % doc_limits::kBlockSize;
  if (tail == 0) {
    return;
  }
  FieldTraits::ReadTailDelta(tail, doc_in, enc_buf, docs, prev_doc);

  // FIXME optimize
  for (const auto doc : std::span{docs + doc_limits::kBlockSize - tail, tail}) {
    SetBit(set[doc / kBits], doc % kBits);
  }
}

template<typename FormatTraits>
size_t PostingsReaderImpl<FormatTraits>::BitUnion(
  const IndexFeatures field_features, const term_provider_f& provider,
  size_t* set, uint8_t wand_count) {
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
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Failed to reopen document input");

    throw IoError("failed to reopen document input");
  }

  size_t count = 0;
  while (const TermMeta* meta = provider()) {
    auto& term_state = static_cast<const TermMetaImpl&>(*meta);

    if (term_state.docs_count > 1) {
      doc_in->Seek(term_state.doc_start);
      SDB_ASSERT(!doc_in->IsEOF());
      if (term_state.docs_count < doc_limits::kBlockSize) {
        CommonSkipWandData(DynamicExtent{wand_count}, *doc_in);
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
      const doc_id_t doc = doc_limits::min() + term_state.e_single_doc;
      SetBit(set[doc / kBits], doc % kBits);

      ++count;
    }
  }

  return count;
}

template<typename FormatTraits>
template<typename FieldTraits, typename Factory>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::IteratorImpl(
  IndexFeatures enabled, Factory&& factory) {
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
DocIterator::ptr PostingsReaderImpl<FormatTraits>::IteratorImpl(
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

auto ResolveWandFeatures(IndexFeatures field_features, auto&& f) {
  switch (ToIndex(field_features)) {
    case kPosOffs:
      return f.template operator()<true, true>();
    case kPos:
      return f.template operator()<true, false>();
    default:
      return f.template operator()<false, false>();
  }
}

auto ResolveWandType(IndexFeatures field_features, uint8_t count,
                     DataInput::Type type, auto&& f) {
  return ResolveWandFeatures(
    field_features, [&]<bool Pos, bool Offs> -> DocIterator::ptr {
      return ResolveExtent<1>(count, [&]<typename Extent>(Extent&& extent) {
        return ResolveInputType(
          type, [&]<typename InputType> -> DocIterator::ptr {
            return f.template operator()<Pos, Offs, Extent, InputType>(
              std::forward<Extent>(extent));
          });
      });
    });
}

template<typename FormatTraits>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::WandIterator(
  IndexFeatures field_features, std::span<const PostingCookie> metas,
  IteratorFieldOptions options, ScoreMergeType type) const {
  return ResolveWandType(
    field_features, options.count, _doc_in->GetType(),
    [&]<bool Pos, bool Offs, typename Extent, typename InputType>(
      Extent&& extent) -> DocIterator::ptr {
      auto make_postings_iterator = [&]<bool Root>(
                                      const PostingCookie& cookie) {
        auto it = memory::make_managed<
          SingleWandIterator<FormatTraits, Root, Pos, Offs, Extent, InputType>>(
          std::forward<Extent>(extent));
        it->Prepare(cookie, _doc_in.get(), options.mapped_index);
        return it;
      };

      if (metas.size() == 1) {
        return make_postings_iterator.template operator()<true>(metas[0]);
      }

      std::vector<DocIterator::ptr> iterators;
      iterators.reserve(metas.size());
      for (const auto& meta : metas) {
        auto it = make_postings_iterator.template operator()<false>(meta);
        SDB_ASSERT(it);
        iterators.emplace_back(std::move(it));
      }

      using Iterator =
        SingleWandIterator<FormatTraits, false, Pos, Offs, Extent, InputType>;
      using Adapter = WandPostingAdapter<Iterator>;

      return memory::make_managed<MaxScoreIterator<Adapter>>(
        std::move(iterators));
    });
}

template<typename FormatTraits>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::Iterator(
  IndexFeatures field_features, IndexFeatures required_features,
  std::span<const PostingCookie> metas, IteratorFieldOptions options,
  size_t min_match, ScoreMergeType type) const {
  SDB_ASSERT(!metas.empty());
  SDB_ASSERT(1 <= min_match);
  SDB_ASSERT(min_match <= metas.size());

  if (metas.size() < min_match) {
    return {};
  }

  if (options.Enabled() &&
      IndexFeatures::None != (field_features & IndexFeatures::Freq) &&
      IndexFeatures::None ==
        (required_features & (IndexFeatures::Pos | IndexFeatures::Offs)) &&
      min_match == 1) {
    return WandIterator(field_features, metas, options, type);
  }

  auto make_postings_iterator = [&](const PostingCookie& cookie) {
    return IteratorImpl(
      field_features, required_features,
      [&]<typename IteratorTraits, typename FieldTraits> -> DocIterator::ptr {
        return ResolveExtent<0>(
          options.count,
          [&]<typename Extent>(Extent&& extent) -> DocIterator::ptr {
            if (_doc_in->GetType() == DataInput::Type::BytesViewInput) {
              auto it = memory::make_managed<PostingIteratorImpl<
                IteratorTraits, FieldTraits, Extent, BytesViewInput>>(
                std::forward<Extent>(extent));
              it->Prepare(cookie, _doc_in.get(), _pos_in.get(), _pay_in.get());
              return it;
            } else {
              auto it = memory::make_managed<PostingIteratorImpl<
                IteratorTraits, FieldTraits, Extent, IndexInput>>(
                std::forward<Extent>(extent));
              it->Prepare(cookie, _doc_in.get(), _pos_in.get(), _pay_in.get());
              return it;
            }
          });
      });
  };

  if (metas.size() == 1) {
    return make_postings_iterator(metas[0]);
  }

  std::vector<DocIterator::ptr> iterators;
  iterators.reserve(metas.size());
  for (const auto& meta : metas) {
    auto it = make_postings_iterator(meta);
    SDB_ASSERT(it);
    iterators.emplace_back(std::move(it));
  }

  return IteratorImpl(
    field_features, required_features,
    [&]<typename IteratorTraits, typename FieldTraits> -> DocIterator::ptr {
      using Adapter = PostingAdapter<PostingIteratorBase<IteratorTraits>>;
      std::vector<Adapter> adapters;
      adapters.reserve(iterators.size());
      for (auto& it : iterators) {
        adapters.emplace_back(std::move(it));
      }
      return ResolveMergeType(type, [&]<ScoreMergeType MergeType> {
        using MinMatchIterator = MinMatchIterator<Adapter, MergeType>;
        return MakeWeakDisjunction<MinMatchIterator>(
          options, _docs_count, std::move(adapters), min_match);
      });
    });
}

}  // namespace irs
