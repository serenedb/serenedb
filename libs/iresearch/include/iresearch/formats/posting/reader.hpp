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
#include "iresearch/formats/posting/iterator_doc.hpp"
#include "iresearch/formats/posting/iterator_score.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/search/block_disjunction.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/max_score_iterator.hpp"
#include "iresearch/store/store_utils.hpp"
#include "pg/sql_exception_macro.h"

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

template<typename PostingImpl>
struct PruningPostingAdapter : PostingAdapter<PostingImpl> {
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

  void SetSkipBoundsBelow(doc_id_t max) noexcept {
    this->self().SetSkipBoundsBelow(max);
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
    (features & (IndexFeatures::Offs | IndexFeatures::Pay));

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

  SDB_ASSERT(IndexFeatures::None == (features & IndexFeatures::Offs) ||
             IndexFeatures::None == (features & IndexFeatures::Pay));

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
  }
  if (IndexFeatures::None != (features & IndexFeatures::Pay)) {
    posting_meta.pay_start += vread<uint64_t>(p);
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

  DocIterator::ptr Iterator(IndexFeatures field_features,
                            IndexFeatures required_features,
                            std::span<const PostingCookie> metas,
                            IteratorFieldOptions options, size_t min_match,
                            ScoreMergeType type) const final;

 private:
  DocIterator::ptr PruningIterator(IndexFeatures field_features,
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
                  uint32_t* enc_buf, uint64_t* words) {
  auto read_leaf = [&](uint32_t len, doc_id_t prev) IRS_FORCE_INLINE {
    const auto leaf =
      FieldTraits::ReadTailForFill(len, doc_in, enc_buf, docs, prev);
    if constexpr (FieldTraits::Frequency()) {
      if (len == doc_limits::kBlockSize) {
        FieldTraits::SkipBlock(doc_in);
      }
    }
    if (leaf.IsRun()) {
      const uint64_t first = uint64_t{prev} + 1;
      SetBitRange(words, first, first + len);
    } else if (leaf.IsBitset()) {
      OrBitsetAt(words, prev, leaf.bitset, leaf.words);
    } else {
      OrDocs(words, std::span{docs + doc_limits::kBlockSize - len, len}, 0);
    }
    return leaf.max;
  };

  auto prev_doc = doc_limits::invalid();
  for (auto blocks = docs_count / doc_limits::kBlockSize; blocks--;) {
    prev_doc = read_leaf(doc_limits::kBlockSize, prev_doc);
  }

  if (const auto tail = docs_count % doc_limits::kBlockSize; tail != 0) {
    read_leaf(tail, prev_doc);
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

auto ResolveScoreBoundFeatures(IndexFeatures field_features, auto&& f) {
  switch (ToIndex(field_features)) {
    case kPosOffs:
      return f.template operator()<true, true>();
    case kPos:
      return f.template operator()<true, false>();
    default:
      return f.template operator()<false, false>();
  }
}

auto ResolveHasScoreBounds(bool has_score_bounds, auto&& f) {
  if (has_score_bounds) {
    return f.template operator()<true>();
  } else {
    return f.template operator()<false>();
  }
}

auto ResolveScoreBoundType(IndexFeatures field_features, bool has_score_bounds,
                           DataInput::Type type, auto&& f) {
  return ResolveScoreBoundFeatures(
    field_features, [&]<bool Pos, bool Offs> -> DocIterator::ptr {
      return ResolveHasScoreBounds(
        has_score_bounds, [&]<bool HasScoreBounds>() {
          return ResolveInputType(
            type, [&]<typename InputType> -> DocIterator::ptr {
              return f
                .template operator()<Pos, Offs, HasScoreBounds, InputType>();
            });
        });
    });
}

template<typename FormatTraits>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::PruningIterator(
  IndexFeatures field_features, std::span<const PostingCookie> metas,
  IteratorFieldOptions options, ScoreMergeType type) const {
  SDB_IF_FAILURE("irs::PruningIterator") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }

  return ResolveScoreBoundType(
    field_features, options.has_score_bounds, _doc_in->GetType(),
    [&]<bool Pos, bool Offs, bool HasScoreBounds, typename InputType>()
      -> DocIterator::ptr {
      auto make_postings_iterator =
        [&]<bool Root>(const PostingCookie& cookie) {
          auto it = memory::make_managed<
            SinglePruningIterator<FormatTraits, Root, Pos, Offs, InputType>>();
          it->Prepare(cookie, _doc_in.get());
          return it;
        };

      if (metas.size() == 1) {
        SDB_IF_FAILURE("irs::SinglePruningIterator") {
          THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
        }
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
        SinglePruningIterator<FormatTraits, false, Pos, Offs, InputType>;
      using Adapter = PruningPostingAdapter<Iterator>;

      SDB_IF_FAILURE("irs::MaxScoreIterator") {  //
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
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

  // Dispatch to PruningIterator when
  // (1) the caller asked for score pruning,
  // (2) the field has score bounds persisted,
  // (3) the field exposes Freq,
  // (4) the query doesn't need positional/offset data,
  // (5) min_match is 1.
  if (options.score_prune && options.has_score_bounds &&
      IndexFeatures::None != (field_features & IndexFeatures::Freq) &&
      IndexFeatures::None ==
        (required_features & (IndexFeatures::Pos | IndexFeatures::Offs)) &&
      min_match == 1) {
    return PruningIterator(field_features, metas, options, type);
  }

  auto make_postings_iterator = [&](const PostingCookie& cookie) {
    return IteratorImpl(
      field_features, required_features,
      [&]<typename IteratorTraits, typename FieldTraits> -> DocIterator::ptr {
        return ResolveBool(
          options.has_score_bounds,
          [&]<bool HasScoreBounds> -> DocIterator::ptr {
            if (_doc_in->GetType() == DataInput::Type::BytesViewInput) {
              auto it = memory::make_managed<PostingIteratorImpl<
                IteratorTraits, FieldTraits, HasScoreBounds, BytesViewInput>>();
              it->Prepare(cookie, _doc_in.get(), _pos_in.get(), _pay_in.get());
              return it;
            } else {
              auto it = memory::make_managed<PostingIteratorImpl<
                IteratorTraits, FieldTraits, HasScoreBounds, IndexInput>>();
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
          options.score_prune, _docs_count, std::move(adapters), min_match);
      });
    });
}

}  // namespace irs
