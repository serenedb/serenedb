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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "iresearch/index/docs_mask/base.hpp"
#include "iresearch/index/docs_mask/chunks.hpp"
#include "iresearch/index/docs_mask/kernels.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DocsMaskFactory;

namespace docs_mask {

template<MaskKind K, typename Chunk, template<typename> class Layout>
class Typed : public Chunked<DocsMask<K>, Chunk, Layout<Chunk>> {
  using Base = Chunked<DocsMask<K>, Chunk, Layout<Chunk>>;

 public:
  static constexpr MaskKind kKind = K;
  static constexpr bool kSkipsSpans = true;

 protected:
  Typed(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : Base{mask, visible_end} {}
};

template<MaskKind K, template<typename> class Layout>
class DirectTyped
  : public Chunked<DocsMask<K>, BitsetChunk, Layout<BitsetChunk>> {
  using Base = Chunked<DocsMask<K>, BitsetChunk, Layout<BitsetChunk>>;

 public:
  static constexpr MaskKind kKind = K;
  static constexpr bool kSkipsSpans = false;

  using Base::Remove;

  IRS_FORCE_INLINE bool Test(doc_id_t doc) noexcept {
    return _direct.Test(doc);
  }

  template<typename Fn>
  IRS_FORCE_INLINE auto WithBlockTest(doc_id_t first, doc_id_t last, Fn&& fn) {
    return _direct.WithBlockTest(first, last, std::forward<Fn>(fn));
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t doc) const noexcept {
    return _direct.Probe(doc);
  }

  IRS_FORCE_INLINE doc_id_t NextLive(doc_id_t doc) const noexcept {
    return _direct.NextLive(doc);
  }

  void Remove(doc_id_t min, doc_id_t max,
              uint64_t* IRS_RESTRICT words) noexcept {
    this->template Apply<true>(min, max, words);
  }

 protected:
  DirectTyped(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : Base{mask, visible_end}, _direct{mask, visible_end} {}

 private:
  DirectWords<Layout<BitsetChunk>> _direct;
};

}  // namespace docs_mask

template<>
class DocsMask<MaskKind::Bitsets> final
  : public docs_mask::DirectTyped<MaskKind::Bitsets, docs_mask::DenseLayout> {
  friend class DocsMaskFactory;
  using DirectTyped::DirectTyped;
};

template<>
class DocsMask<MaskKind::Arrays> final
  : public docs_mask::Typed<MaskKind::Arrays, docs_mask::ArrayChunk,
                            docs_mask::GappedLayout> {
  friend class DocsMaskFactory;
  using Typed::Typed;
};

template<>
class DocsMask<MaskKind::Runs> final
  : public docs_mask::Typed<MaskKind::Runs, docs_mask::RunChunk,
                            docs_mask::GappedLayout> {
  friend class DocsMaskFactory;
  using Typed::Typed;
};

template<>
class DocsMask<MaskKind::Mixed> final
  : public docs_mask::Typed<MaskKind::Mixed, docs_mask::MixedChunk,
                            docs_mask::GappedLayout> {
  friend class DocsMaskFactory;
  using Typed::Typed;
};

static_assert(DocsMaskType<DocsMask<MaskKind::Bitsets>>);
static_assert(DocsMaskType<DocsMask<MaskKind::Arrays>>);
static_assert(DocsMaskType<DocsMask<MaskKind::Runs>>);
static_assert(DocsMaskType<DocsMask<MaskKind::Mixed>>);

using GenericDocsMask = DocsMask<MaskKind::Mixed>;

class DocsMaskFactory {
 public:
  template<typename Make>
  static decltype(auto) Resolve(const DocumentMask* mask, doc_id_t visible_end,
                                Make&& make) {
    if (mask == nullptr || mask->Empty()) {
      return make(DocsMask<MaskKind::Runs>{nullptr, visible_end});
    }
    switch (mask->Kind()) {
      case MaskKind::Bitsets:
        return make(DocsMask<MaskKind::Bitsets>{mask, visible_end});
      case MaskKind::Arrays:
        return make(DocsMask<MaskKind::Arrays>{mask, visible_end});
      case MaskKind::Runs:
        return make(DocsMask<MaskKind::Runs>{mask, visible_end});
      case MaskKind::Mixed:
        break;
    }
    return make(DocsMask<MaskKind::Mixed>{mask, visible_end});
  }

  static GenericDocsMask Generic(const DocumentMask* mask,
                                 doc_id_t visible_end) noexcept {
    return GenericDocsMask{mask, visible_end};
  }
};

template<typename Make>
decltype(auto) ResolveDocsMask(const DocumentMask* mask, doc_id_t visible_end,
                               Make&& make) {
  return DocsMaskFactory::Resolve(mask, visible_end, std::forward<Make>(make));
}

template<typename Make>
decltype(auto) ResolveDocsMask(const SubReader& segment, Make&& make) {
  return DocsMaskFactory::Resolve(
    segment.docs_mask(), segment.Meta().visible_end, std::forward<Make>(make));
}

inline GenericDocsMask MakeGenericDocsMask(const DocumentMask* mask,
                                           doc_id_t visible_end) noexcept {
  return DocsMaskFactory::Generic(mask, visible_end);
}

inline GenericDocsMask MakeGenericDocsMask(const SubReader& segment) noexcept {
  return DocsMaskFactory::Generic(segment.docs_mask(),
                                  segment.Meta().visible_end);
}

inline doc_id_t LiveEnd(const SubReader& segment) noexcept {
  return static_cast<doc_id_t>(doc_limits::min() + segment.docs_count());
}

template<typename Fn>
void VisitLiveRanges(const DocumentMask* mask, doc_id_t visible_end,
                     doc_id_t begin, doc_id_t end, Fn&& fn) {
  ResolveDocsMask(mask, visible_end, [&]<DocsMaskType Mask>(Mask docs_mask) {
    docs_mask.VisitLiveRanges(begin, end, fn);
  });
}

}  // namespace irs
