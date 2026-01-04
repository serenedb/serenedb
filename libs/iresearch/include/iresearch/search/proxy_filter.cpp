////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "proxy_filter.hpp"

#include <absl/synchronization/mutex.h>

#include <bit>

#include "basics/containers/bitset.hpp"
#include "cost.hpp"
#include "iresearch/index/index_reader.hpp"  // for full definition of SubReader

namespace irs {

// Bitset expecting doc iterator to be able only to move forward.
// So in case of "seek" to the still unfilled word
// internally it does bunch of "next" calls.
class LazyFilterBitset : private util::Noncopyable {
 public:
  using WordT = size_t;

  LazyFilterBitset(const ExecutionContext& ctx, const Filter::Query& filter)
    : _manager{ctx.memory} {
    const size_t bits = ctx.segment.docs_count() + doc_limits::min();
    _words = bitset::bits_to_words(bits);

    auto bytes = sizeof(*this) + sizeof(WordT) * _words;
    _manager.Increase(bytes);
    Finally decrease = [&]() noexcept { ctx.memory.DecreaseChecked(bytes); };

    // TODO(mbkkt) use mask from segment manually to avoid virtual call
    _real_doc_itr = ctx.segment.mask(filter.execute(ctx));

    _cost = CostAttr::extract(*_real_doc_itr);

    _set = std::allocator<WordT>{}.allocate(_words);
    std::memset(_set, 0, sizeof(WordT) * _words);
    _begin = _set;
    _end = _begin;

    bytes = 0;
  }

  ~LazyFilterBitset() {
    std::allocator<WordT>{}.deallocate(_set, _words);
    _manager.Decrease(sizeof(*this) + sizeof(WordT) * _words);
  }

  bool Get(size_t word_idx, WordT* data) {
    constexpr auto kBits{BitsRequired<WordT>()};
    SDB_ASSERT(_set);
    if (word_idx >= _words) {
      return false;
    }

    const auto* const requested = _set + word_idx;
    if (requested >= _end) {
      const auto block_limit = ((word_idx + 1) * kBits) - 1;
      while (true) {
        const auto doc_id = _real_doc_itr->advance();
        if (doc_limits::eof(doc_id)) {
          break;
        }
        SetBit(_set[doc_id / kBits], doc_id % kBits);
        if (doc_id >= block_limit) {
          break;  // we've filled requested word
        }
      }
      _end = requested + 1;
    }
    *data = *requested;
    return true;
  }

  CostAttr::Type GetCost() const noexcept { return _cost; }

 private:
  IResourceManager& _manager;

  DocIterator::ptr _real_doc_itr;
  CostAttr::Type _cost;

  WordT* _set{nullptr};
  size_t _words{0};
  const WordT* _begin{nullptr};
  const WordT* _end{nullptr};
};

class LazyFilterBitsetIterator : public DocIterator, private util::Noncopyable {
 public:
  explicit LazyFilterBitsetIterator(LazyFilterBitset& bitset) noexcept
    : _bitset(bitset), _cost(_bitset.GetCost()) {
    Reset();
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    if (Type<DocAttr>::id() == id) {
      return &_doc;
    }
    return Type<CostAttr>::id() == id ? &_cost : nullptr;
  }

  doc_id_t value() const noexcept final { return _doc.value; }

  doc_id_t advance() final {
    while (!_word) {
      if (_bitset.Get(_word_idx, &_word)) {
        ++_word_idx;  // move only if ok. Or we could be overflowed!
        _base += BitsRequired<LazyFilterBitset::WordT>();
        _doc.value = _base - 1;
        continue;
      }
      _word = 0;
      return _doc.value = doc_limits::eof();
    }
    const auto delta = std::countr_zero(_word);
    SDB_ASSERT(0 <= delta);
    SDB_ASSERT(delta < BitsRequired<LazyFilterBitset::WordT>());
    _word = (_word >> delta) >> 1;
    return _doc.value += 1 + delta;
  }

  doc_id_t seek(doc_id_t target) final {
    _word_idx = target / BitsRequired<LazyFilterBitset::WordT>();
    if (_bitset.Get(_word_idx, &_word)) {
      const doc_id_t bit_idx = target % BitsRequired<LazyFilterBitset::WordT>();
      _base = _word_idx * BitsRequired<LazyFilterBitset::WordT>();
      _word >>= bit_idx;
      _doc.value = _base - 1 + bit_idx;
      ++_word_idx;  // mark this word as consumed
      // FIXME consider inlining to speedup
      return advance();
    } else {
      _doc.value = doc_limits::eof();
      _word = 0;
      return _doc.value;
    }
  }

  uint32_t count() final {
    // TODO(mbkkt) custom implementation?
    return Count(*this);
  }

  void Reset() noexcept {
    _word_idx = 0;
    _word = 0;
    // before the first word
    _base = doc_limits::invalid() - BitsRequired<LazyFilterBitset::WordT>();
    _doc.value = doc_limits::invalid();
  }

 private:
  LazyFilterBitset& _bitset;
  CostAttr _cost;
  DocAttr _doc;
  doc_id_t _word_idx{0};
  LazyFilterBitset::WordT _word{0};
  doc_id_t _base{doc_limits::invalid()};
};

struct ProxyQueryCache {
  ProxyQueryCache(IResourceManager& memory, Filter::ptr&& ptr) noexcept
    : real_filter{std::move(ptr)}, readers{Alloc{memory}} {}

  using Alloc = ManagedTypedAllocator<
    std::pair<const SubReader* const, std::unique_ptr<LazyFilterBitset>>>;

  Filter::ptr real_filter;
  Filter::Query::ptr real_filter_prepared;
  absl::Mutex readers_lock;
  absl::flat_hash_map<
    const SubReader*, std::unique_ptr<LazyFilterBitset>,
    absl::container_internal::hash_default_hash<const SubReader*>,
    absl::container_internal::hash_default_eq<const SubReader*>, Alloc>
    readers;
};

class ProxyQuery : public Filter::Query {
 public:
  explicit ProxyQuery(ProxyFilter::cache_ptr cache) : _cache{cache} {
    SDB_ASSERT(_cache);
    SDB_ASSERT(_cache->real_filter_prepared);
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto* cache_bitset = [&] -> LazyFilterBitset* {
      absl::ReaderMutexLock lock{&_cache->readers_lock};
      auto it = _cache->readers.find(&ctx.segment);
      if (it != _cache->readers.end()) {
        return it->second.get();
      }
      return nullptr;
    }();
    if (!cache_bitset) {
      auto bitset =
        std::make_unique<LazyFilterBitset>(ctx, *_cache->real_filter_prepared);
      cache_bitset = bitset.get();
      absl::WriterMutexLock lock{&_cache->readers_lock};
      SDB_ASSERT(!_cache->readers.contains(&ctx.segment));
      _cache->readers.emplace(&ctx.segment, std::move(bitset));
    }
    return memory::make_tracked<LazyFilterBitsetIterator>(ctx.memory,
                                                          *cache_bitset);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // No terms to visit
  }

  score_t Boost() const noexcept final { return kNoBoost; }

 private:
  ProxyFilter::cache_ptr _cache;
};

Filter::Query::ptr ProxyFilter::prepare(const PrepareContext& ctx) const {
  // Currently we do not support caching scores.
  // Proxy filter should not be used with scorers!
  SDB_ASSERT(ctx.scorers.empty());
  if (!_cache || !ctx.scorers.empty()) {
    return Filter::Query::empty();
  }
  if (!_cache->real_filter_prepared) {
    _cache->real_filter_prepared = _cache->real_filter->prepare(ctx);
    _cache->real_filter.reset();
  }
  return memory::make_tracked<ProxyQuery>(ctx.memory, _cache);
}

Filter& ProxyFilter::cache_filter(IResourceManager& memory,
                                  Filter::ptr&& real) {
  _cache = std::make_shared<ProxyQueryCache>(memory, std::move(real));
  SDB_ASSERT(_cache->real_filter);
  return *_cache->real_filter;
}

}  // namespace irs
