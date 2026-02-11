////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <benchmark/benchmark.h>

#include <iresearch/formats/empty_term_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/top_terms_collector.hpp>
#include <iresearch/utils/string.hpp>

namespace {

struct TermMetaBench : irs::TermMeta {
  TermMetaBench(uint32_t docs_count = 0, uint32_t freq = 0) noexcept {
    this->docs_count = docs_count;
    this->freq = freq;
  }
};

}  // namespace

namespace irs {

// use base irs::TermMeta type for ancestors
template<>
struct Type<TermMetaBench> : Type<TermMeta> {};

}  // namespace irs

namespace {

template<typename T>
class SeekTermIterator : public irs::SeekTermIterator {
 public:
  using iterator_type = const std::tuple<irs::bytes_view, TermMetaBench, T>*;

  SeekTermIterator(iterator_type begin, size_t count)
    : _begin(begin), _end(begin + count), _cookie_ptr(begin) {}

  irs::SeekResult seek_ge(irs::bytes_view) final {
    return irs::SeekResult::NotFound;
  }

  bool seek(irs::bytes_view) final { return false; }

  irs::SeekCookie::ptr cookie() const final {
    return std::make_unique<SeekPtr>(_cookie_ptr);
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<decltype(_meta)>::id()) {
      return &_meta;
    }
    return nullptr;
  }

  bool next() noexcept final {
    if (_begin == _end) {
      return false;
    }

    _value = std::get<0>(*_begin);
    _cookie_ptr = _begin;
    _meta = std::get<1>(*_begin);
    ++_begin;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _value; }

  void read() final {}

  irs::DocIterator::ptr postings(irs::IndexFeatures /*features*/) const final {
    return irs::DocIterator::empty();
  }

  struct SeekPtr final : irs::SeekCookie {
    explicit SeekPtr(iterator_type ptr) noexcept : ptr(ptr) {}

    irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
      return nullptr;
    }

    bool IsEqual(const irs::SeekCookie& /*rhs*/) const final { return false; }

    size_t Hash() const final { return 0; }

    iterator_type ptr;
  };

 private:
  TermMetaBench _meta;
  irs::bytes_view _value;
  iterator_type _begin;
  iterator_type _end;
  iterator_type _cookie_ptr;
};

struct SubReader final : irs::SubReader {
  explicit SubReader(uint32_t num_docs)
    : info{.docs_count = num_docs, .live_docs_count = num_docs} {}

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const noexcept final { return info; }
  const irs::ColumnReader* column(std::string_view) const final {
    return nullptr;
  }
  const irs::DocumentMask* docs_mask() const noexcept final { return nullptr; }
  irs::ColumnIterator::ptr columns() const final {
    return irs::ColumnIterator::empty();
  }
  const irs::ColumnReader* column(irs::field_id) const final { return nullptr; }
  irs::DocIterator::ptr docs_iterator() const final {
    return irs::DocIterator::empty();
  }
  const irs::TermReader* field(std::string_view) const final { return nullptr; }
  irs::FieldIterator::ptr fields() const final {
    return irs::FieldIterator::empty();
  }
  const irs::ColumnReader* sort() const final { return nullptr; }

  irs::SegmentInfo info;
};

struct State {
  struct SegmentState {
    const irs::TermReader* field;
    uint32_t docs_count;
    std::vector<const std::pair<std::string_view, TermMetaBench>*> cookies;
  };

  std::map<const irs::SubReader*, SegmentState> segments;
};

void BmTopTermCollector(benchmark::State& state) {
  using collector_type = irs::TopTermsCollector<irs::TopTermState<int>>;
  collector_type collector(64);  // same as collector(1)
  irs::EmptyTermReader term_reader(42);
  SubReader segment(100);

  std::vector<std::tuple<irs::bytes_view, TermMetaBench, int>> terms(
    state.range(0));
  for (auto& term : terms) {
    auto& key = std::get<2>(term) = ::rand();
    std::get<0>(term) = irs::bytes_view(
      reinterpret_cast<const irs::byte_type*>(&key), sizeof(key));
  }

  for (auto _ : state) {
    SeekTermIterator<int> it(terms.data(), terms.size());
    collector.Prepare(segment, term_reader, it);

    while (it.next()) {
      collector.Visit(*reinterpret_cast<const int*>(it.value().data()));
    }
  }
}

}  // namespace

BENCHMARK(BmTopTermCollector)->DenseRange(0, 2048, 32);

BENCHMARK_MAIN();
