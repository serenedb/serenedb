////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <unordered_set>

#include "index/index_tests.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/common/posting_pos.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/lead/posting_docs.hpp"

namespace tests {

class SeekPostings : public irs::lead::Node {
 public:
  using ptr = irs::memory::managed_ptr<SeekPostings>;

  irs::doc_id_t Value() const noexcept { return _doc; }

  virtual irs::PosAttr* Positions() noexcept = 0;

  virtual uint32_t GetFreq() noexcept = 0;

 protected:
  irs::doc_id_t _doc = irs::doc_limits::invalid();
};

template<typename Leaf>
class SeekPostingsImpl : public SeekPostings {
 public:
  template<typename... Args>
  explicit SeekPostingsImpl(Args&&... args) {
    _leaf.Prepare(std::forward<Args>(args)...);
  }

  irs::doc_id_t Advance() final { return _doc = _leaf.Advance(); }

  irs::doc_id_t Seek(irs::doc_id_t target) final {
    return _doc = _leaf.Seek(target);
  }

  irs::PosAttr* Positions() noexcept final {
    if constexpr (kPositions) {
      return &_leaf.Positions();
    } else {
      return nullptr;
    }
  }

  uint32_t GetFreq() noexcept final {
    if constexpr (kPositions) {
      return _leaf.Positions().DocFreq();
    } else {
      return 0;
    }
  }

 private:
  static constexpr bool kPositions = requires(Leaf& leaf) { leaf.Positions(); };

  Leaf _leaf;
};

inline SeekPostings::ptr MakeSeekPostings(const irs::PostingMeta& meta,
                                          const irs::PostingsHandles& handles,
                                          irs::IndexFeatures layout,
                                          irs::IndexFeatures required,
                                          bool has_score_bounds) {
  return irs::search::ResolveInput(
    *handles.doc, [&]<typename Input> -> SeekPostings::ptr {
      if (irs::IndexFeatures::None == (required & irs::IndexFeatures::Pos)) {
        return irs::memory::make_managed<
          SeekPostingsImpl<irs::search::PostingLead<Input>>>(
          meta, *handles.doc, layout, has_score_bounds);
      }
      return irs::search::ResolveBounds(
        has_score_bounds, [&]<bool Bounds> -> SeekPostings::ptr {
          if (irs::IndexFeatures::None !=
              (required & irs::IndexFeatures::Offs)) {
            return irs::memory::make_managed<
              SeekPostingsImpl<irs::search::PostingPos<Input, Bounds, true>>>(
              meta, *handles.doc, layout, *handles.pos, handles.pay);
          }
          return irs::memory::make_managed<
            SeekPostingsImpl<irs::search::PostingPos<Input, Bounds, false>>>(
            meta, *handles.doc, layout, *handles.pos, handles.pay);
        });
    });
}

class MockTermReader final : public irs::BasicTermReader {
 public:
  explicit MockTermReader(irs::TermIterator& it, irs::FieldMeta meta,
                          irs::bytes_view min_term, irs::bytes_view max_term)
    : _it{it},
      _meta{std::move(meta)},
      _min_term{min_term},
      _max_term(max_term) {}

 private:
  irs::Attribute* GetMutable(irs::TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }
  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::to_managed<irs::TermOnlyIterator>(_it);
  }
  const irs::FieldMeta& meta() const { return _meta; }
  irs::field_id id() const final { return meta().id; }
  irs::FieldProperties properties() const final { return meta(); }
  irs::bytes_view min() const final { return _min_term; }
  irs::bytes_view max() const final { return _max_term; }

  irs::TermIterator& _it;
  irs::FieldMeta _meta;
  irs::bytes_view _min_term;
  irs::bytes_view _max_term;
};

class FormatTestCase : public IndexTestBase {
 public:
  class TestPostings;

  class Position final : public irs::PosAttr {
   public:
    explicit Position(irs::IndexFeatures features) {
      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs)) {
        _poffs = &_offs;
      }
    }

    Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      if (irs::Type<irs::OffsAttr>::id() == type) {
        return _poffs;
      }

      return nullptr;
    }

    bool next() final {
      if (_value == _end) {
        _value = _end = irs::pos_limits::eof();

        return false;
      }

      ++_value;
      EXPECT_TRUE(irs::pos_limits::valid(_value));

      const auto written = sprintf(_pay_data, "%d", _value);

      _offs.start = _value;
      _offs.end = _offs.start + written;
      return true;
    }

    void clear() { _offs.clear(); }

    void reset() final {
      SDB_ASSERT(false);  // unsupported
    }

   private:
    friend class TestPostings;

    uint32_t _end;
    irs::OffsAttr _offs;
    irs::OffsAttr* _poffs{};
    char _pay_data[21];  // enough to hold numbers up to max of uint64_t
  };

  class TestPostings : public irs::TermPostings {
   public:
    // DocId + Freq
    using docs_t = std::span<const std::pair<irs::doc_id_t, uint32_t>>;

    TestPostings(std::span<const std::pair<irs::doc_id_t, uint32_t>> docs,
                 irs::IndexFeatures features = irs::IndexFeatures::None)
      : _next(std::begin(docs)),
        _end(std::end(docs)),
        _pos(features),
        _has_pos{
          irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq) &&
          irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos)} {}

    irs::doc_id_t Advance() final {
      if (_next == _end) {
        return _doc = irs::doc_limits::eof();
      }

      std::tie(_doc, _freq) = *_next;

      EXPECT_TRUE(irs::doc_limits::valid(_doc));
      _pos._value = _doc;
      EXPECT_TRUE(irs::pos_limits::valid(_pos._value));
      _pos._end = _pos._value + _freq;
      _pos.clear();
      ++_next;

      return _doc;
    }

    irs::doc_id_t SeekTo(irs::doc_id_t target) {
      while (_doc < target) {
        Advance();
      }
      return _doc;
    }

    uint32_t GetFreq() const final { return _freq; }

    irs::PosAttr* Positions() noexcept final {
      return _has_pos ? &_pos : nullptr;
    }

   private:
    docs_t::iterator _next;
    docs_t::iterator _end;
    uint32_t _freq = 0;
    FormatTestCase::Position _pos;
    bool _has_pos;
  };

  bool supports_encryption() const noexcept { return true; }

  bool supports_columnstore_headers() const noexcept { return true; }

  template<typename It>
  class Terms : public irs::TermIterator {
   public:
    using docs_type = std::vector<std::pair<irs::doc_id_t, uint32_t>>;

    Terms(const It& begin, const It& end) : _next(begin), _end(end) {
      SDB_ASSERT(std::is_sorted(begin, end));
      _docs.emplace_back((irs::doc_limits::min)(), 0);
    }

    Terms(const It& begin, const It& end, docs_type::const_iterator doc_begin,
          docs_type::const_iterator doc_end)
      : _docs(doc_begin, doc_end), _next(begin), _end(end) {
      SDB_ASSERT(std::is_sorted(begin, end));
    }

    bool next() final {
      if (_next == _end) {
        return false;
      }

      _val = *_next;
      ++_next;
      return true;
    }

    irs::bytes_view value() const noexcept final { return _val; }

    // Every term in this source carries the same doc list, so its record is
    // the same too.
    const irs::PostingMeta& cookie() const noexcept final { return _meta; }

    irs::TermPostings::ptr postings(
      irs::IndexFeatures /*features*/) const final {
      return irs::memory::make_managed<FormatTestCase::TestPostings>(_docs);
    }

    irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
      return nullptr;
    }

   private:
    irs::bytes_view _val;
    irs::PostingMeta _meta;
    docs_type _docs;
    It _next;
    It _end;
  };

  template<typename Actual>
  void AssertFrequencyAndPositions(TestPostings& expected, Actual& actual,
                                   irs::IndexFeatures features) {
    if (irs::doc_limits::eof(expected.Value())) {
      ASSERT_TRUE(irs::doc_limits::eof(actual.Value()));
      return;
    }

    ASSERT_EQ(expected.Value(), actual.Value());

    auto* expected_pos = expected.Positions();
    auto* actual_pos = actual.Positions();
    ASSERT_EQ(irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos),
              actual_pos != nullptr);
    ASSERT_EQ(!expected_pos, !actual_pos);

    if (!expected_pos) {
      return;
    }

    ASSERT_EQ(expected.GetFreq(), actual.GetFreq());

    auto* expected_offset = irs::get<irs::OffsAttr>(*expected_pos);
    auto* actual_offset = irs::get<irs::OffsAttr>(*actual_pos);
    ASSERT_EQ(!expected_offset, !actual_offset);

    auto* expected_payload = irs::get<irs::PayAttr>(*expected_pos);
    auto* actual_payload = irs::get<irs::PayAttr>(*actual_pos);
    ASSERT_EQ(!expected_payload, !actual_payload);

    for (; expected_pos->next();) {
      ASSERT_TRUE(actual_pos->next());
      ASSERT_EQ(expected_pos->value(), actual_pos->value());

      if (expected_offset) {
        ASSERT_EQ(expected_offset->start, actual_offset->start);
        ASSERT_EQ(expected_offset->end, actual_offset->end);
      }

      if (expected_payload) {
        ASSERT_EQ(expected_payload->value, actual_payload->value);
      }
    }
    ASSERT_FALSE(actual_pos->next());
  }

  void AssertNoDirectoryArtifacts(
    const irs::Directory& dir, const irs::Format& codec,
    const std::unordered_set<std::string>& expect_additional = {});
};

class FormatTestCaseWithEncryption : public FormatTestCase {};

}  // namespace tests
namespace irs {

// use base irs::position type for ancestors
template<>
struct Type<::tests::FormatTestCase::Position> : Type<irs::PosAttr> {};

}  // namespace irs
