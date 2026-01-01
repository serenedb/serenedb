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
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/field_meta.hpp>
#include <unordered_set>

#include "index/index_tests.hpp"

namespace tests {

class MockTermReader final : public irs::BasicTermReader {
 public:
  explicit MockTermReader(irs::TermIterator& it, irs::FieldMeta meta,
                          irs::bytes_view min_term, irs::bytes_view max_term)
    : _it{it},
      _meta{std::move(meta)},
      _min_term{min_term},
      _max_term(max_term) {}

 private:
  irs::Attribute* GetMutable(irs::TypeInfo::type_id /*type*/) final {
    return nullptr;
  }
  irs::TermIterator::ptr iterator() const final {
    return irs::memory::to_managed<irs::TermIterator>(_it);
  }
  const irs::FieldMeta& meta() const { return _meta; }
  std::string_view name() const final { return meta().name; }
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

      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Pay)) {
        _ppay = &_pay;
      }
    }

    Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      if (irs::Type<irs::OffsAttr>::id() == type) {
        return _poffs;
      }

      if (irs::Type<irs::PayAttr>::id() == type) {
        return _ppay;
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
      _pay.value = irs::bytes_view(
        reinterpret_cast<const irs::byte_type*>(_pay_data), written);

      _offs.start = _value;
      _offs.end = _offs.start + written;
      return true;
    }

    void clear() {
      _pay.value = {};
      _offs.clear();
    }

    void reset() final {
      SDB_ASSERT(false);  // unsupported
    }

   private:
    friend class TestPostings;

    uint32_t _end;
    irs::OffsAttr _offs;
    irs::PayAttr _pay;
    irs::OffsAttr* _poffs{};
    irs::PayAttr* _ppay{};
    char _pay_data[21];  // enough to hold numbers up to max of uint64_t
  };

  class TestPostings : public irs::DocIterator {
   public:
    // DocId + Freq
    using docs_t = std::span<const std::pair<irs::doc_id_t, uint32_t>>;

    TestPostings(std::span<const std::pair<irs::doc_id_t, uint32_t>> docs,
                 irs::IndexFeatures features = irs::IndexFeatures::None)
      : _next(std::begin(docs)), _end(std::end(docs)), _pos(features) {
      _attrs[irs::Type<irs::DocAttr>::id()] = &_doc;
      _attrs[irs::Type<irs::AttrProviderChangeAttr>::id()] = &_callback;
      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq)) {
        _freq.value = 0;
        _attrs[irs::Type<irs::FreqAttr>::id()] = &_freq;
        if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos)) {
          _attrs[irs::Type<irs::PosAttr>::id()] = &_pos;
        }
      }
    }

    irs::doc_id_t advance() final {
      if (!irs::doc_limits::valid(_doc.value)) {
        _callback(*this);
      }

      if (_next == _end) {
        return _doc.value = irs::doc_limits::eof();
      }

      std::tie(_doc.value, _freq.value) = *_next;

      EXPECT_TRUE(irs::doc_limits::valid(_doc.value));
      _pos._value = _doc.value;
      EXPECT_TRUE(irs::pos_limits::valid(_pos._value));
      _pos._end = _pos._value + _freq.value;
      _pos.clear();
      ++_next;

      return _doc.value;
    }

    irs::doc_id_t value() const final { return _doc.value; }

    irs::doc_id_t seek(irs::doc_id_t target) final {
      irs::seek(*this, target);
      return value();
    }

    irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      const auto it = _attrs.find(type);
      return it == _attrs.end() ? nullptr : it->second;
    }

   private:
    std::map<irs::TypeInfo::type_id, irs::Attribute*> _attrs;
    docs_t::iterator _next;
    docs_t::iterator _end;
    irs::FreqAttr _freq;
    irs::AttrProviderChangeAttr _callback;
    FormatTestCase::Position _pos;
    irs::DocAttr _doc;
  };

  irs::ColumnInfo lz4_column_info() const noexcept;
  irs::ColumnInfo none_column_info() const noexcept;

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

    irs::bytes_view value() const final { return _val; }

    irs::DocIterator::ptr postings(
      irs::IndexFeatures /*features*/) const final {
      return irs::memory::make_managed<FormatTestCase::TestPostings>(_docs);
    }

    void read() final {}

    irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
      return nullptr;
    }

   private:
    irs::bytes_view _val;
    docs_type _docs;
    It _next;
    It _end;
  };

  void AssertFrequencyAndPositions(irs::DocIterator& expected,
                                   irs::DocIterator& actual);

  void AssertNoDirectoryArtifacts(
    const irs::Directory& dir, const irs::Format& codec,
    const std::unordered_set<std::string>& expect_additional = {});
};

class FormatTestCaseWithEncryption : public FormatTestCase {};

}  // namespace tests

namespace irs {

// use base irs::position type for ancestors
template<>
struct Type<tests::FormatTestCase::Position> : Type<irs::PosAttr> {};

}  // namespace irs
