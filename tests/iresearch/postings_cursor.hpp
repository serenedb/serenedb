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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>
#include <vector>

namespace tests {

class PostingsCursor {
 public:
  explicit PostingsCursor(
    irs::TermPostings::ptr postings,
    irs::IndexFeatures features = irs::IndexFeatures::None,
    irs::DocumentMask::Iterator mask = {})
    : _postings{std::move(postings)},
      _mask{std::move(mask)},
      _has_freq{irs::IsSubsetOf(irs::IndexFeatures::Freq, features)},
      _has_pos{irs::IsSubsetOf(
        irs::IndexFeatures::Freq | irs::IndexFeatures::Pos, features)},
      _pos{_has_pos && irs::IsSubsetOf(irs::IndexFeatures::Offs, features)} {}

  irs::doc_id_t Next() {
    while (true) {
      if (_at == _size) {
        _size = _postings->NextDocs(_docs, _freqs);
        _at = 0;
        if (_size == 0) {
          _freq = 0;
          return _doc = irs::doc_limits::eof();
        }
      }
      _doc = _docs[_at];
      _freq = _has_freq ? _freqs[_at] : 0;
      ++_at;
      if (_has_pos) {
        _pos.Load(*_postings, _freq);
      }
      if (!_mask.Contains(_doc)) {
        return _doc;
      }
    }
  }

  irs::doc_id_t SeekTo(irs::doc_id_t target) {
    while (_doc < target) {
      Next();
    }
    return _doc;
  }

  irs::doc_id_t Value() const noexcept { return _doc; }

  uint32_t GetFreq() const noexcept { return _freq; }

  irs::PosAttr* Positions() noexcept { return _has_pos ? &_pos : nullptr; }

 private:
  class Pos final : public irs::PosAttr {
   public:
    explicit Pos(bool has_offs) noexcept : _has_offs{has_offs} {}

    Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      return _has_offs && irs::Type<irs::OffsAttr>::id() == type ? &_offs
                                                                 : nullptr;
    }

    bool next() final {
      if (_next == _values.size()) {
        _value = irs::pos_limits::eof();
        return false;
      }
      _value = _values[_next];
      if (_has_offs) {
        _offs.start = _starts[_next];
        _offs.end = _ends[_next];
      }
      ++_next;
      return true;
    }

    void Load(irs::TermPostings& postings, uint32_t freq) {
      _values.resize(freq);
      _starts.resize(freq);
      _ends.resize(freq);
      postings.NextPositions(_values.data(),
                             _has_offs ? _starts.data() : nullptr,
                             _has_offs ? _ends.data() : nullptr, freq);
      uint32_t value = 0;
      uint32_t start = 0;
      for (uint32_t i = 0; i != freq; ++i) {
        value += _values[i];
        _values[i] = value;
        if (_has_offs) {
          start += _starts[i];
          _starts[i] = start;
          _ends[i] += start;
        }
      }
      _next = 0;
      _value = irs::pos_limits::invalid();
      _offs.clear();
    }

   private:
    std::vector<uint32_t> _values;
    std::vector<uint32_t> _starts;
    std::vector<uint32_t> _ends;
    size_t _next = 0;
    irs::OffsAttr _offs;
    bool _has_offs;
  };

  irs::TermPostings::ptr _postings;
  irs::DocumentMask::Iterator _mask;
  irs::doc_id_t _docs[irs::doc_limits::kBlockSize];
  uint32_t _freqs[irs::doc_limits::kBlockSize];
  uint32_t _at = 0;
  uint32_t _size = 0;
  irs::doc_id_t _doc = irs::doc_limits::invalid();
  uint32_t _freq = 0;
  bool _has_freq;
  bool _has_pos;
  Pos _pos;
};

inline std::unique_ptr<PostingsCursor> Docs(
  irs::TermPostings::ptr postings,
  irs::IndexFeatures features = irs::IndexFeatures::None) {
  return std::make_unique<PostingsCursor>(std::move(postings), features);
}

}  // namespace tests
