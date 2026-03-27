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

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats_attributes.hpp"
#include "iresearch/formats/posting/common.hpp"

namespace irs {

struct DocState {
  const IndexInput* pos_in;
  const IndexInput* pay_in;
  const TermMetaImpl* term_state;
  uint32_t* enc_buf;
};

template<typename IteratorTraits>
class PositionImpl final : public PosAttr {
 public:
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (IteratorTraits::Offset()) {
      return irs::Type<OffsAttr>::id() == type ? &_offs : nullptr;
    } else {
      return nullptr;
    }
  }

  value_t seek(value_t target) final {
    const uint32_t freq = _freq;
    if (_pend_pos > freq) {
      Skip(_pend_pos - freq);
      _pend_pos = freq;
    }
    while (_value < target && _pend_pos) {
      if (_buf_pos == doc_limits::kBlockSize) {
        ReadBlock();
        _buf_pos = 0;
      }
      _value += _pos_deltas[_buf_pos];
      SDB_ASSERT(pos_limits::valid(_value));
      ReadAttributes();

      ++_buf_pos;
      --_pend_pos;
    }
    if (0 == _pend_pos && _value < target) {
      _value = pos_limits::eof();
    }
    return _value;
  }

  bool next() final {
    if (0 == _pend_pos) {
      _value = pos_limits::eof();

      return false;
    }

    const uint32_t freq = _freq;

    if (_pend_pos > freq) {
      Skip(_pend_pos - freq);
      _pend_pos = freq;
    }

    if (_buf_pos == doc_limits::kBlockSize) {
      ReadBlock();
      _buf_pos = 0;
    }
    _value += _pos_deltas[_buf_pos];
    SDB_ASSERT(pos_limits::valid(_value));
    ReadAttributes();

    ++_buf_pos;
    --_pend_pos;
    return true;
  }

  void reset() final {
    Clear();
    if (_cookie.file_pointer != std::numeric_limits<uint64_t>::max()) {
      _buf_pos = doc_limits::kBlockSize;
      _pend_pos = _cookie.pend_pos;
      _pos_in->Seek(_cookie.file_pointer);
    }
  }

  // prepares iterator to work
  template<typename InputType>
  void Prepare(const DocState& state) {
    _pos_in = sdb::basics::downCast<InputType>(*state.pos_in).Reopen();

    if (!_pos_in) {
      // implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen positions input");

      throw IoError("failed to reopen positions input");
    }

    _cookie.file_pointer = state.term_state->pos_start;
    _cookie.pend_pos = state.term_state->pos_offset;
    sdb::basics::downCast<InputType>(*_pos_in).Seek(
      state.term_state->pos_start);
    _enc_buf = state.enc_buf;
    _pend_pos = _cookie.pend_pos;

    if constexpr (IteratorTraits::Offset()) {
      _pay_in = sdb::basics::downCast<InputType>(*state.pay_in).Reopen();

      if (!_pay_in) {
        // implementation returned wrong pointer
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to reopen payload input");

        throw IoError("failed to reopen payload input");
      }

      sdb::basics::downCast<InputType>(*_pay_in).Seek(
        state.term_state->pay_start);
    }
  }

  // notifies iterator that doc iterator has skipped to a new block
  template<typename InputType>
  void Prepare(const SkipState& state) {
    sdb::basics::downCast<InputType>(*_pos_in).Seek(state.pos_ptr);
    _pend_pos = state.pos_offset;
    _buf_pos = doc_limits::kBlockSize;
    _cookie.file_pointer = state.pos_ptr;
    _cookie.pend_pos = _pend_pos;

    if constexpr (IteratorTraits::Offset()) {
      sdb::basics::downCast<InputType>(*_pay_in).Seek(state.pay_ptr);
    }
  }

  // notify iterator that corresponding DocIterator has moved forward
  void Notify(uint32_t freq, uint32_t n) {
    _freq = freq;
    _pend_pos += n;
    _cookie.pend_pos += n;
  }

  void Clear() noexcept {
    _value = pos_limits::invalid();
    ClearAttributes();
  }

  uint32_t DocFreq() const noexcept { return _freq; }

 private:
  void Skip(uint64_t count) {
    SDB_ASSERT(count != 0);
    auto left = doc_limits::kBlockSize - _buf_pos;
    if (count > left) {
      count -= left;
      while (count >= doc_limits::kBlockSize) {
        SkipBlock();
        count -= doc_limits::kBlockSize;
      }
      if (count == 0) {
        _buf_pos = doc_limits::kBlockSize;
      } else {
        ReadBlock();
        _buf_pos = 0;
      }
    }
    _buf_pos += count;
    SDB_ASSERT(_buf_pos <= doc_limits::kBlockSize);
    Clear();
  }

  void ReadAttributes() noexcept {
    if constexpr (IteratorTraits::Offset()) {
      _offs.start += _offs_start_deltas[_buf_pos];
      _offs.end = _offs.start + _offs_lengths[_buf_pos];
    }
  }

  void ClearAttributes() noexcept {
    if constexpr (IteratorTraits::Offset()) {
      _offs.clear();
    }
  }

  void ReadBlock() {
    IteratorTraits::ReadBlock(*_pos_in, _enc_buf, _pos_deltas);
    if constexpr (IteratorTraits::Offset()) {
      IteratorTraits::ReadBlock(*_pay_in, _enc_buf, _offs_start_deltas);
      IteratorTraits::ReadBlock(*_pay_in, _enc_buf, _offs_lengths);
    }
  }

  void SkipBlock() {
    IteratorTraits::SkipBlock(*_pos_in);
    if constexpr (IteratorTraits::Offset()) {
      IteratorTraits::SkipBlock(*_pay_in);
      IteratorTraits::SkipBlock(*_pay_in);
    }
  }

  struct Cookie {
    uint64_t pend_pos = 0;
    uint64_t file_pointer = std::numeric_limits<uint64_t>::max();
  };

  template<typename T>
  using ForOffset = utils::Need<IteratorTraits::Offset(), T>;

  uint32_t _pos_deltas[doc_limits::kBlockSize];
  [[no_unique_address]] ForOffset<uint32_t[doc_limits::kBlockSize]>
    _offs_start_deltas;
  [[no_unique_address]] ForOffset<uint32_t[doc_limits::kBlockSize]>
    _offs_lengths;
  uint32_t _freq = 0;      // length of the posting list for a document
  uint32_t* _enc_buf;      // auxillary buffer to decode data
  uint64_t _pend_pos = 0;  // how many positions "behind" we are
  uint64_t _buf_pos = doc_limits::kBlockSize;  // position in pos_deltas_
  Cookie _cookie;
  IndexInput::ptr _pos_in;
  [[no_unique_address]] ForOffset<IndexInput::ptr> _pay_in;
  [[no_unique_address]] ForOffset<OffsAttr> _offs;
};

// use base PosAttr type for ancestors
template<typename IteratorTraits>
struct Type<PositionImpl<IteratorTraits>> : Type<PosAttr> {};

}  // namespace irs
