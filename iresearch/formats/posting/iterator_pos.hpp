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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs {

struct DocState {
  const IndexInput* pos_in;
  const IndexInput* pay_in;
  const PostingMeta* term_state;
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
    if (_cookie.pos_file_pointer != std::numeric_limits<uint64_t>::max()) {
      _buf_pos = doc_limits::kBlockSize;
      _pend_pos = _cookie.pend_pos;
      _pos_in->Seek(_cookie.pos_file_pointer);
      if constexpr (IteratorTraits::Offset()) {
        _pay_in->Seek(_cookie.pay_file_pointer);
      }
    }
  }

  // prepares iterator to work
  template<typename InputType>
  void Prepare(const DocState& state) {
    SDB_ASSERT(!_pos_in);
    _pos_in = irs::utils::downCast<InputType>(*state.pos_in).Reopen();

    if (!_pos_in) {
      // implementation returned wrong pointer
      SDB_ERROR(IRESEARCH, "Failed to reopen positions input");

      throw IoError("failed to reopen positions input");
    }

    _pos_view = IteratorTraits::View(*_pos_in);
    _cookie.pos_file_pointer = state.term_state->pos_start;
    _cookie.pend_pos = state.term_state->pos_offset;
    irs::utils::downCast<InputType>(*_pos_in).Seek(state.term_state->pos_start);
    LimitPosReadahead(irs::utils::downCast<InputType>(*_pos_in),
                      *state.term_state);
    _enc_buf = state.enc_buf;
    _pend_pos = _cookie.pend_pos;

    if constexpr (IteratorTraits::Offset()) {
      SDB_ASSERT(!_pay_in);
      _pay_in = irs::utils::downCast<InputType>(*state.pay_in).Reopen();

      if (!_pay_in) {
        // implementation returned wrong pointer
        SDB_ERROR(IRESEARCH, "Failed to reopen payload input");

        throw IoError("failed to reopen payload input");
      }

      _pay_view = IteratorTraits::View(*_pay_in);
      _cookie.pay_file_pointer = state.term_state->pay_start;
      irs::utils::downCast<InputType>(*_pay_in).Seek(
        state.term_state->pay_start);
    }
  }

  // notifies iterator that doc iterator has skipped to a new block
  template<typename InputType>
  void Prepare(const SkipState& state) {
    irs::utils::downCast<InputType>(*_pos_in).Seek(state.pos_ptr);
    _pend_pos = state.pos_offset;
    _buf_pos = doc_limits::kBlockSize;
    _cookie.pos_file_pointer = state.pos_ptr;
    _cookie.pend_pos = _pend_pos;

    if constexpr (IteratorTraits::Offset()) {
      _cookie.pay_file_pointer = state.pay_ptr;
      irs::utils::downCast<InputType>(*_pay_in).Seek(state.pay_ptr);
    }
  }

  // notify the positions that the document stream has moved forward
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

  template<typename Input>
  IRS_FORCE_INLINE void ReadBlock(Input& pos, Input* pay) {
    IteratorTraits::ReadBlock(pos, _enc_buf, _pos_deltas);
    if constexpr (IteratorTraits::Offset()) {
      IteratorTraits::ReadBlock(*pay, _enc_buf, _offs_start_deltas);
      IteratorTraits::ReadBlock(*pay, _enc_buf, _offs_lengths);
    }
  }

  void ReadBlock() {
    if (_pos_view != nullptr && (!IteratorTraits::Offset() || PayView()))
      [[likely]] {
      ReadBlock<BytesViewInput>(*_pos_view, PayView());
    } else {
      ReadBlock<IndexInput>(*_pos_in, PayIn());
    }
  }

  template<typename Input>
  IRS_FORCE_INLINE static void SkipBlock(Input& pos, Input* pay) {
    IteratorTraits::SkipBlock(pos);
    if constexpr (IteratorTraits::Offset()) {
      IteratorTraits::SkipBlock(*pay);
      IteratorTraits::SkipBlock(*pay);
    }
  }

  void SkipBlock() {
    if (_pos_view != nullptr && (!IteratorTraits::Offset() || PayView()))
      [[likely]] {
      SkipBlock<BytesViewInput>(*_pos_view, PayView());
    } else {
      SkipBlock<IndexInput>(*_pos_in, PayIn());
    }
  }

  IRS_FORCE_INLINE BytesViewInput* PayView() const noexcept {
    if constexpr (IteratorTraits::Offset()) {
      return _pay_view;
    } else {
      return nullptr;
    }
  }

  IRS_FORCE_INLINE IndexInput* PayIn() const noexcept {
    if constexpr (IteratorTraits::Offset()) {
      return _pay_in.get();
    } else {
      return nullptr;
    }
  }

  struct Cookie {
    uint64_t pend_pos = 0;
    uint64_t pos_file_pointer = std::numeric_limits<uint64_t>::max();
    [[no_unique_address]] utils::Need<IteratorTraits::Offset(), uint64_t>
      pay_file_pointer;
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
  BytesViewInput* _pos_view = nullptr;
  [[no_unique_address]] ForOffset<IndexInput::ptr> _pay_in;
  [[no_unique_address]] ForOffset<BytesViewInput*> _pay_view{};
  [[no_unique_address]] ForOffset<OffsAttr> _offs;
};

// use base PosAttr type for ancestors
template<typename IteratorTraits>
struct Type<PositionImpl<IteratorTraits>> : Type<PosAttr> {};

}  // namespace irs
