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
    if (_cookie.pos_group != std::numeric_limits<uint64_t>::max()) {
      Land(_cookie.pos_group, PayGroupOf(_cookie), _cookie.pend_pos);
    }
  }

  // prepares iterator to work
  template<typename InputType>
  void Prepare(const DocState& state) {
    SDB_ASSERT(!_pos.in);
    _pos.in = irs::utils::downCast<InputType>(*state.pos_in).Reopen();

    if (!_pos.in) {
      // implementation returned wrong pointer
      SDB_ERROR(IRESEARCH, "Failed to reopen positions input");

      throw IoError("failed to reopen positions input");
    }

    _pos.view = IteratorTraits::View(*_pos.in);
    LimitPosReadahead(irs::utils::downCast<InputType>(*_pos.in),
                      *state.term_state);
    _enc_buf = state.enc_buf;

    if constexpr (IteratorTraits::Offset()) {
      SDB_ASSERT(!_pay.in);
      _pay.in = irs::utils::downCast<InputType>(*state.pay_in).Reopen();

      if (!_pay.in) {
        // implementation returned wrong pointer
        SDB_ERROR(IRESEARCH, "Failed to reopen payload input");

        throw IoError("failed to reopen payload input");
      }

      _pay.view = IteratorTraits::View(*_pay.in);
    }
    Land(state.term_state->pos_start, state.term_state->pay_start,
         state.term_state->pos_offset);
  }

  // notifies iterator that doc iterator has skipped to a new block
  void Prepare(const SkipState& state) noexcept {
    Land(state.pos_ptr, state.pay_ptr, state.pos_offset);
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
  struct Stream {
    void Load() {
      if (view != nullptr) {
        header = view->ReadStable(group, PosGroup::kHeaderBytes);
      } else {
        in->Seek(group);
        in->ReadData(copy, PosGroup::kHeaderBytes);
        header = copy;
      }
    }

    void Hop() {
      group = PosGroup::Next(group, header);
      Load();
    }

    void Step() {
      group = PosGroup::Next(group, header);
      if (view != nullptr) {
        SDB_ASSERT(view->Position() == group);
        header = view->ReadStable(PosGroup::kHeaderBytes);
      } else {
        SDB_ASSERT(in->Position() == group);
        in->ReadData(copy, PosGroup::kHeaderBytes);
        header = copy;
      }
    }

    void SeekBlock(uint32_t block) {
      const auto at =
        group + PosGroup::kHeaderBytes + PosGroup::Start(header, block);
      if (view != nullptr) {
        view->Seek(at);
      } else {
        in->Seek(at);
      }
    }

    IndexInput::ptr in;
    BytesViewInput* view = nullptr;
    uint64_t group = 0;
    const byte_type* header = nullptr;
    byte_type copy[PosGroup::kHeaderBytes];
  };

  struct Cookie {
    uint64_t pend_pos = 0;
    uint64_t pos_group = std::numeric_limits<uint64_t>::max();
    [[no_unique_address]] utils::Need<IteratorTraits::Offset(), uint64_t>
      pay_group;
  };

  static uint64_t PayGroupOf(const Cookie& cookie) noexcept {
    if constexpr (IteratorTraits::Offset()) {
      return cookie.pay_group;
    } else {
      return 0;
    }
  }

  void Land(uint64_t pos_group, uint64_t pay_group, uint64_t pend) noexcept {
    _pos.group = pos_group;
    _pos.header = nullptr;
    _cookie.pos_group = pos_group;
    if constexpr (IteratorTraits::Offset()) {
      _pay.group = pay_group;
      _cookie.pay_group = pay_group;
    }
    _next = 0;
    _buf_pos = doc_limits::kBlockSize;
    _pend_pos = pend;
    _cookie.pend_pos = pend;
  }

  void Enter(uint64_t block) {
    if (_pos.header == nullptr) {
      _pos.Load();
      if constexpr (IteratorTraits::Offset()) {
        _pay.Load();
      }
    }
    for (; block >= PosGroup::kBlocks; block -= PosGroup::kBlocks) {
      _pos.Hop();
      if constexpr (IteratorTraits::Offset()) {
        _pay.Hop();
      }
    }
    _next = static_cast<uint32_t>(block);
    _pos.SeekBlock(_next);
    if constexpr (IteratorTraits::Offset()) {
      _pay.SeekBlock(_next);
    }
  }

  void Skip(uint64_t count) {
    SDB_ASSERT(count != 0);
    const uint64_t left = doc_limits::kBlockSize - _buf_pos;
    if (count > left) {
      count -= left;
      Enter(_next + count / doc_limits::kBlockSize);
      count %= doc_limits::kBlockSize;
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
  IRS_FORCE_INLINE void Decode(Input& pos, Input* pay) {
    IteratorTraits::ReadBlock(pos, _enc_buf, _pos_deltas);
    if constexpr (IteratorTraits::Offset()) {
      IteratorTraits::ReadBlock(*pay, _enc_buf, _offs_start_deltas);
      IteratorTraits::ReadBlock(*pay, _enc_buf, _offs_lengths);
    }
  }

  void ReadBlock() {
    if (_pos.header == nullptr) [[unlikely]] {
      Enter(_next);
    } else if (_next == PosGroup::kBlocks) [[unlikely]] {
      _pos.Step();
      if constexpr (IteratorTraits::Offset()) {
        _pay.Step();
      }
      _next = 0;
    }
    if (_pos.view != nullptr && (!IteratorTraits::Offset() || PayView()))
      [[likely]] {
      Decode<BytesViewInput>(*_pos.view, PayView());
    } else {
      Decode<IndexInput>(*_pos.in, PayIn());
    }
    ++_next;
  }

  IRS_FORCE_INLINE BytesViewInput* PayView() const noexcept {
    if constexpr (IteratorTraits::Offset()) {
      return _pay.view;
    } else {
      return nullptr;
    }
  }

  IRS_FORCE_INLINE IndexInput* PayIn() const noexcept {
    if constexpr (IteratorTraits::Offset()) {
      return _pay.in.get();
    } else {
      return nullptr;
    }
  }

  template<typename T>
  using ForOffset = utils::Need<IteratorTraits::Offset(), T>;

  uint32_t _pos_deltas[doc_limits::kBlockSize];
  [[no_unique_address]] ForOffset<uint32_t[doc_limits::kBlockSize]>
    _offs_start_deltas;
  [[no_unique_address]] ForOffset<uint32_t[doc_limits::kBlockSize]>
    _offs_lengths;
  uint32_t _freq = 0;      // length of the posting list for a document
  uint32_t _next = 0;
  uint32_t* _enc_buf;      // auxillary buffer to decode data
  uint64_t _pend_pos = 0;  // how many positions "behind" we are
  uint64_t _buf_pos = doc_limits::kBlockSize;  // position in pos_deltas_
  Cookie _cookie;
  Stream _pos;
  [[no_unique_address]] ForOffset<Stream> _pay;
  [[no_unique_address]] ForOffset<OffsAttr> _offs;
};

// use base PosAttr type for ancestors
template<typename IteratorTraits>
struct Type<PositionImpl<IteratorTraits>> : Type<PosAttr> {};

}  // namespace irs
