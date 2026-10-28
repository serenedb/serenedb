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
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/data_input.hpp"
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
      if (_buf_pos == pos_limits::kBlockSize) {
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

    if (_buf_pos == pos_limits::kBlockSize) {
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

  void ReadDeltas(uint32_t* pos, uint32_t* offs_start, uint32_t* offs_len,
                  uint32_t n) {
    if (_pend_pos != 0) {
      Skip(_pend_pos);
      _pend_pos = 0;
    }
    while (n != 0) {
      if (_buf_pos == pos_limits::kBlockSize) {
        ReadBlock();
        _buf_pos = 0;
      }
      const auto take = static_cast<uint32_t>(
        std::min<uint64_t>(n, pos_limits::kBlockSize - _buf_pos));
      std::copy_n(_pos_deltas + _buf_pos, take, pos);
      pos += take;
      if constexpr (IteratorTraits::Offset()) {
        if (offs_start != nullptr) {
          std::copy_n(_offs_start_deltas + _buf_pos, take, offs_start);
          std::copy_n(_offs_lengths + _buf_pos, take, offs_len);
          offs_start += take;
          offs_len += take;
        }
      }
      _buf_pos += take;
      n -= take;
    }
  }

  void SkipDeltas(uint64_t n) noexcept { _pend_pos += n; }

  void reset() final {
    Clear();
    if (_cookie.pos_group != std::numeric_limits<uint64_t>::max()) {
      Land(_cookie.pos_group, PayGroupOf(_cookie), _cookie.pend_pos);
    }
  }

  // prepares iterator to work
  void Prepare(const DocState& state) {
    if (!_pos.Opened()) {
      if constexpr (IteratorTraits::Offset()) {
        const bool whole = state.pos_in->Whole() && state.pay_in->Whole();
        _pos.Open(*state.pos_in, whole);
        _pay.Open(*state.pay_in, whole);
      } else {
        _pos.Open(*state.pos_in, state.pos_in->Whole());
      }
    }
    _pos.hint.Arm(state.term_state->pos_start,
                  state.term_state->pos_start + PosExtent(*state.term_state));
    _enc_buf = state.enc_buf;

    if constexpr (IteratorTraits::Offset()) {
      _pay.hint.Arm(state.term_state->pay_start,
                    state.term_state->pay_start + PayExtent(*state.term_state));
    }
    Land(state.term_state->pos_start, state.term_state->pay_start,
         state.term_state->pos_offset);
  }

  // notifies iterator that doc iterator has skipped to a new block
  void Prepare(const BlockLanding& state) noexcept {
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
    void Open(const IndexInput& source, bool whole) {
      viewed = whole;
      if (viewed) {
        view = BytesViewInput{source};
      } else {
        in = source;
      }
    }

    bool Opened() const noexcept {
      return viewed ? static_cast<bool>(view) : static_cast<bool>(in);
    }

    template<auto Input>
    void Load() {
      auto& input = this->*Input;
      hint.Advance(input, group);
      input.Seek(group);
      ReadHeader(input);
    }

    template<auto Input>
    void Hop() {
      group = PosGroup::Next(group, header);
      Load<Input>();
    }

    template<auto Input>
    void Step() {
      group = PosGroup::Next(group, header);
      auto& input = this->*Input;
      SDB_ASSERT(input.Position() == group);
      ReadHeader(input);
    }

    template<typename Input>
    void ReadHeader(Input& input) {
      if constexpr (Input::kSpan) {
        header = input.Current();
        input.Skip(PosGroup::kHeaderBytes);
      } else {
        input.ReadData(copy, PosGroup::kHeaderBytes);
        header = copy;
      }
    }

    template<auto Input>
    void SeekBlock(uint32_t block) {
      (this->*Input)
        .Seek(group + PosGroup::kHeaderBytes + PosGroup::Start(header, block));
    }

    IndexInput in;
    BytesViewInput view;
    bool viewed = false;
    uint64_t group = 0;
    const byte_type* header = nullptr;
    byte_type copy[PosGroup::kHeaderBytes];
    GrowingHint hint;
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
    _buf_pos = pos_limits::kBlockSize;
    _pend_pos = pend;
    _cookie.pend_pos = pend;
  }

  void Enter(uint64_t block) {
    if (_pos.viewed) [[likely]] {
      EnterFrom<&Stream::view>(block);
    } else {
      EnterFrom<&Stream::in>(block);
    }
  }

  template<auto Input>
  void EnterFrom(uint64_t block) {
    if (_pos.header == nullptr) {
      _pos.template Load<Input>();
      if constexpr (IteratorTraits::Offset()) {
        _pay.template Load<Input>();
      }
    }
    for (; block >= PosGroup::kBlocks; block -= PosGroup::kBlocks) {
      _pos.template Hop<Input>();
      if constexpr (IteratorTraits::Offset()) {
        _pay.template Hop<Input>();
      }
    }
    _next = static_cast<uint32_t>(block);
    _pos.template SeekBlock<Input>(_next);
    if constexpr (IteratorTraits::Offset()) {
      _pay.template SeekBlock<Input>(_next);
    }
  }

  void Skip(uint64_t count) {
    SDB_ASSERT(count != 0);
    const uint64_t left = pos_limits::kBlockSize - _buf_pos;
    if (count > left) {
      count -= left;
      Enter(_next + count / pos_limits::kBlockSize);
      count %= pos_limits::kBlockSize;
      if (count == 0) {
        _buf_pos = pos_limits::kBlockSize;
      } else {
        ReadBlock();
        _buf_pos = 0;
      }
    }
    _buf_pos += count;
    SDB_ASSERT(_buf_pos <= pos_limits::kBlockSize);
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
    if (_pos.viewed) [[likely]] {
      ReadBlockFrom<&Stream::view>();
    } else {
      ReadBlockFrom<&Stream::in>();
    }
  }

  template<auto Input>
  IRS_FORCE_INLINE void ReadBlockFrom() {
    if (_pos.header == nullptr) [[unlikely]] {
      EnterFrom<Input>(_next);
    } else if (_next == PosGroup::kBlocks) [[unlikely]] {
      _pos.template Step<Input>();
      if constexpr (IteratorTraits::Offset()) {
        _pay.template Step<Input>();
      }
      _next = 0;
    }
    Decode<Input>();
    ++_next;
  }

  template<auto Input>
  IRS_FORCE_INLINE void Decode() {
    auto& pos = _pos.*Input;
    _pos.hint.Advance(pos, pos.Position());
    block_io::ReadBlock(pos, _enc_buf, _pos_deltas);
    if constexpr (IteratorTraits::Offset()) {
      auto& pay = _pay.*Input;
      _pay.hint.Advance(pay, pay.Position());
      block_io::ReadBlock(pay, _enc_buf, _offs_start_deltas);
      block_io::ReadBlock(pay, _enc_buf, _offs_lengths);
    }
  }

  template<typename T>
  using ForOffset = utils::Need<IteratorTraits::Offset(), T>;

  uint32_t _pos_deltas[pos_limits::kBlockSize];
  [[no_unique_address]] ForOffset<uint32_t[pos_limits::kBlockSize]>
    _offs_start_deltas;
  [[no_unique_address]] ForOffset<uint32_t[pos_limits::kBlockSize]>
    _offs_lengths;
  uint32_t _freq = 0;  // length of the posting list for a document
  uint32_t _next = 0;
  uint32_t* _enc_buf;      // auxillary buffer to decode data
  uint64_t _pend_pos = 0;  // how many positions "behind" we are
  uint64_t _buf_pos = pos_limits::kBlockSize;  // position in pos_deltas_
  Cookie _cookie;
  Stream _pos;
  [[no_unique_address]] ForOffset<Stream> _pay;
  [[no_unique_address]] ForOffset<OffsAttr> _offs;
};

// use base PosAttr type for ancestors
template<typename IteratorTraits>
struct Type<PositionImpl<IteratorTraits>> : Type<PosAttr> {};

}  // namespace irs
