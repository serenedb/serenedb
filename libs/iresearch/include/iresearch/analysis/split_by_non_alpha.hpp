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

#include <absl/strings/ascii.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

namespace irs::analysis {

template<typename Mask>
inline Mask ClassifyAlnumBlock(const char* block) noexcept {
  const auto as_mask = [](auto value) noexcept {
    return static_cast<Mask>(value);
  };
  Mask bitmask = 0;
  for (unsigned i = 0; i < std::numeric_limits<Mask>::digits; ++i) {
    const unsigned char byte = static_cast<unsigned char>(block[i]);
    const unsigned char lowered = byte | 0x20;
    const uint32_t is_digit = static_cast<unsigned char>(byte - '0') < 10;
    const uint32_t is_letter = static_cast<unsigned char>(lowered - 'a') < 26;
    bitmask = as_mask(bitmask | as_mask((is_digit | is_letter) << i));
  }
  return bitmask;
}

template<typename EmitFn>
class AlnumTokenAssembler {
 public:
  AlnumTokenAssembler(const char* input, EmitFn& emit) noexcept
    : _input{input}, _emit{emit} {}

  template<typename Mask>
  void ConsumeBlock(Mask bitmask, size_t offset) {
    constexpr int kBits = std::numeric_limits<Mask>::digits;
    // Just not to bloat
    const auto as_mask = [](auto value) noexcept {
      return static_cast<Mask>(value);
    };

    if (_token_open) {
      const auto separators = as_mask(~bitmask);
      if (separators == 0) {
        return;
      }
      const auto token_end = as_mask(std::countr_zero(separators));
      CloseToken(offset + token_end);
      bitmask = as_mask(bitmask & ~LowBitsMask<Mask>(token_end));
    }

    while (bitmask != 0) {
      const auto token_begin = as_mask(std::countr_zero(bitmask));
      const auto run = as_mask(bitmask >> token_begin);
      const auto token_length = as_mask(std::countr_zero(as_mask(~run)));
      if (token_begin + token_length >= kBits) {
        OpenToken(offset + token_begin);
        return;
      }
      EmitToken(offset + token_begin, token_length);
      bitmask = as_mask(
        bitmask & ~as_mask(LowBitsMask<Mask>(token_length) << token_begin));
    }
  }

  void ConsumeTail(size_t begin, size_t end) {
    for (size_t pos = begin; pos < end; ++pos) {
      const bool alnum =
        absl::ascii_isalnum(static_cast<unsigned char>(_input[pos]));
      if (alnum && !_token_open) {
        OpenToken(pos);
      } else if (!alnum && _token_open) {
        CloseToken(pos);
      }
    }
  }

  void Finish(size_t input_end) {
    if (_token_open) {
      CloseToken(input_end);
    }
  }

 private:
  template<typename Mask>
  static Mask LowBitsMask(int count) noexcept {
    return static_cast<Mask>((Mask{1} << count) - 1);
  }

  void OpenToken(size_t begin) noexcept {
    _token_begin = begin;
    _token_open = true;
  }

  void CloseToken(size_t end) {
    _emit(std::string_view{_input + _token_begin, end - _token_begin});
    _token_open = false;
  }

  void EmitToken(size_t begin, int length) {
    _emit(std::string_view{_input + begin, static_cast<size_t>(length)});
  }

  const char* _input;
  EmitFn& _emit;
  size_t _token_begin = 0;
  bool _token_open = false;
};

template<typename EmitFn>
void SplitByNonAlpha(std::string_view data, EmitFn&& emit) {
  const char* const input = data.data();
  const size_t size = data.size();

  AlnumTokenAssembler assembler{input, emit};
  size_t offset = 0;
  while (size - offset >= 32) {
    assembler.template ConsumeBlock<uint32_t>(
      ClassifyAlnumBlock<uint32_t>(input + offset), offset);
    offset += 32;
  }
  if (size - offset >= 16) {
    assembler.template ConsumeBlock<uint16_t>(
      ClassifyAlnumBlock<uint16_t>(input + offset), offset);
    offset += 16;
  }
  if (size - offset >= 8) {
    assembler.template ConsumeBlock<uint8_t>(
      ClassifyAlnumBlock<uint8_t>(input + offset), offset);
    offset += 8;
  }
  assembler.ConsumeTail(offset, size);
  assembler.Finish(size);
}

}  // namespace irs::analysis
