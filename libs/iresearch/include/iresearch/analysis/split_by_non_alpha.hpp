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
#include <string>
#include <string_view>

namespace irs::analysis {

inline constexpr size_t kAlnumBlockBytes = 32;

inline uint32_t ClassifyAlnumBlock(const char* block) noexcept {
  uint32_t bitmask = 0;
  for (uint32_t i = 0; i < kAlnumBlockBytes; ++i) {
    const unsigned char byte = static_cast<unsigned char>(block[i]);
    const unsigned char lowered = byte | 0x20;
    const uint32_t is_digit = static_cast<unsigned char>(byte - '0') < 10;
    const uint32_t is_letter = static_cast<unsigned char>(lowered - 'a') < 26;
    bitmask |= (is_digit | is_letter) << i;
  }
  return bitmask;
}

template<typename EmitFn>
class AlnumTokenAssembler {
 public:
  AlnumTokenAssembler(const char* input, EmitFn& emit) noexcept
    : _input{input}, _emit{emit} {}

  void ConsumeBlock(uint32_t bitmask, size_t offset) {
    if (_token_open) {
      const uint32_t separators = ~bitmask;
      if (separators == 0) {
        return;
      }
      const int token_end = std::countr_zero(separators);
      CloseToken(offset + token_end);
      bitmask &= ~LowBitsMask(token_end);
    }

    while (bitmask != 0) {
      const int token_begin = std::countr_zero(bitmask);
      const int token_length = std::countr_zero(~(bitmask >> token_begin));
      if (token_begin + token_length >= static_cast<int>(kAlnumBlockBytes)) {
        OpenToken(offset + token_begin);
        return;
      }
      EmitToken(offset + token_begin, token_length);
      bitmask &= ~(LowBitsMask(token_length) << token_begin);
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
  static uint32_t LowBitsMask(int count) noexcept {
    return (uint32_t{1} << count) - 1;
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
  const size_t whole_blocks_size = size & ~(kAlnumBlockBytes - 1);

  AlnumTokenAssembler assembler{input, emit};
  for (size_t offset = 0; offset < whole_blocks_size;
       offset += kAlnumBlockBytes) {
    assembler.ConsumeBlock(ClassifyAlnumBlock(input + offset), offset);
  }
  assembler.ConsumeTail(whole_blocks_size, size);
  assembler.Finish(size);
}

template<typename EmitFn>
void SplitByNonAlpha(std::string_view data, bool to_lower, std::string& buf,
                     EmitFn&& emit) {
  SplitByNonAlpha(data, [&](std::string_view token) {
    if (to_lower) {
      buf.resize(token.size());
      absl::ascii_internal::AsciiStrToLower(buf.data(), token.data(),
                                            token.size());
      emit(std::string_view{buf});
    } else {
      emit(token);
    }
  });
}

}  // namespace irs::analysis
