////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "automaton.hpp"
#include "string.hpp"

namespace irs {
enum class RegexpType {
  Literal,  
  Prefix,  
  Complex,
};


enum RegexpMeta : byte_type {
  kDot = '.',      
  kStar = '*',     
  kPlus = '+',      
  kQuestion = '?', 
  kPipe = '|',      
  kLParen = '(',   
  kRParen = ')',   
  kLBracket = '[',  
  kRBracket = ']',  
  kCaret = '^',    
  kDollar = '$',   
  kEscape = '\\',  
};

constexpr bool IsRegexpMeta(byte_type c) noexcept {
  switch (c) {
    case kDot:
    case kStar:
    case kPlus:
    case kQuestion:
    case kPipe:
    case kLParen:
    case kRParen:
    case kLBracket:
    case kRBracket:
    case kCaret:
    case kDollar:
    case kEscape:
      return true;
    default:
      return false;
  }
}


RegexpType ComputeRegexpType(bytes_view pattern) noexcept;

bytes_view ExtractRegexpPrefix(bytes_view pattern) noexcept;

automaton FromRegexp(bytes_view pattern);

inline automaton FromRegexp(std::string_view pattern) {
  return FromRegexp(ViewCast<byte_type>(pattern));
}

}  // namespace irs
