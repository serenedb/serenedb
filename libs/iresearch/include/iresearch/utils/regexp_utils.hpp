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
