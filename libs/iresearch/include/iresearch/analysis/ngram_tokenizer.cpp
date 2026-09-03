////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "ngram_tokenizer.hpp"

#include <simdutf.h>

#include <cstring>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_batch.hpp"

namespace irs::analysis {

NGramTokenizerBase::NGramTokenizerBase(Options&& options)
  : _options(std::move(options)) {
  _options.min_gram = std::max<size_t>(_options.min_gram, 1);
  _options.max_gram = std::max(_options.max_gram, _options.min_gram);
}

Tokenizer::ptr NGramTokenizerBase::Make(Options opts) {
  return std::make_unique<NGramTokenizer>(std::move(opts));
}

NGramTokenizer::NGramTokenizer(Options&& options)
  : NGramTokenizerBase{std::move(options)} {}

std::tuple<bool, NGramTokenizerBase::Kernel, bool> NGramTokenizer::PrepareBatch(
  BlockTraits traits) const {
  const auto kernel = [&] {
    switch (_options.ngram_mode) {
      case NGramMode::All:
        return _options.min_gram == _options.max_gram ? Kernel::AllFixed
                                                      : Kernel::AllVariable;
      case NGramMode::Prefix:
        return Kernel::Prefix;
      case NGramMode::Suffix:
        return Kernel::Suffix;
      case NGramMode::PrefixAndSuffix:
        return Kernel::PrefixAndSuffix;
    }
    return Kernel::AllVariable;
  }();
  return {PlainFill(), kernel,
          _options.stream_bytes_type == InputType::Binary || traits.ascii};
}

namespace {

using Options = NGramTokenizerBase::Options;

enum class EmitOriginal {
  None,
  WithoutMarkers,
  WithStartMarker,
  WithEndMarker,
};

template<TokenLayout Layout, bool Identity>
struct GramSink {
  TokenSink& sink;
  const byte_type* base;
  uint32_t data_size;
  const uint32_t* bounds;

  uint32_t ByteOffset(uint32_t symbol) const noexcept {
    if constexpr (Identity) {
      return symbol;
    } else {
      return bounds[symbol];
    }
  }

  IRS_FORCE_INLINE void Emit(uint32_t off_start, uint32_t off_end,
                             uint32_t position) const {
    sink.EmitSlice<Layout>(base, base + data_size, Offs{off_start, off_end},
                           position);
  }

  void EmitConcat(bytes_view prefix, bytes_view suffix, uint32_t off_start,
                  uint32_t off_end, uint32_t position) const {
    const auto size = static_cast<uint32_t>(prefix.size() + suffix.size());
    sink.Emit<Layout>(
      size,
      [&](byte_type* mem) IRS_FORCE_INLINE {
        std::memcpy(mem, prefix.data(), prefix.size());
        std::memcpy(mem + prefix.size(), suffix.data(), suffix.size());
        return size;
      },
      position, Offs{off_start, off_end});
  }
};

template<TokenLayout Layout, bool Identity>
IRS_FORCE_INLINE void EmitOriginalStep(const GramSink<Layout, Identity>& grams,
                                       bytes_view start_marker,
                                       bytes_view end_marker,
                                       EmitOriginal& pending,
                                       uint32_t position) {
  switch (pending) {
    case EmitOriginal::WithoutMarkers:
      grams.Emit(0, grams.data_size, position);
      pending = EmitOriginal::None;
      break;
    case EmitOriginal::WithEndMarker:
      grams.EmitConcat({grams.base, grams.data_size}, end_marker, 0,
                       grams.data_size, position);
      pending = EmitOriginal::None;
      break;
    case EmitOriginal::WithStartMarker:
      grams.EmitConcat(start_marker, {grams.base, grams.data_size}, 0,
                       grams.data_size, position);
      pending =
        end_marker.empty() ? EmitOriginal::None : EmitOriginal::WithEndMarker;
      break;
    case EmitOriginal::None:
      SDB_ASSERT(false);
      break;
  }
}

template<TokenLayout Layout, bool Identity>
IRS_FORCE_INLINE void EmitPrefixGrams(const Options& options,
                                      const GramSink<Layout, Identity>& grams,
                                      uint32_t nsym) {
  const size_t max_sym = std::min<size_t>(options.max_gram, nsym);
  for (size_t length = options.min_gram; length <= max_sym; ++length) {
    grams.Emit(0, grams.ByteOffset(static_cast<uint32_t>(length)), 1);
  }
}

template<TokenLayout Layout, bool Identity, bool DrainOriginal>
IRS_FORCE_INLINE void EmitMarkedPrefixGrams(
  const Options& options, const GramSink<Layout, Identity>& grams,
  uint32_t nsym, EmitOriginal& pending) {
  const auto* base = grams.base;
  const uint32_t data_size = grams.data_size;
  const bytes_view start_marker = options.start_marker;
  const bytes_view end_marker = options.end_marker;
  const auto max_sym =
    static_cast<uint32_t>(std::min<size_t>(options.max_gram, nsym));
  if (options.min_gram <= max_sym) {
    const auto min_sym = static_cast<uint32_t>(options.min_gram);
    const uint32_t interior_max = std::min(max_sym, nsym - 1);
    if (!start_marker.empty()) {
      for (uint32_t length = min_sym; length <= interior_max; ++length) {
        const uint32_t end_off = grams.ByteOffset(length);
        grams.EmitConcat(start_marker, bytes_view{base, end_off}, 0, end_off,
                         1);
      }
    } else if (min_sym <= interior_max) {
      const auto slots = [grams, min_sym](size_t j) IRS_FORCE_INLINE {
        return EmitKSlotPos{
          0, grams.ByteOffset(min_sym + static_cast<uint32_t>(j)), 1};
      };
      grams.sink.template EmitK<Layout>(interior_max - min_sym + 1, base,
                                        base + data_size, slots);
    }
    if (max_sym == nsym) {
      if (pending != EmitOriginal::None) {
        EmitOriginalStep(grams, start_marker, end_marker, pending, 1);
      } else if (!start_marker.empty()) {
        grams.EmitConcat(start_marker, bytes_view{base, data_size}, 0,
                         data_size, 1);
        if (!end_marker.empty()) {
          pending = EmitOriginal::WithEndMarker;
        }
      } else if (!end_marker.empty()) {
        grams.EmitConcat(bytes_view{base, data_size}, end_marker, 0, data_size,
                         1);
      } else {
        grams.Emit(0, data_size, 1);
      }
    }
  }
  if constexpr (DrainOriginal) {
    while (pending != EmitOriginal::None) {
      EmitOriginalStep(grams, start_marker, end_marker, pending, 1);
    }
  }
}

template<TokenLayout Layout, bool Identity>
auto FixedGramSlots(const GramSink<Layout, Identity>& grams, uint32_t min_sym,
                    uint32_t first) {
  return [grams, min_sym, first](size_t j) IRS_FORCE_INLINE {
    const auto start = first + static_cast<uint32_t>(j);
    return EmitKSlotPos{grams.ByteOffset(start),
                        grams.ByteOffset(start + min_sym), start + 1};
  };
}

template<TokenLayout Layout, bool Identity, bool Marked>
IRS_FORCE_INLINE void EmitFixedGrams(const Options& options,
                                     const GramSink<Layout, Identity>& grams,
                                     uint32_t nsym) {
  SDB_ASSERT(options.min_gram == options.max_gram);
  if (options.min_gram > nsym) {
    return;
  }
  constexpr uint32_t first = Marked ? 1 : 0;
  const auto* base = grams.base;
  const auto min_sym = static_cast<uint32_t>(options.min_gram);
  const uint32_t data_size = grams.data_size;
  bytes_view end_marker;
  uint32_t count = nsym - min_sym + 1 - first;
  bool tail_marked = false;
  if constexpr (Marked) {
    end_marker = options.end_marker;
    tail_marked = !end_marker.empty() && count > 0;
    if (tail_marked) {
      --count;
    }
  }
  grams.sink.template EmitK<Layout>(count, base, base + data_size,
                                    FixedGramSlots(grams, min_sym, first));
  if constexpr (Marked) {
    if (tail_marked) {
      const uint32_t start = first + count;
      const uint32_t off = grams.ByteOffset(start);
      grams.EmitConcat(bytes_view{base + off, data_size - off}, end_marker, off,
                       data_size, start + 1);
    }
  }
}

template<TokenLayout Layout, bool Identity>
struct VariableGramSlots {
  GramSink<Layout, Identity> grams;
  uint32_t nsym;
  size_t max_gram;
  uint32_t min_sym;
  uint32_t start;
  uint32_t len;
  uint32_t max_sym;

  IRS_FORCE_INLINE EmitKSlotPos operator()(size_t) {
    const EmitKSlotPos t{grams.ByteOffset(start), grams.ByteOffset(start + len),
                         start + 1};
    if (len == max_sym) {
      ++start;
      len = min_sym;
      max_sym = static_cast<uint32_t>(std::min<size_t>(max_gram, nsym - start));
    } else {
      ++len;
    }
    return t;
  }
};

template<TokenLayout Layout, bool Identity>
IRS_FORCE_INLINE void EmitVariableGrams(const Options& options,
                                        const GramSink<Layout, Identity>& grams,
                                        uint32_t nsym) {
  if (options.min_gram > nsym) {
    return;
  }
  const auto* base = grams.base;
  const auto min_sym = static_cast<uint32_t>(options.min_gram);
  const size_t max_gram = options.max_gram;
  const size_t total = static_cast<size_t>(nsym) + 1;
  const size_t full = total > max_gram ? total - max_gram : 0;
  const size_t tail = (total - min_sym) - full;
  const size_t k = full * (max_gram - min_sym + 1) + tail * (tail + 1) / 2;
  VariableGramSlots<Layout, Identity> slots{
    grams,
    nsym,
    max_gram,
    min_sym,
    0,
    min_sym,
    static_cast<uint32_t>(std::min<size_t>(max_gram, nsym))};
  grams.sink.template EmitK<Layout>(k, base, base + grams.data_size, slots);
}

template<TokenLayout Layout, bool Identity>
IRS_FORCE_INLINE void EmitMarkedVariableGrams(
  const Options& options, const GramSink<Layout, Identity>& grams,
  uint32_t nsym) {
  if (options.min_gram > nsym) {
    return;
  }
  const auto* base = grams.base;
  const auto min_sym = static_cast<uint32_t>(options.min_gram);
  const size_t max_gram = options.max_gram;
  const bytes_view end_marker = options.end_marker;
  for (uint32_t start = 1; start + min_sym <= nsym; ++start) {
    const auto max_sym =
      static_cast<uint32_t>(std::min<size_t>(max_gram, nsym - start));
    const uint32_t ngrams = max_sym - min_sym + 1;
    const uint32_t off_start = grams.ByteOffset(start);
    const uint32_t position = start + 1;
    const auto slots = [grams, min_sym, start, off_start,
                        position](size_t j) IRS_FORCE_INLINE {
      const auto len_sym = static_cast<uint32_t>(min_sym + j);
      return EmitKSlotPos{off_start, grams.ByteOffset(start + len_sym),
                          position};
    };
    const bool tail_marked = !end_marker.empty() && start + max_sym == nsym;
    grams.sink.template EmitK<Layout>(tail_marked ? ngrams - 1 : ngrams, base,
                                      base + grams.data_size, slots);
    if (tail_marked) {
      const uint32_t end_off = grams.ByteOffset(start + max_sym);
      grams.EmitConcat(bytes_view{base + off_start, end_off - off_start},
                       end_marker, off_start, end_off, position);
    }
  }
}

template<TokenLayout Layout, bool Identity, bool WithPrefix>
IRS_FORCE_INLINE void EmitSuffixGrams(const Options& options,
                                      const GramSink<Layout, Identity>& grams,
                                      uint32_t nsym) {
  const uint32_t data_size = grams.data_size;
  const uint32_t suffix_pos = WithPrefix ? 2u : 1u;
  const size_t max_len = std::min<size_t>(options.max_gram, nsym);
  for (size_t length = options.min_gram; length <= max_len; ++length) {
    const uint32_t off_start =
      grams.ByteOffset(static_cast<uint32_t>(nsym - length));
    if constexpr (WithPrefix) {
      if (off_start == 0) {
        break;
      }
    }
    grams.Emit(off_start, data_size, suffix_pos);
  }
}

template<TokenLayout Layout, bool Identity, bool WithPrefix>
void EmitMarkedSuffixGrams(const Options& options,
                           const GramSink<Layout, Identity>& grams,
                           uint32_t nsym, EmitOriginal& pending) {
  const auto* base = grams.base;
  const uint32_t data_size = grams.data_size;
  const bytes_view start_marker = options.start_marker;
  const bytes_view end_marker = options.end_marker;
  const uint32_t suffix_pos = WithPrefix ? 2u : 1u;
  const size_t max_len = std::min<size_t>(options.max_gram, nsym);
  for (size_t length = options.min_gram; length <= max_len; ++length) {
    const uint32_t off_start =
      grams.ByteOffset(static_cast<uint32_t>(nsym - length));
    const uint32_t len = data_size - off_start;
    const bool whole = off_start == 0;
    if constexpr (WithPrefix) {
      if (whole) {
        break;
      }
    }
    if (whole && pending != EmitOriginal::None) {
      break;
    }
    const bool marked_whole = whole && !start_marker.empty();
    if (!marked_whole && end_marker.empty()) {
      grams.Emit(off_start, data_size, suffix_pos);
    } else if (marked_whole) {
      grams.EmitConcat(start_marker, bytes_view{base + off_start, len},
                       off_start, data_size, suffix_pos);
      if (!end_marker.empty()) {
        pending = EmitOriginal::WithEndMarker;
      }
    } else {
      grams.EmitConcat(bytes_view{base + off_start, len}, end_marker, off_start,
                       data_size, suffix_pos);
    }
  }
  const uint32_t orig_pos =
    WithPrefix && options.min_gram > nsym ? 1u : suffix_pos;
  while (pending != EmitOriginal::None) {
    EmitOriginalStep(grams, start_marker, end_marker, pending, orig_pos);
  }
}

}  // namespace

template<TokenLayout Layout, bool Plain, NGramTokenizerBase::Kernel K,
         bool KnownAscii>
bool NGramTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  constexpr bool Identity = KnownAscii;
  const auto* base = reinterpret_cast<const byte_type*>(raw.GetData());
  const uint32_t size = raw.GetSize();
  if (size == 0) {
    return true;
  }
  const uint32_t* bounds = nullptr;
  uint32_t nsym = size;
  if constexpr (!Identity) {
    nsym = static_cast<uint32_t>(classify::BuildUtf8CpBounds(
      base, size,
      simdutf::validate_utf8(reinterpret_cast<const char*>(base), size),
      _fill_bounds));
    bounds = _fill_bounds.data();
  }
  const GramSink<Layout, Identity> gram_sink{sink, base, size, bounds};
  if (nsym == 0) {
    return true;
  }
  EmitOriginal pending = EmitOriginal::None;
  if constexpr (!Plain) {
    if (_options.preserve_original) {
      pending = !_options.start_marker.empty() ? EmitOriginal::WithStartMarker
                : !_options.end_marker.empty() ? EmitOriginal::WithEndMarker
                                               : EmitOriginal::WithoutMarkers;
    }
  }

  if constexpr (K == Kernel::AllFixed) {
    if constexpr (!Plain) {
      EmitMarkedPrefixGrams<Layout, Identity, true>(_options, gram_sink, nsym,
                                                    pending);
    }
    EmitFixedGrams<Layout, Identity, !Plain>(_options, gram_sink, nsym);
  } else if constexpr (K == Kernel::AllVariable && Plain) {
    EmitVariableGrams<Layout, Identity>(_options, gram_sink, nsym);
  } else if constexpr (K == Kernel::AllVariable) {
    EmitMarkedPrefixGrams<Layout, Identity, true>(_options, gram_sink, nsym,
                                                  pending);
    EmitMarkedVariableGrams<Layout, Identity>(_options, gram_sink, nsym);
  } else if constexpr (K == Kernel::Prefix && Plain) {
    EmitPrefixGrams<Layout, Identity>(_options, gram_sink, nsym);
  } else if constexpr (K == Kernel::Prefix) {
    EmitMarkedPrefixGrams<Layout, Identity, true>(_options, gram_sink, nsym,
                                                  pending);
  } else if constexpr (K == Kernel::Suffix && Plain) {
    EmitSuffixGrams<Layout, Identity, false>(_options, gram_sink, nsym);
  } else if constexpr (K == Kernel::Suffix) {
    EmitMarkedSuffixGrams<Layout, Identity, false>(_options, gram_sink, nsym,
                                                   pending);
  } else if constexpr (Plain) {
    EmitPrefixGrams<Layout, Identity>(_options, gram_sink, nsym);
    EmitSuffixGrams<Layout, Identity, true>(_options, gram_sink, nsym);
  } else {
    EmitMarkedPrefixGrams<Layout, Identity, false>(_options, gram_sink, nsym,
                                                   pending);
    EmitMarkedSuffixGrams<Layout, Identity, true>(_options, gram_sink, nsym,
                                                  pending);
  }
  return true;
}

template class TypedTokenizer<NGramTokenizer>;

}  // namespace irs::analysis
