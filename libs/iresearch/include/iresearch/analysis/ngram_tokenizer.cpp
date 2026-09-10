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
namespace {

using Options = NGramTokenizer::Options;
using Kernel = NGramTokenizer::Kernel;
using NGramMode = NGramTokenizer::NGramMode;

Kernel KernelFor(const Options& options) noexcept {
  switch (options.ngram_mode) {
    case NGramMode::All:
      return options.min_gram == options.max_gram ? Kernel::AllFixed
                                                  : Kernel::AllVariable;
    case NGramMode::Prefix:
      return Kernel::Prefix;
    case NGramMode::Suffix:
      return Kernel::Suffix;
    case NGramMode::PrefixAndSuffix:
      return Kernel::PrefixAndSuffix;
  }
  return Kernel::AllVariable;
}

}  // namespace

NGramTokenizer::NGramTokenizer(Options&& options)
  : _options(std::move(options)) {
  _options.min_gram = std::max<size_t>(_options.min_gram, 1);
  _options.max_gram = std::max(_options.max_gram, _options.min_gram);
  _kernel = KernelFor(_options);
}

Tokenizer::ptr NGramTokenizer::Make(Options opts) {
  return std::make_unique<NGramTokenizer>(std::move(opts));
}

std::tuple<bool, NGramTokenizer::Kernel, bool> NGramTokenizer::PrepareBatch(
  BlockTraits traits) const {
  return {PlainFill(), _kernel,
          _options.stream_bytes_type == InputType::Binary || traits.ascii};
}

namespace {

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

template<bool Plain>
IRS_FORCE_INLINE bytes_view MarkerView(const bstring& marker) noexcept {
  if constexpr (Plain) {
    return {};
  } else {
    return marker;
  }
}

template<TokenLayout Layout, bool Identity>
IRS_FORCE_INLINE void EmitMarkedOriginal(
  const GramSink<Layout, Identity>& grams, bytes_view start_marker,
  bytes_view end_marker, uint32_t position) {
  const uint32_t data_size = grams.data_size;
  if (start_marker.empty() && end_marker.empty()) {
    grams.Emit(0, data_size, position);
    return;
  }
  const bytes_view data{grams.base, data_size};
  if (!start_marker.empty()) {
    grams.EmitConcat(start_marker, data, 0, data_size, position);
  }
  if (!end_marker.empty()) {
    grams.EmitConcat(data, end_marker, 0, data_size, position);
  }
}

template<TokenLayout Layout, bool Identity, bool Plain>
IRS_FORCE_INLINE void EmitOriginal(const Options& options,
                                   const GramSink<Layout, Identity>& grams,
                                   uint32_t position) {
  if constexpr (Plain) {
    grams.Emit(0, grams.data_size, position);
  } else {
    EmitMarkedOriginal(grams, options.start_marker, options.end_marker,
                       position);
  }
}

template<TokenLayout Layout, bool Identity, bool Plain, bool DrainOriginal>
IRS_FORCE_INLINE void EmitPrefixGrams(const Options& options,
                                      const GramSink<Layout, Identity>& grams,
                                      uint32_t nsym, bool& original) {
  const auto* base = grams.base;
  const bytes_view start_marker = MarkerView<Plain>(options.start_marker);
  const auto max_sym =
    static_cast<uint32_t>(std::min<size_t>(options.max_gram, nsym));
  if (options.min_gram <= max_sym) {
    const auto min_sym = static_cast<uint32_t>(options.min_gram);
    const uint32_t interior_max = Plain ? max_sym : std::min(max_sym, nsym - 1);
    if constexpr (Plain) {
      for (uint32_t length = min_sym; length <= interior_max; ++length) {
        grams.Emit(0, grams.ByteOffset(length), 1);
      }
    } else if (!start_marker.empty()) {
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
                                        base + grams.data_size, slots);
    }
    if constexpr (!Plain) {
      if (max_sym == nsym) {
        EmitOriginal<Layout, Identity, Plain>(options, grams, 1);
        original = false;
      }
    }
  }
  if constexpr (DrainOriginal && !Plain) {
    if (original) {
      EmitOriginal<Layout, Identity, Plain>(options, grams, 1);
      original = false;
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

template<TokenLayout Layout, bool Identity, bool Plain>
IRS_FORCE_INLINE void EmitFixedGrams(const Options& options,
                                     const GramSink<Layout, Identity>& grams,
                                     uint32_t nsym) {
  SDB_ASSERT(options.min_gram == options.max_gram);
  if (options.min_gram > nsym) {
    return;
  }
  constexpr uint32_t first = Plain ? 0 : 1;
  const auto* base = grams.base;
  const auto min_sym = static_cast<uint32_t>(options.min_gram);
  const uint32_t data_size = grams.data_size;
  const bytes_view end_marker = MarkerView<Plain>(options.end_marker);
  uint32_t count = nsym - min_sym + 1 - first;
  const bool tail_marked = !end_marker.empty() && count > 0;
  if (tail_marked) {
    --count;
  }
  grams.sink.template EmitK<Layout>(count, base, base + data_size,
                                    FixedGramSlots(grams, min_sym, first));
  if (tail_marked) {
    const uint32_t start = first + count;
    const uint32_t off = grams.ByteOffset(start);
    grams.EmitConcat(bytes_view{base + off, data_size - off}, end_marker, off,
                     data_size, start + 1);
  }
}

template<TokenLayout Layout, bool Identity>
struct VariableGramRuns {
  GramSink<Layout, Identity> grams;
  uint32_t nsym;
  size_t max_gram;
  uint32_t min_sym;
  uint32_t start;

  IRS_FORCE_INLINE auto operator()() {
    const auto max_sym =
      static_cast<uint32_t>(std::min<size_t>(max_gram, nsym - start));
    const uint32_t count = max_sym - min_sym + 1;
    const uint32_t begin = grams.ByteOffset(start);
    const uint32_t first = start + min_sym;
    const uint32_t pos = start + 1;
    ++start;
    if constexpr (Identity) {
      return EmitRun{begin, first, count, pos};
    } else {
      return EmitRunEnds{begin, grams.bounds + first, count, pos};
    }
  }
};

template<TokenLayout Layout, bool Identity, bool Plain>
IRS_FORCE_INLINE void EmitVariableGrams(const Options& options,
                                        const GramSink<Layout, Identity>& grams,
                                        uint32_t nsym) {
  if (options.min_gram > nsym) {
    return;
  }
  const auto* base = grams.base;
  const auto min_sym = static_cast<uint32_t>(options.min_gram);
  const size_t max_gram = options.max_gram;
  if constexpr (Plain) {
    const size_t total = static_cast<size_t>(nsym) + 1;
    const size_t full = total > max_gram ? total - max_gram : 0;
    const size_t tail = (total - min_sym) - full;
    const size_t k = full * (max_gram - min_sym + 1) + tail * (tail + 1) / 2;
    VariableGramRuns<Layout, Identity> runs{grams, nsym, max_gram, min_sym, 0};
    grams.sink.template EmitRuns<Layout>(k, max_gram - min_sym + 1, base,
                                         base + grams.data_size, runs);
  } else {
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
}

template<TokenLayout Layout, bool Identity, bool Plain, bool WithPrefix>
IRS_FORCE_INLINE void EmitSuffixGrams(const Options& options,
                                      const GramSink<Layout, Identity>& grams,
                                      uint32_t nsym, bool& original) {
  const auto* base = grams.base;
  const uint32_t data_size = grams.data_size;
  const bytes_view end_marker = MarkerView<Plain>(options.end_marker);
  const size_t min_gram = options.min_gram;
  const size_t max_gram = options.max_gram;
  constexpr uint32_t suffix_pos = WithPrefix ? 2u : 1u;
  constexpr bool kWholeIsLongest = Plain && !WithPrefix;
  if constexpr (!kWholeIsLongest) {
    if constexpr (!WithPrefix) {
      if (min_gram <= nsym && nsym <= max_gram) {
        original = true;
      }
    }
    if (original) {
      EmitOriginal<Layout, Identity, Plain>(
        options, grams, WithPrefix && min_gram > nsym ? 1u : suffix_pos);
      original = false;
    }
  }
  const size_t longest =
    std::min<size_t>(max_gram, kWholeIsLongest ? nsym : nsym - 1);
  for (size_t len = longest; len >= min_gram; --len) {
    const uint32_t off_start =
      grams.ByteOffset(static_cast<uint32_t>(nsym - len));
    if (end_marker.empty()) {
      grams.Emit(off_start, data_size, suffix_pos);
    } else {
      grams.EmitConcat(bytes_view{base + off_start, data_size - off_start},
                       end_marker, off_start, data_size, suffix_pos);
    }
  }
}

}  // namespace

template<TokenLayout Layout, bool Plain, NGramTokenizer::Kernel K,
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
  bool original = !Plain && _options.preserve_original;
  if constexpr (K == Kernel::AllFixed) {
    if constexpr (!Plain) {
      EmitPrefixGrams<Layout, Identity, Plain, true>(_options, gram_sink, nsym,
                                                     original);
    }
    EmitFixedGrams<Layout, Identity, Plain>(_options, gram_sink, nsym);
  } else if constexpr (K == Kernel::AllVariable) {
    if constexpr (!Plain) {
      EmitPrefixGrams<Layout, Identity, Plain, true>(_options, gram_sink, nsym,
                                                     original);
    }
    EmitVariableGrams<Layout, Identity, Plain>(_options, gram_sink, nsym);
  } else if constexpr (K == Kernel::Prefix) {
    EmitPrefixGrams<Layout, Identity, Plain, true>(_options, gram_sink, nsym,
                                                   original);
  } else if constexpr (K == Kernel::Suffix) {
    EmitSuffixGrams<Layout, Identity, Plain, false>(_options, gram_sink, nsym,
                                                    original);
  } else {
    EmitPrefixGrams<Layout, Identity, Plain, false>(_options, gram_sink, nsym,
                                                    original);
    EmitSuffixGrams<Layout, Identity, Plain, true>(_options, gram_sink, nsym,
                                                   original);
  }
  return true;
}

template class TypedTokenizer<NGramTokenizer>;

}  // namespace irs::analysis
