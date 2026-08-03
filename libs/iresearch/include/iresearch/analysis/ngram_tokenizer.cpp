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
#include <string_view>

#include "iresearch/analysis/classify.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

template<NGramTokenizerBase::InputType StreamType>
Tokenizer::ptr NGramTokenizer<StreamType>::make(
  NGramTokenizerBase::Options&& options) {
  return std::make_unique<NGramTokenizer<StreamType>>(std::move(options));
}

NGramTokenizerBase::NGramTokenizerBase(Options&& options)
  : _options(std::move(options)) {
  _options.min_gram = std::max<size_t>(_options.min_gram, 1);
  _options.max_gram = std::max(_options.max_gram, _options.min_gram);
}

Tokenizer::ptr NGramTokenizerBase::Make(Options opts) {
  const auto stream_bytes_type = opts.stream_bytes_type;
  switch (stream_bytes_type) {
    case NGramTokenizerBase::InputType::Binary:
      return NGramTokenizer<NGramTokenizerBase::InputType::Binary>::make(
        std::move(opts));
    case NGramTokenizerBase::InputType::UTF8:
      return NGramTokenizer<NGramTokenizerBase::InputType::UTF8>::make(
        std::move(opts));
  }
  THROW_SQL_ERROR(ERR_MSG("ngram: unsupported input type"));
}

template<NGramTokenizerBase::InputType StreamType>
NGramTokenizer<StreamType>::NGramTokenizer(
  NGramTokenizerBase::Options&& options)
  : NGramTokenizerBase{std::move(options)} {
  SDB_ASSERT(StreamType == _options.stream_bytes_type);
}

void NGramTokenizerBase::BuildBoundaries(bytes_view data) {
  const auto* base = data.data();
  const size_t size = data.size();
  BuildUtf8CpBounds(
    base, size,
    simdutf::validate_utf8(reinterpret_cast<const char*>(base), size),
    _fill_bounds);
}

namespace {

using Options = NGramTokenizerBase::Options;
using NGramMode = NGramTokenizerBase::NGramMode;

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

  void Emit(uint32_t off_start, uint32_t off_end, uint32_t position) const {
    sink.Emit<Layout>(MakeTermView(base + off_start, off_end - off_start),
                      position, Offs{off_start, off_end});
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
void EmitOriginalStep(const GramSink<Layout, Identity>& grams, bytes_view data,
                      bytes_view start_marker, bytes_view end_marker,
                      EmitOriginal& pending, uint32_t position) {
  switch (pending) {
    case EmitOriginal::WithoutMarkers:
      grams.Emit(0, grams.data_size, position);
      pending = EmitOriginal::None;
      break;
    case EmitOriginal::WithEndMarker:
      grams.EmitConcat(data, end_marker, 0, grams.data_size, position);
      pending = EmitOriginal::None;
      break;
    case EmitOriginal::WithStartMarker:
      grams.EmitConcat(start_marker, data, 0, grams.data_size, position);
      pending =
        end_marker.empty() ? EmitOriginal::None : EmitOriginal::WithEndMarker;
      break;
    case EmitOriginal::None:
      SDB_ASSERT(false);
      break;
  }
}

template<TokenLayout Layout, bool Identity, bool Plain, NGramMode Mode>
void EmitPrefixGrams(const Options& options,
                     const GramSink<Layout, Identity>& grams, bytes_view data,
                     uint32_t nsym, EmitOriginal& pending) {
  const auto* base = grams.base;
  const uint32_t data_size = grams.data_size;
  const bytes_view start_marker = options.start_marker;
  const bytes_view end_marker = options.end_marker;
  const size_t max_sym = std::min<size_t>(options.max_gram, nsym);
  for (size_t length = options.min_gram; length <= max_sym; ++length) {
    const uint32_t end_off = grams.ByteOffset(static_cast<uint32_t>(length));
    if constexpr (Plain) {
      grams.Emit(0, end_off, 1);
    } else if (pending == EmitOriginal::None || end_off != data_size) {
      if (start_marker.empty() &&
          (end_marker.empty() || end_off != data_size)) {
        grams.Emit(0, end_off, 1);
      } else if (!start_marker.empty()) {
        grams.EmitConcat(start_marker, bytes_view{base, end_off}, 0, end_off,
                         1);
        if (end_off == data_size && !end_marker.empty()) {
          pending = EmitOriginal::WithEndMarker;
        }
      } else {
        grams.EmitConcat(bytes_view{base, end_off}, end_marker, 0, end_off, 1);
      }
    } else {
      EmitOriginalStep(grams, data, start_marker, end_marker, pending, 1);
    }
  }
  if constexpr (!Plain && Mode != NGramMode::PrefixAndSuffix) {
    while (pending != EmitOriginal::None) {
      EmitOriginalStep(grams, data, start_marker, end_marker, pending, 1);
    }
  }
}

template<TokenLayout Layout, bool Identity, bool Plain>
void EmitInteriorGrams(const Options& options,
                       const GramSink<Layout, Identity>& grams, uint32_t nsym) {
  if (options.min_gram > nsym) {
    return;
  }
  const auto* base = grams.base;
  const uint32_t data_size = grams.data_size;
  const bytes_view end_marker = options.end_marker;
  const size_t max_gram = options.max_gram;
  const auto min_sym = static_cast<uint32_t>(options.min_gram);

  if (options.min_gram == max_gram) {
    // Fixed-gram flat loop over positions 1..nsym-min_sym: one gram per
    // position, ramp positions/offsets, bulk slot claims.
    uint32_t count = nsym - min_sym;
    const bool tail_marked = !Plain && !end_marker.empty() && count > 0;
    if (tail_marked) {
      --count;
    }
    grams.sink.template EmitK<Layout>(
      count, base,
      [grams, min_sym](size_t j) {
        const auto start = static_cast<uint32_t>(1 + j);
        return Offs{grams.ByteOffset(start), grams.ByteOffset(start + min_sym)};
      },
      PosSeq{2});
    if (tail_marked) {
      const uint32_t start = 1 + count;
      const uint32_t off = grams.ByteOffset(start);
      grams.EmitConcat(bytes_view{base + off, data_size - off}, end_marker, off,
                       data_size, start + 1);
    }
    return;
  }

  // Variable gram lengths: bulk-claim the grams of each position.
  for (uint32_t start = 1; start + min_sym <= nsym; ++start) {
    const auto max_sym =
      static_cast<uint32_t>(std::min<size_t>(max_gram, nsym - start));
    const uint32_t ngrams = max_sym - min_sym + 1;
    const uint32_t off_start = grams.ByteOffset(start);
    const uint32_t position = start + 1;
    const bool tail_marked =
      !Plain && !end_marker.empty() && start + max_sym == nsym;
    grams.sink.template EmitK<Layout>(
      tail_marked ? ngrams - 1 : ngrams, base,
      [grams, min_sym, start, off_start](size_t j) {
        const auto len_sym = static_cast<uint32_t>(min_sym + j);
        return Offs{off_start, grams.ByteOffset(start + len_sym)};
      },
      Pos{position});
    if (tail_marked) {
      const uint32_t end_off = grams.ByteOffset(start + max_sym);
      grams.EmitConcat(bytes_view{base + off_start, end_off - off_start},
                       end_marker, off_start, end_off, position);
    }
  }
}

template<TokenLayout Layout, bool Identity, bool Plain, NGramMode Mode>
void EmitSuffixGrams(const Options& options,
                     const GramSink<Layout, Identity>& grams, bytes_view data,
                     uint32_t nsym, EmitOriginal& pending) {
  const auto* base = grams.base;
  const uint32_t data_size = grams.data_size;
  const bytes_view start_marker = options.start_marker;
  const bytes_view end_marker = options.end_marker;
  const uint32_t suffix_pos = Mode == NGramMode::PrefixAndSuffix ? 2u : 1u;
  const size_t max_len = std::min<size_t>(options.max_gram, nsym);
  for (size_t length = options.min_gram; length <= max_len; ++length) {
    const uint32_t off_start =
      grams.ByteOffset(static_cast<uint32_t>(nsym - length));
    const uint32_t len = data_size - off_start;
    const bool whole = off_start == 0;
    if constexpr (Mode == NGramMode::PrefixAndSuffix) {
      if (whole) {
        break;
      }
    }
    if constexpr (Plain) {
      grams.Emit(off_start, data_size, suffix_pos);
    } else if (whole && pending != EmitOriginal::None) {
      break;
    } else if ((!whole || start_marker.empty()) && end_marker.empty()) {
      grams.Emit(off_start, data_size, suffix_pos);
    } else if (whole && !start_marker.empty()) {
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
  if constexpr (!Plain) {
    const uint32_t orig_pos =
      Mode == NGramMode::PrefixAndSuffix && options.min_gram > nsym
        ? 1u
        : suffix_pos;
    while (pending != EmitOriginal::None) {
      EmitOriginalStep(grams, data, start_marker, end_marker, pending,
                       orig_pos);
    }
  }
}

}  // namespace

template<TokenLayout Layout, bool Identity, bool Plain,
         NGramTokenizerBase::NGramMode Mode>
void NGramTokenizerBase::EmitGrams(TokenSink& sink, bytes_view data,
                                   const uint32_t* bounds, uint32_t nsym) {
  if (nsym == 0) {
    return;
  }
  const GramSink<Layout, Identity> gram_sink{
    sink, data.data(), static_cast<uint32_t>(data.size()), bounds};
  EmitOriginal pending = EmitOriginal::None;
  if constexpr (!Plain) {
    if (_options.preserve_original) {
      pending = !_options.start_marker.empty() ? EmitOriginal::WithStartMarker
                : !_options.end_marker.empty() ? EmitOriginal::WithEndMarker
                                               : EmitOriginal::WithoutMarkers;
    }
  }

  if constexpr (Mode != NGramMode::Suffix) {
    EmitPrefixGrams<Layout, Identity, Plain, Mode>(_options, gram_sink, data,
                                                   nsym, pending);
  }
  if constexpr (Mode == NGramMode::All) {
    EmitInteriorGrams<Layout, Identity, Plain>(_options, gram_sink, nsym);
  } else if constexpr (Mode == NGramMode::Suffix ||
                       Mode == NGramMode::PrefixAndSuffix) {
    EmitSuffixGrams<Layout, Identity, Plain, Mode>(_options, gram_sink, data,
                                                   nsym, pending);
  }
}

template<NGramTokenizerBase::InputType StreamType>
template<TokenLayout Layout, bool Plain, NGramTokenizerBase::NGramMode Mode>
bool NGramTokenizer<StreamType>::DoFill(duckdb::string_t raw,
                                        TokenSink& sink) {
  const bytes_view data{reinterpret_cast<const byte_type*>(raw.GetData()),
                        raw.GetSize()};
  if constexpr (StreamType == InputType::Binary) {
    EmitGrams<Layout, true, Plain, Mode>(sink, data, nullptr,
                                         static_cast<uint32_t>(data.size()));
  } else {
    if (simdutf::validate_ascii(reinterpret_cast<const char*>(data.data()),
                                data.size())) {
      EmitGrams<Layout, true, Plain, Mode>(sink, data, nullptr,
                                           static_cast<uint32_t>(data.size()));
    } else {
      BuildBoundaries(data);
      EmitGrams<Layout, false, Plain, Mode>(
        sink, data, _fill_bounds.data(),
        static_cast<uint32_t>(_fill_bounds.size() - 1));
    }
  }
  return true;
}

template class NGramTokenizer<NGramTokenizerBase::InputType::Binary>;
template class NGramTokenizer<NGramTokenizerBase::InputType::UTF8>;
template class TypedTokenizer<
  NGramTokenizer<NGramTokenizerBase::InputType::Binary>>;
template class TypedTokenizer<
  NGramTokenizer<NGramTokenizerBase::InputType::UTF8>>;

}  // namespace irs::analysis
