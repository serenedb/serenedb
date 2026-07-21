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

#include <string_view>

#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
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

bool NGramTokenizerBase::Bind(std::string_view value) noexcept {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  _data = ViewCast<byte_type>(value);
  _data_end = _data.data() + _data.size();
  return true;
}

namespace {

template<typename Mask>
Mask ClassifyBoundaryBlock(const byte_type* block) noexcept {
  Mask bitmask = 0;
  for (int i = 0; i < std::numeric_limits<Mask>::digits; ++i) {
    bitmask |=
      static_cast<Mask>(static_cast<Mask>((block[i] & 0xC0) != 0x80) << i);
  }
  return bitmask;
}

}  // namespace

void NGramTokenizerBase::BuildBoundaries() {
  _fill_bounds.clear();
  const auto* base = _data.data();
  const size_t size = _data.size();
  if (simdutf::validate_utf8(reinterpret_cast<const char*>(base), size)) {
    size_t offset = 0;
    while (size - offset >= 32) {
      auto mask = ClassifyBoundaryBlock<uint32_t>(base + offset);
      while (mask != 0) {
        _fill_bounds.push_back(
          static_cast<uint32_t>(offset + std::countr_zero(mask)));
        mask &= mask - 1;
      }
      offset += 32;
    }
    for (; offset < size; ++offset) {
      if ((base[offset] & 0xC0) != 0x80) {
        _fill_bounds.push_back(static_cast<uint32_t>(offset));
      }
    }
  } else {
    for (const auto* it = base; it != _data_end;
         it = utf8_utils::Next(it, _data_end)) {
      _fill_bounds.push_back(static_cast<uint32_t>(it - base));
    }
  }
  _fill_bounds.push_back(static_cast<uint32_t>(size));
}

template<TokenLayout Layout, bool Identity>
void NGramTokenizerBase::EmitGrams(TokenEmitter& sink, const uint32_t* bounds,
                                   uint32_t nsym) {
  auto& buf = sink.buf;
  const auto* base = _data.data();
  const auto data_size = static_cast<uint32_t>(_data.size());
  const size_t min_gram = _options.min_gram;
  const size_t max_gram = _options.max_gram;
  const bytes_view start_marker = _options.start_marker;
  const bytes_view end_marker = _options.end_marker;

  const auto bnd = [&](uint32_t i) -> uint32_t {
    if constexpr (Identity) {
      return i;
    } else {
      return bounds[i];
    }
  };

  const auto emit = [&](bytes_view term, uint32_t off_start, uint32_t off_end,
                        uint32_t position, bool intern) {
    const auto i = sink.Next();
    buf.terms[i] = intern ? sink.Intern(term) : MakeTermView(term);
    if constexpr (Layout != TokenLayout::Terms) {
      buf.pos[i] = position;
    }
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = off_start;
      buf.offs_end[i] = off_end;
    }
  };

  const auto emit2 = [&](bytes_view prefix, bytes_view suffix,
                         uint32_t off_start, uint32_t off_end,
                         uint32_t position) {
    const auto i = sink.Next();
    buf.terms[i] = sink.Intern(prefix, suffix);
    if constexpr (Layout != TokenLayout::Terms) {
      buf.pos[i] = position;
    }
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = off_start;
      buf.offs_end[i] = off_end;
    }
  };

  EmitOriginal pending = EmitOriginal::None;
  if (_options.preserve_original) {
    pending = !start_marker.empty() ? EmitOriginal::WithStartMarker
              : !end_marker.empty() ? EmitOriginal::WithEndMarker
                                    : EmitOriginal::WithoutMarkers;
  }

  const auto emit_original_step = [&] {
    switch (pending) {
      case EmitOriginal::WithoutMarkers:
        emit(_data, 0, data_size, 1, false);
        pending = EmitOriginal::None;
        break;
      case EmitOriginal::WithEndMarker:
        emit2(_data, end_marker, 0, data_size, 1);
        pending = EmitOriginal::None;
        break;
      case EmitOriginal::WithStartMarker:
        emit2(start_marker, _data, 0, data_size, 1);
        pending =
          end_marker.empty() ? EmitOriginal::None : EmitOriginal::WithEndMarker;
        break;
      case EmitOriginal::None:
        SDB_ASSERT(false);
        break;
    }
  };

  if (nsym == 0) {
    return;
  }

  // Position 0: marker and preserve-original interplay, general path.
  {
    const size_t max_sym = std::min<size_t>(max_gram, nsym);
    for (size_t length = 1; length <= max_sym; ++length) {
      if (length < min_gram) {
        continue;
      }
      const uint32_t end_off = bnd(static_cast<uint32_t>(length));
      if (pending == EmitOriginal::None || end_off != data_size) {
        if (start_marker.empty() &&
            (end_marker.empty() || end_off != data_size)) {
          emit(bytes_view{base, end_off}, 0, end_off, 1, false);
        } else if (!start_marker.empty()) {
          emit2(start_marker, bytes_view{base, end_off}, 0, end_off, 1);
          if (end_off == data_size && !end_marker.empty()) {
            pending = EmitOriginal::WithEndMarker;
          }
        } else {
          emit2(bytes_view{base, end_off}, end_marker, 0, end_off, 1);
        }
      } else {
        emit_original_step();
      }
    }
    while (pending != EmitOriginal::None) {
      emit_original_step();
    }
  }

  if (min_gram > nsym) {
    return;
  }
  const auto mm = static_cast<uint32_t>(min_gram);

  if (min_gram == max_gram) {
    // Fixed-gram flat loop over positions 1..nsym-mm: one gram per
    // position, ramp positions/offsets, bulk slot claims.
    uint32_t count = nsym - mm;
    const bool tail_marked = !end_marker.empty() && count > 0;
    if (tail_marked) {
      --count;
    }
    uint32_t done = 0;
    while (done < count) {
      const auto slots = sink.Next(count - done);
      const auto first = static_cast<uint32_t>(slots.data() - buf.terms);
      for (size_t j = 0; j < slots.size(); ++j) {
        const auto s = static_cast<uint32_t>(1 + done + j);
        const uint32_t off = bnd(s);
        const uint32_t end = bnd(s + mm);
        slots[j] =
          MakeTermView(reinterpret_cast<const char*>(base + off), end - off);
        if constexpr (Layout != TokenLayout::Terms) {
          buf.pos[first + j] = s + 1;
        }
        if constexpr (Layout == TokenLayout::TermsPosOffs) {
          buf.offs_start[first + j] = off;
          buf.offs_end[first + j] = end;
        }
      }
      done += static_cast<uint32_t>(slots.size());
    }
    if (tail_marked) {
      const uint32_t s = 1 + count;
      const uint32_t off = bnd(s);
      emit2(bytes_view{base + off, data_size - off}, end_marker, off, data_size,
            s + 1);
    }
    return;
  }

  // Variable gram lengths: bulk-claim the k grams of each position.
  for (uint32_t s = 1; s + mm <= nsym; ++s) {
    const auto max_sym =
      static_cast<uint32_t>(std::min<size_t>(max_gram, nsym - s));
    const uint32_t k = max_sym - mm + 1;
    const uint32_t off_start = bnd(s);
    const uint32_t position = s + 1;
    const bool tail_marked = !end_marker.empty() && s + max_sym == nsym;
    uint32_t emitted = 0;
    while (emitted < k) {
      const auto slots = sink.Next(k - emitted);
      const auto first = static_cast<uint32_t>(slots.data() - buf.terms);
      for (size_t j = 0; j < slots.size(); ++j) {
        const auto len_sym = static_cast<uint32_t>(mm + emitted + j);
        const uint32_t end_off = bnd(s + len_sym);
        const uint32_t len = end_off - off_start;
        if (tail_marked && len_sym == max_sym) [[unlikely]] {
          slots[j] = sink.Intern(bytes_view{base + off_start, len}, end_marker);
        } else {
          slots[j] =
            MakeTermView(reinterpret_cast<const char*>(base + off_start), len);
        }
        if constexpr (Layout != TokenLayout::Terms) {
          buf.pos[first + j] = position;
        }
        if constexpr (Layout == TokenLayout::TermsPosOffs) {
          buf.offs_start[first + j] = off_start;
          buf.offs_end[first + j] = end_off;
        }
      }
      emitted += static_cast<uint32_t>(slots.size());
    }
  }
}

template<NGramTokenizerBase::InputType StreamType>
template<TokenLayout Layout>
bool NGramTokenizer<StreamType>::DoFill(std::string_view value,
                                        TokenEmitter& sink) {
  if (!Bind(value)) {
    return false;
  }
  if constexpr (StreamType == InputType::Binary) {
    EmitGrams<Layout, true>(sink, nullptr, static_cast<uint32_t>(_data.size()));
  } else {
    if (simdutf::validate_ascii(reinterpret_cast<const char*>(_data.data()),
                                _data.size())) {
      EmitGrams<Layout, true>(sink, nullptr,
                              static_cast<uint32_t>(_data.size()));
    } else {
      BuildBoundaries();
      EmitGrams<Layout, false>(sink, _fill_bounds.data(),
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
