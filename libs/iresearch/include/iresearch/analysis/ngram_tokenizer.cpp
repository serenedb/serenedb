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

#include <string_view>

#include "basics/exceptions.h"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis {

template<NGramTokenizerBase::InputType StreamType>
Analyzer::ptr NGramTokenizer<StreamType>::make(Options&& options) {
  return std::make_unique<NGramTokenizer<StreamType>>(std::move(options));
}

NGramTokenizerBase::NGramTokenizerBase(Options&& options)
  : _options(std::move(options)),
    _start_marker_empty(_options.start_marker.empty()),
    _end_marker_empty(_options.end_marker.empty()) {
  _options.min_gram = std::max<size_t>(_options.min_gram, 1);
  _options.max_gram = std::max(_options.max_gram, _options.min_gram);
}

Analyzer::ptr NGramTokenizerBase::Make(Options opts) {
  const auto stream_bytes_type = opts.stream_bytes_type;
  switch (stream_bytes_type) {
    case NGramTokenizerBase::InputType::Binary:
      return NGramTokenizer<NGramTokenizerBase::InputType::Binary>::make(
        std::move(opts));
    case NGramTokenizerBase::InputType::UTF8:
      return NGramTokenizer<NGramTokenizerBase::InputType::UTF8>::make(
        std::move(opts));
  }
  SDB_THROW(sdb::ERROR_BAD_PARAMETER, "ngram: unsupported input type");
}

template<NGramTokenizerBase::InputType StreamType>
NGramTokenizer<StreamType>::NGramTokenizer(Options&& options)
  : NGramTokenizerBase{std::move(options)} {
  SDB_ASSERT(StreamType == _options.stream_bytes_type);
}

void NGramTokenizerBase::emit_original() noexcept {
  auto& term = std::get<TermAttr>(_attrs);
  auto& offset = std::get<OffsAttr>(_attrs);
  auto& inc = std::get<IncAttr>(_attrs);

  switch (_emit_original) {
    case EmitOriginal::WithoutMarkers:
      term.value = _data;
      SDB_ASSERT(_data.size() <= std::numeric_limits<uint32_t>::max());
      offset.end = uint32_t(_data.size());
      _emit_original = EmitOriginal::None;
      inc.value = _next_inc_val;
      break;
    case EmitOriginal::WithEndMarker:
      _marked_term_buffer.clear();
      SDB_ASSERT(_marked_term_buffer.capacity() >=
                 (_options.end_marker.size() + _data.size()));
      _marked_term_buffer.append(_data.data(), _data_end);
      _marked_term_buffer.append_range(_options.end_marker);
      term.value = _marked_term_buffer;
      SDB_ASSERT(_marked_term_buffer.size() <=
                 std::numeric_limits<uint32_t>::max());
      offset.start = 0;
      offset.end = uint32_t(_data.size());
      _emit_original = EmitOriginal::None;  // end marker is emitted last, so we
                                            // are done emitting original
      inc.value = _next_inc_val;
      break;
    case EmitOriginal::WithStartMarker:
      _marked_term_buffer.clear();
      SDB_ASSERT(_marked_term_buffer.capacity() >=
                 (_options.start_marker.size() + _data.size()));
      _marked_term_buffer.append_range(_options.start_marker);
      _marked_term_buffer.append(_data.data(), _data_end);
      term.value = _marked_term_buffer;
      SDB_ASSERT(_marked_term_buffer.size() <=
                 std::numeric_limits<uint32_t>::max());
      offset.start = 0;
      offset.end = uint32_t(_data.size());
      _emit_original = _options.end_marker.empty()
                         ? EmitOriginal::None
                         : EmitOriginal::WithEndMarker;
      inc.value = _next_inc_val;
      break;
    case EmitOriginal::None:
      SDB_ASSERT(false);
      break;
  }
  _next_inc_val = 0;
}

bool NGramTokenizerBase::reset(std::string_view value) noexcept {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    // can't handle data which is longer than
    // std::numeric_limits<uint32_t>::max()
    return false;
  }

  auto& term = std::get<TermAttr>(_attrs);
  auto& offset = std::get<OffsAttr>(_attrs);

  // reset term attribute
  term.value = {};

  // reset offset attribute
  offset.start = std::numeric_limits<uint32_t>::max();
  offset.end = std::numeric_limits<uint32_t>::max();

  // reset stream
  _data = ViewCast<byte_type>(value);
  _begin = _data.data();
  _ngram_end = _begin;
  _data_end = _data.data() + _data.size();
  offset.start = 0;
  _length = 0;
  if (_options.preserve_original) {
    if (!_start_marker_empty) {
      _emit_original = EmitOriginal::WithStartMarker;
    } else if (!_end_marker_empty) {
      _emit_original = EmitOriginal::WithEndMarker;
    } else {
      _emit_original = EmitOriginal::WithoutMarkers;
    }
  } else {
    _emit_original = EmitOriginal::None;
  }
  _next_inc_val = 1;
  SDB_ASSERT(_length < _options.min_gram);
  const size_t max_marker_size =
    std::max(_options.start_marker.size(), _options.end_marker.size());
  if (max_marker_size > 0) {
    // we have at least one marker. As we need to append marker to ngram and
    // provide term value as continious buffer, we can`t return pointer to some
    // byte inside input stream but rather we return pointer to buffer with
    // copied values of ngram and marker For sake of performance we allocate
    // requested memory right now
    size_t buffer_size = _options.preserve_original
                           ? _data.size()
                           : std::min(_data.size(), _options.max_gram);
    buffer_size += max_marker_size;
    _marked_term_buffer.reserve(buffer_size);
  }
  return true;
}

template<NGramTokenizerBase::InputType StreamType>
bool NGramTokenizer<StreamType>::NextSymbol(
  const byte_type*& it) const noexcept {
  SDB_ASSERT(it);
  if (it == _data_end) [[unlikely]] {
    return false;
  }
  if constexpr (StreamType == InputType::Binary) {
    ++it;
  } else if constexpr (StreamType == InputType::UTF8) {
    it = utf8_utils::Next(it, _data_end);
  }
  return true;
}

template<NGramTokenizerBase::InputType StreamType>
bool NGramTokenizer<StreamType>::next() noexcept {
  auto& term = std::get<TermAttr>(_attrs);
  auto& offset = std::get<OffsAttr>(_attrs);
  auto& inc = std::get<IncAttr>(_attrs);

  while (_begin < _data_end) {
    if (_length < _options.max_gram && NextSymbol(_ngram_end)) {
      // we have next ngram from current position
      ++_length;
      if (_length >= _options.min_gram) {
        SDB_ASSERT(_begin <= _ngram_end);
        SDB_ASSERT(static_cast<size_t>(std::distance(_begin, _ngram_end)) <=
                   std::numeric_limits<uint32_t>::max());
        const auto ngram_byte_len =
          static_cast<uint32_t>(std::distance(_begin, _ngram_end));
        if (EmitOriginal::None == _emit_original || 0 != offset.start ||
            ngram_byte_len != _data.size()) {
          offset.end = offset.start + ngram_byte_len;
          inc.value = _next_inc_val;
          _next_inc_val = 0;
          if ((0 != offset.start || _start_marker_empty) &&
              (_end_marker_empty || _ngram_end != _data_end)) {
            term.value = irs::bytes_view(_begin, ngram_byte_len);
          } else if (0 == offset.start && !_start_marker_empty) {
            _marked_term_buffer.clear();
            SDB_ASSERT(_marked_term_buffer.capacity() >=
                       (_options.start_marker.size() + ngram_byte_len));
            _marked_term_buffer.append_range(_options.start_marker);
            _marked_term_buffer.append(_begin, ngram_byte_len);
            term.value = _marked_term_buffer;
            SDB_ASSERT(_marked_term_buffer.size() <=
                       std::numeric_limits<uint32_t>::max());
            if (ngram_byte_len == _data.size() && !_end_marker_empty) {
              // this term is whole original stream and we have end marker, so
              // we need to emit this term again with end marker just like
              // original, so pretend we need to emit original
              _emit_original = EmitOriginal::WithEndMarker;
            }
          } else {
            SDB_ASSERT(!_end_marker_empty && _ngram_end == _data_end);
            _marked_term_buffer.clear();
            SDB_ASSERT(_marked_term_buffer.capacity() >=
                       (_options.end_marker.size() + ngram_byte_len));
            _marked_term_buffer.append(_begin, ngram_byte_len);
            _marked_term_buffer.append_range(_options.end_marker);
            term.value = _marked_term_buffer;
          }
        } else {
          // if ngram covers original stream we need to process it specially
          emit_original();
        }
        return true;
      }
    } else if (EmitOriginal::None == _emit_original) {
      // need to move to next position
      if (!NextSymbol(_begin)) [[unlikely]] {
        return false;  // stream exhausted
      }
      _next_inc_val = 1;
      _length = 0;
      _ngram_end = _begin;
      offset.start = static_cast<uint32_t>(std::distance(_data.data(), _begin));
    } else {
      // as stream has unsigned incremet attribute
      // we cannot go back, so we must emit original before we leave start pos
      // in stream (as it starts from pos=0 in stream)
      emit_original();
      return true;
    }
  }
  return false;
}

template class NGramTokenizer<NGramTokenizerBase::InputType::Binary>;
template class NGramTokenizer<NGramTokenizerBase::InputType::UTF8>;

}  // namespace irs::analysis
