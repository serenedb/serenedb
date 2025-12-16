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

#include <boost/asio/buffer.hpp>

#include "basics/message_chunk.h"

namespace sdb::message {

class SequenceView {
 public:
  SequenceView() = default;
  SequenceView(BufferOffset begin, BufferOffset end)
    : _begin{begin}, _end{end} {}

  std::string Print() const;

  bool Empty() const { return _begin == _end; }

  class ConstIterator {
   public:
    using value_type = boost::asio::const_buffer;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = boost::asio::const_buffer&;
    using iterator_category = std::forward_iterator_tag;

    explicit ConstIterator(BufferOffset it, BufferOffset end)
      : _it{it}, _end{end} {}

    ConstIterator& operator++();
    ConstIterator operator++(int) {
      auto it = *this;
      ++(*this);
      return it;
    }

    boost::asio::const_buffer operator*() const;

    bool operator==(const ConstIterator& it) const = default;

   private:
    BufferOffset _it;
    BufferOffset _end;
  };

  ConstIterator begin() const { return ConstIterator{_begin, _end}; }
  ConstIterator end() const { return ConstIterator{_end, _end}; }

 private:
  BufferOffset _begin;
  BufferOffset _end;
};

}  // namespace sdb::message
