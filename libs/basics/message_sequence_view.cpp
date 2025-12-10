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

#include "basics/message_sequence_view.h"

#include <absl/strings/escaping.h>
#include <absl/strings/internal/ostringstream.h>

#include "basics/assert.h"

namespace sdb::message {

SequenceView::ConstIterator& SequenceView::ConstIterator::operator++() {
  SDB_ASSERT(_it != _end);
  if (_it.chunk == _end.chunk) [[unlikely]] {
    _it = _end;
  } else {
    auto* next = _it.chunk->Next();
    SDB_ASSERT(next->GetBegin() == 0);
    _it = {next, 0};
    SDB_ASSERT(_it);
  }
  return *this;
}

boost::asio::const_buffer SequenceView::ConstIterator::operator*() const {
  SDB_ASSERT(_it != _end);
  SDB_ASSERT(_it.in_chunk == _it.chunk->GetBegin());
  if (_it.chunk != _end.chunk) [[likely]] {
    return _it.chunk->Data(_it.chunk->GetEnd());
  }
  SDB_ASSERT(_it.in_chunk < _end.in_chunk);
  auto s = _it.chunk->Data(_end.in_chunk);
  return s;
}

std::string SequenceView::Print() const {
  std::string str;
  auto it = begin();
  auto end_it = end();
  absl::strings_internal::OStringStream ss{&str};
  while (it != end_it) {
    std::string_view sv{static_cast<const char*>((*it).data()), (*it).size()};
    for (size_t i = 0; i < sv.size(); ++i) {
      if (std::isgraph(sv[i])) {
        ss << sv[i] << "(" << (int)sv[i] << ") ";
      } else {
        ss << (int)sv[i] << " ";
      }
    }
    ++it;
  }
  return str;
}

}  // namespace sdb::message
