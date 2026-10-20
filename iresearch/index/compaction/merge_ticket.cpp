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

#include "iresearch/index/compaction/merge_ticket.hpp"

#include <utility>

#include "iresearch/index/directory_reader_impl.hpp"
#include "iresearch/index/index_writer.hpp"

namespace irs {

MergeTicket::MergeTicket() noexcept = default;

MergeTicket::MergeTicket(IndexWriter& writer,
                         std::unique_ptr<State> state) noexcept
  : _writer{&writer}, _state{std::move(state)} {}

MergeTicket::MergeTicket(MergeTicket&& other) noexcept
  : _writer{std::exchange(other._writer, nullptr)},
    _state{std::move(other._state)} {}

MergeTicket& MergeTicket::operator=(MergeTicket&& other) noexcept {
  if (this != &other) {
    Reset();
    _writer = std::exchange(other._writer, nullptr);
    _state = std::move(other._state);
  }
  return *this;
}

MergeTicket::~MergeTicket() { Reset(); }

CompactionView MergeTicket::Candidates() const noexcept {
  if (!_state) {
    return {};
  }
  return _state->candidates;
}

bool MergeTicket::Revoked() const noexcept {
  return _state && _state->revoked.load(std::memory_order_relaxed);
}

void MergeTicket::Reset() noexcept {
  if (!_state) {
    return;
  }
  _writer->ReleaseTicket(*_state);
  _state.reset();
  _writer = nullptr;
}

}  // namespace irs
