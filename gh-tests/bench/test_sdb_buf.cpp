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

#include <iostream>
#include <thread>

#include "basics/assert.h"
#include "basics/message_buffer.h"

int main() {
  std::string in;
  std::string out;
  sdb::message::SequenceView seq;
  std::atomic_bool f{false};
  sdb::message::Buffer buffer{64, 4096, 0, [&](sdb::message::SequenceView s) {
                                SDB_ASSERT(seq.Empty());
                                seq = s;
                                SDB_ASSERT(!f.load());
                                f.store(true);
                              }};
  size_t N = 4;
  auto t1 = std::thread([&] {
    int n = N;
    for (int i = 0; i < n; ++i) {
      std::string str = std::to_string(i) + "_23456789";
      in += str;
      buffer.Write(str, true);
    }
    n = 0;
  });

  auto t2 = std::thread([&] {
    while (out.size() < N * 10) {
      while (!f.load()) {
      }
      auto it = seq.begin();
      while (it != seq.end()) {
        out += std::string{(char*)((*it).data()), (*it).size()};
        it++;
      }
      f.store(false);
      seq = sdb::message::SequenceView{};
      buffer.FlushDone();
    }
  });
  t1.join();
  t2.join();
  std::cout << in << "\n" << out << "\n";
  SDB_ASSERT(in == out);
}
