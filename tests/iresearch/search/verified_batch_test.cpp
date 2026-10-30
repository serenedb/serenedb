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

#include <cstdint>
#include <iresearch/search/detail/verified_batch.hpp>
#include <iresearch/search/lead/two_phase.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "tests_shared.hpp"

namespace {

bool Accepts(irs::doc_id_t doc) noexcept { return doc % 3 == 0; }

class Approx {
 public:
  explicit Approx(std::vector<irs::doc_id_t> docs) : _docs{std::move(docs)} {}

  irs::doc_id_t Next() {
    ++_pos;
    return Current();
  }

  irs::doc_id_t Seek(irs::doc_id_t target) {
    if (_pos < 0) {
      _pos = 0;
    }
    while (static_cast<size_t>(_pos) < _docs.size() && _docs[_pos] < target) {
      ++_pos;
    }
    return Current();
  }

 private:
  irs::doc_id_t Current() const {
    return static_cast<size_t>(_pos) < _docs.size() ? _docs[_pos]
                                                    : irs::doc_limits::eof();
  }

  std::vector<irs::doc_id_t> _docs;
  int64_t _pos = -1;
};

struct Verifier {
  void Check(std::span<const irs::doc_id_t> docs, std::span<bool> matched) {
    sizes.push_back(docs.size());
    for (size_t i = 0; i < docs.size(); ++i) {
      matched[i] = Accepts(docs[i]);
    }
  }

  std::vector<size_t> sizes;
};

struct BatchedSlots {
  explicit BatchedSlots(Approx docs) : approx{std::move(docs)} {}

  irs::doc_id_t Seek(irs::doc_id_t target) {
    return batch.Seek(approx, verifier, target);
  }
  irs::doc_id_t Next(irs::doc_id_t) { return batch.Next(approx, verifier); }
  bool Match(irs::doc_id_t doc) const { return batch.Match(doc); }

  Approx approx;
  Verifier verifier;
  irs::detail::VerifiedBatch<Verifier> batch;
};

struct PlainSlots {
  explicit PlainSlots(Approx docs) : approx{std::move(docs)} {}

  irs::doc_id_t Seek(irs::doc_id_t target) { return approx.Seek(target); }
  irs::doc_id_t Next(irs::doc_id_t) { return approx.Next(); }
  bool Match(irs::doc_id_t doc) const { return Accepts(doc); }

  Approx approx;
};

std::vector<irs::doc_id_t> Candidates(std::mt19937_64& rng, size_t count) {
  std::vector<irs::doc_id_t> docs;
  irs::doc_id_t doc = irs::doc_limits::min();
  for (size_t i = 0; i < count; ++i) {
    doc += 1 + static_cast<irs::doc_id_t>(rng() % 9);
    docs.push_back(doc);
  }
  return docs;
}

TEST(VerifiedBatchTest, NextReturnsWhatOneByOneChecksReturn) {
  std::mt19937_64 rng{11};
  const auto docs = Candidates(rng, 5000);
  irs::lead::TwoPhaseDocs<BatchedSlots> batched{Approx{docs}};
  irs::lead::TwoPhaseDocs<PlainSlots> plain{Approx{docs}};
  for (;;) {
    const auto want = plain.Next();
    ASSERT_EQ(want, batched.Next());
    if (irs::doc_limits::eof(want)) {
      break;
    }
  }
}

TEST(VerifiedBatchTest, SeeksAndNextsReturnWhatOneByOneChecksReturn) {
  std::mt19937_64 rng{17};
  for (int round = 0; round < 50; ++round) {
    const auto docs = Candidates(rng, 2000);
    irs::lead::TwoPhaseDocs<BatchedSlots> batched{Approx{docs}};
    irs::lead::TwoPhaseDocs<PlainSlots> plain{Approx{docs}};
    irs::doc_id_t target = irs::doc_limits::min();
    for (;;) {
      irs::doc_id_t want;
      irs::doc_id_t got;
      if (rng() % 3 == 0) {
        target += static_cast<irs::doc_id_t>(rng() % 400);
        want = plain.Seek(target);
        got = batched.Seek(target);
      } else {
        want = plain.Next();
        got = batched.Next();
      }
      ASSERT_EQ(want, got);
      if (irs::doc_limits::eof(want)) {
        break;
      }
      target = std::max(target, want);
    }
  }
}

TEST(VerifiedBatchTest, NextGrowsTheBatchAndFarSeeksShrinkIt) {
  std::vector<irs::doc_id_t> docs;
  for (irs::doc_id_t doc = 1; doc <= 100000; ++doc) {
    docs.push_back(doc);
  }
  BatchedSlots slots{Approx{docs}};
  for (int i = 0; i < 1000; ++i) {
    ASSERT_FALSE(irs::doc_limits::eof(slots.Next(0)));
  }
  EXPECT_EQ(irs::detail::VerifiedBatch<Verifier>::kMaxSize,
            slots.verifier.sizes.back());

  irs::doc_id_t target = 2000;
  for (int i = 0; i < 10; ++i) {
    target += 5000;
    ASSERT_EQ(target, slots.Seek(target));
  }
  EXPECT_EQ(1u, slots.verifier.sizes.back());
}

}  // namespace
