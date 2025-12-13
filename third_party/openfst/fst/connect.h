// Copyright 2005-2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Classes and functions to remove unsuccessful paths from an FST.

#ifndef FST_CONNECT_H_
#define FST_CONNECT_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include <fst/cc-visitors.h>
#include <fst/dfs-visit.h>
#include <fst/fst.h>
#include <fst/mutable-fst.h>
#include <fst/properties.h>

namespace fst {

// Trims an FST, removing states and arcs that are not on successful paths.
// This version modifies its input.
//
// Complexity:
//
//   Time:  O(V + E)
//   Space: O(V + E)
//
// where V = # of states and E = # of arcs.
template <class Arc>
void Connect(MutableFst<Arc> *fst) {
  using StateId = typename Arc::StateId;
  std::vector<bool> access;
  std::vector<bool> coaccess;
  uint64_t props = 0;
  SccVisitor<Arc> scc_visitor(nullptr, &access, &coaccess, &props);
  DfsVisit(*fst, &scc_visitor);
  std::vector<StateId> dstates;
  dstates.reserve(access.size());
  for (StateId s = 0; s < access.size(); ++s) {
    if (!access[s] || !coaccess[s]) dstates.push_back(s);
  }
  fst->DeleteStates(dstates);
  fst->SetProperties(kAccessible | kCoAccessible, kAccessible | kCoAccessible);
}

// Returns an acyclic FST where each SCC in the input FST has been condensed to
// a single state with transitions between SCCs retained and within SCCs
// dropped. Also populates 'scc' with a mapping from input to output states.
template <class Arc>
void Condense(const Fst<Arc> &ifst, MutableFst<Arc> *ofst,
              std::vector<typename Arc::StateId> *scc) {
  using StateId = typename Arc::StateId;
  ofst->DeleteStates();
  uint64_t props = 0;
  SccVisitor<Arc> scc_visitor(scc, nullptr, nullptr, &props);
  DfsVisit(ifst, &scc_visitor);
  const auto iter = std::max_element(scc->cbegin(), scc->cend());
  if (iter == scc->cend()) return;
  const StateId num_condensed_states = 1 + *iter;
  ofst->ReserveStates(num_condensed_states);
  for (StateId c = 0; c < num_condensed_states; ++c) {
    ofst->AddState();
  }
  for (StateId s = 0; s < scc->size(); ++s) {
    const auto c = (*scc)[s];
    if (s == ifst.Start()) ofst->SetStart(c);
    const auto weight = ifst.Final(s);
    if (weight != Arc::Weight::Zero())
      ofst->SetFinal(c, Plus(ofst->Final(c), weight));
    for (ArcIterator<Fst<Arc>> aiter(ifst, s); !aiter.Done(); aiter.Next()) {
      const auto &arc = aiter.Value();
      const auto nextc = (*scc)[arc.nextstate];
      if (nextc != c) {
        Arc condensed_arc = arc;
        condensed_arc.nextstate = nextc;
        ofst->AddArc(c, std::move(condensed_arc));
      }
    }
  }
  ofst->SetProperties(kAcyclic | kInitialAcyclic, kAcyclic | kInitialAcyclic);
}

}  // namespace fst

#endif  // FST_CONNECT_H_
