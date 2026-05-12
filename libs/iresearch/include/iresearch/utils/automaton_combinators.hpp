#pragma once

#include "iresearch/utils/automaton.hpp"

namespace irs {

automaton IntersectAutomatons(const automaton& a1, const automaton& a2);

automaton UnionAutomatons(const automaton& a1, const automaton& a2);

automaton UnionAutomatonsDeMorgan(const automaton& a1, const automaton& a2);

}  // namespace irs
