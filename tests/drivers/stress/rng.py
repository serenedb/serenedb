_MASK = (1 << 64) - 1
_GOLDEN = 0x9E3779B97F4A7C15


def _mix(z):
    z = (z + _GOLDEN) & _MASK
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK
    return z ^ (z >> 31)


class Stream:
    __slots__ = ("_state",)

    def __init__(self, seed):
        self._state = seed & _MASK

    def next_u64(self):
        self._state = (self._state + _GOLDEN) & _MASK
        return _mix(self._state - _GOLDEN)

    def below(self, n):
        return self.next_u64() % n if n > 0 else 0

    def fraction(self):
        return self.next_u64() / float(1 << 64)

    def choice(self, seq):
        return seq[self.below(len(seq))]

    def weighted(self, pairs):
        total = sum(w for _, w in pairs)
        pick = self.below(total) if total else 0
        acc = 0
        for item, w in pairs:
            acc += w
            if pick < acc:
                return item
        return pairs[-1][0]


def derive(base_seed, worker_id, salt=0):
    return Stream(_mix((base_seed & _MASK) ^ _mix((worker_id << 32) ^ salt)))
