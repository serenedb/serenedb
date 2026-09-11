import enum

ABSENT = None


class Present:
    __slots__ = ("token", "rows")

    def __init__(self, token, rows=frozenset()):
        self.token = token
        self.rows = frozenset(rows)

    def with_rows(self, rows):
        return Present(self.token, rows)

    def __eq__(self, other):
        return (isinstance(other, Present) and other.token == self.token
                and other.rows == self.rows)

    def __hash__(self):
        return hash((self.token, self.rows))

    def __repr__(self):
        n = len(self.rows)
        return f"Present({self.token}, {n} row{'' if n == 1 else 's'})"


class Outcome(enum.Enum):
    COMMITTED = "committed"
    REFUSED_CONFLICT = "refused_conflict"
    REFUSED_PERMANENT = "refused_permanent"
    UNKNOWN_CRASH = "unknown_crash"
    UNKNOWN_CANCEL = "unknown_cancel"
    UNKNOWN_TIMEOUT = "unknown_timeout"

    @property
    def ambiguous(self):
        return self in (
            Outcome.UNKNOWN_CRASH,
            Outcome.UNKNOWN_CANCEL,
            Outcome.UNKNOWN_TIMEOUT,
        )

    @property
    def applied(self):
        return self is Outcome.COMMITTED


class ModelError(Exception):
    pass


class Finding:
    def __init__(self, kind, key, detail, candidates=None, observed=None):
        self.kind = kind
        self.key = key
        self.detail = detail
        self.candidates = candidates
        self.observed = observed

    def as_dict(self):
        return {
            "kind": self.kind,
            "key": list(self.key) if isinstance(self.key, tuple) else self.key,
            "detail": self.detail,
            "candidates": sorted(
                repr(c) if c is not ABSENT else "ABSENT"
                for c in (self.candidates or ())
            ) or None,
            "observed": ("ABSENT" if self.observed is ABSENT
                         else repr(self.observed)),
        }

    def __repr__(self):
        return f"Finding({self.kind}, {self.key}, {self.detail})"


MAX_CANDIDATES = 8


class Model:
    def __init__(self):
        self._owned = {}
        self._shared = set()
        self._ambiguous_ops = 0
        self._incarnation = {}

    def declare_owned(self, key):
        if key in self._shared:
            raise ModelError(f"{key} already declared shared")
        self._owned.setdefault(key, frozenset({ABSENT}))

    def declare_shared(self, key):
        if key in self._owned:
            raise ModelError(f"{key} already declared owned")
        self._shared.add(key)

    def is_owned(self, key):
        return key in self._owned

    def is_shared(self, key):
        return key in self._shared

    def owned_keys(self):
        return dict(self._owned)

    def shared_keys(self):
        return set(self._shared)

    def candidates(self, key):
        return self._owned.get(key)

    def ambiguous_keys(self):
        return {k: v for k, v in self._owned.items() if len(v) > 1}

    def ambiguous_op_count(self):
        return self._ambiguous_ops

    def apply(self, key, after, outcome):
        if key in self._shared:
            return
        if key not in self._owned:
            raise ModelError(f"apply on undeclared key {key}")
        before = self._owned[key]
        if outcome.applied:
            nxt = frozenset({after})
        elif outcome.ambiguous:
            nxt = before | {after}
            self._ambiguous_ops += 1
        else:
            nxt = before
        if len(nxt) > MAX_CANDIDATES:
            raise ModelError(
                f"candidate set for {key} grew to {len(nxt)}; quiesce more often"
            )
        self._owned[key] = nxt

    def apply_create(self, key, token, outcome, rows=frozenset()):
        if outcome.applied:
            self._incarnation[key] = self._incarnation.get(key, 0) + 1
        self.apply(key, Present(token, rows), outcome)

    def incarnation(self, key):
        return self._incarnation.get(key, 0)

    def rows_of(self, key):
        cands = self._owned.get(key)
        if cands is None or len(cands) != 1:
            return None
        only = next(iter(cands))
        return only.rows if isinstance(only, Present) else None

    def apply_rows(self, key, added=(), removed=(), outcome=None):
        if key in self._shared or key not in self._owned:
            return
        before = self._owned[key]
        nxt = set()
        for state in before:
            if not isinstance(state, Present):
                nxt.add(state)
                continue
            rows = set(state.rows)
            rows.difference_update(removed)
            rows.update(added)
            after = state.with_rows(rows)
            if outcome is not None and outcome.applied:
                nxt.add(after)
            elif outcome is not None and outcome.ambiguous:
                nxt.add(state)
                nxt.add(after)
            else:
                nxt.add(state)
        if len(nxt) > MAX_CANDIDATES:
            raise ModelError(
                f"candidate set for {key} grew to {len(nxt)}; quiesce more often")
        self._owned[key] = frozenset(nxt)

    def apply_drop(self, key, outcome):
        self.apply(key, ABSENT, outcome)

    def apply_cascade(self, keys, outcome):
        for key in keys:
            if key in self._owned:
                self.apply(key, ABSENT, outcome)

    def collapse(self, observed):
        findings = []
        for key, cands in list(self._owned.items()):
            if key not in observed:
                seen = ABSENT
            else:
                seen = observed[key]
            if seen in cands:
                self._owned[key] = frozenset({seen})
                continue
            if len(cands) == 1:
                only = next(iter(cands))
                if only is not ABSENT and seen is ABSENT:
                    kind = "missing"
                elif only is ABSENT:
                    kind = "unexpected_present"
                elif (isinstance(only, Present) and isinstance(seen, Present)
                      and only.token == seen.token and only.rows != seen.rows):
                    kind = "wrong_rows"
                else:
                    kind = "wrong_content"
                findings.append(Finding(
                    f"model_disagreement_{kind}", key,
                    "observed state is not the single modelled state",
                    cands, seen,
                ))
            else:
                findings.append(Finding(
                    "ambiguous_resolved_to_third_state", key,
                    "observed state is outside the candidate set an ambiguous "
                    "outcome allowed",
                    cands, seen,
                ))
            self._owned[key] = frozenset({seen})
        return findings

    def expected_present(self):
        out = {}
        for key, cands in self._owned.items():
            if len(cands) == 1:
                only = next(iter(cands))
                if only is not ABSENT:
                    out[key] = only
        return out

    def row_bearing_keys(self):
        out = {}
        for key, cands in self._owned.items():
            states = [c for c in cands if isinstance(c, Present)]
            if states:
                out[key] = states
        return out
