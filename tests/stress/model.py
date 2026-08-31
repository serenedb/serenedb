import enum

ABSENT = None


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
                ("ABSENT" if c is ABSENT else c) for c in (self.candidates or ())
            ) or None,
            "observed": "ABSENT" if self.observed is ABSENT else self.observed,
        }

    def __repr__(self):
        return f"Finding({self.kind}, {self.key}, {self.detail})"


MAX_CANDIDATES = 8


class Model:
    def __init__(self):
        self._owned = {}
        self._shared = set()
        self._ambiguous_ops = 0

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

    def apply_create(self, key, token, outcome):
        self.apply(key, token, outcome)

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
                kind = ("missing" if only is not ABSENT and seen is ABSENT
                        else "unexpected_present" if only is ABSENT
                        else "wrong_content")
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
