import datetime
import os

try:
    import yaml
except ImportError:
    yaml = None

DEFAULT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "known_wedges.yaml")


class QuarantineError(Exception):
    pass


class Entry:
    def __init__(self, raw):
        for field in ("id", "kinds", "expires", "why"):
            if field not in raw:
                raise QuarantineError(f"quarantine entry missing '{field}': {raw}")
        self.id = raw["id"]
        self.kinds = frozenset(raw["kinds"])
        self.why = raw["why"]
        self.issue = raw.get("issue")
        self.expires = _as_date(raw["expires"], self.id)
        fp = raw.get("fingerprint") or {}
        self.detail_contains = list(fp.get("detail_contains") or ())
        self.scenarios = frozenset(fp.get("scenarios") or ())
        self.min_workers = fp.get("min_workers")
        if not self.detail_contains and not self.scenarios:
            raise QuarantineError(
                f"entry '{self.id}' has no fingerprint; a quarantine that matches "
                "on kind alone would hide unrelated regressions")
        self.matched = 0

    def matches(self, finding, scenario, workers):
        if finding.get("kind") not in self.kinds:
            return False
        detail = str(finding.get("detail") or "")
        if self.detail_contains and not any(m in detail for m in self.detail_contains):
            return False
        if self.scenarios and scenario not in self.scenarios:
            return False
        if self.min_workers is not None and workers < self.min_workers:
            return False
        return True

    def expired(self, today):
        return today > self.expires

    def as_dict(self):
        return {"id": self.id, "matched": self.matched,
                "expires": self.expires.isoformat(), "issue": self.issue}


def _as_date(value, entry_id):
    if isinstance(value, datetime.date):
        return value
    try:
        return datetime.date.fromisoformat(str(value))
    except ValueError as exc:
        raise QuarantineError(
            f"entry '{entry_id}' has an unparseable expires '{value}': {exc}")


def load(path=DEFAULT_PATH):
    if not os.path.exists(path):
        return []
    if yaml is None:
        raise QuarantineError(
            f"{path} exists but pyyaml is not importable; refusing to run with an "
            "unreadable quarantine list rather than silently ignoring it")
    raw = yaml.safe_load(open(path).read()) or {}
    return [Entry(e) for e in (raw.get("known_wedges") or ())]


def apply(findings, entries, scenario, workers, today=None):
    today = today or datetime.date.today()
    surviving, quarantined, meta = [], [], []
    for finding in findings:
        hit = None
        for entry in entries:
            if entry.matches(finding, scenario, workers):
                hit = entry
                break
        if hit is None:
            surviving.append(finding)
            continue
        hit.matched += 1
        if hit.expired(today):
            item = dict(finding)
            item["quarantine_expired"] = hit.id
            item["detail"] = (
                f"[quarantine {hit.id} EXPIRED {hit.expires.isoformat()}] "
                + str(finding.get("detail") or ""))
            surviving.append(item)
        else:
            item = dict(finding)
            item["quarantined_by"] = hit.id
            quarantined.append(item)

    for entry in entries:
        if entry.expired(today):
            meta.append({
                "kind": "quarantine_entry_expired", "key": None,
                "detail": f"quarantine '{entry.id}' expired on "
                          f"{entry.expires.isoformat()}; fix it or move the date "
                          f"deliberately ({entry.issue or 'no issue linked'})",
                "candidates": None, "observed": None})
        elif entry.matched == 0 and entry.scenarios and scenario in entry.scenarios:
            meta.append({
                "kind": "quarantine_no_longer_reproduces", "key": None,
                "detail": f"quarantine '{entry.id}' matched nothing in a run of "
                          f"'{scenario}' that it claims to cover; if it is fixed, "
                          f"delete the entry ({entry.issue or 'no issue linked'})",
                "candidates": None, "observed": None})
    return surviving + meta, quarantined
