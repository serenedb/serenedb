import json
import threading


class Journal:
    def __init__(self, path):
        self.path = str(path)
        self._fh = open(self.path, "w", buffering=1)
        self._lock = threading.Lock()
        self._count = 0

    def write(self, record):
        line = json.dumps(record, default=str)
        with self._lock:
            self._fh.write(line + "\n")
            self._count += 1

    def count(self):
        return self._count

    def close(self):
        with self._lock:
            try:
                self._fh.flush()
                self._fh.close()
            except OSError:
                pass


def render_repro(summary_path, meta, findings, tail):
    lines = []
    lines.append("SereneDB catalog stress: FAILED")
    lines.append("")
    lines.append("reproduce:")
    lines.append(f"  {meta['repro_cmd']}")
    lines.append("")
    for k in ("profile", "scenario", "seed", "workers", "seconds", "binary",
              "build_config", "port", "datadir", "server_log", "journal"):
        if k in meta:
            lines.append(f"{k:14} {meta[k]}")
    lines.append("")
    lines.append(f"findings ({len(findings)}):")
    for f in findings[:40]:
        lines.append(f"  [{f.get('kind')}] {f.get('detail')}")
        if f.get("key"):
            lines.append(f"      key={f['key']} candidates={f.get('candidates')} "
                         f"observed={f.get('observed')}")
    if len(findings) > 40:
        lines.append(f"  ... {len(findings) - 40} more in the json report")
    if tail:
        lines.append("")
        lines.append("last journal records:")
        for rec in tail:
            lines.append("  " + json.dumps(rec, default=str)[:400])
    text = "\n".join(lines) + "\n"
    with open(summary_path, "w") as fh:
        fh.write(text)
    return text
