import json
from pathlib import Path


class Timing:
    def __init__(self, units=None):
        self.units = dict(units or {})

    @classmethod
    def load(cls, path):
        if not path:
            return cls()
        p = Path(path)
        if not p.is_file():
            return cls()
        data = json.loads(p.read_text())
        return cls(data.get("units", {}))

    def expect(self, unit_id):
        return self.units.get(unit_id)

    def save(self, path):
        Path(path).write_text(json.dumps({"units": self.units}, sort_keys=True))
