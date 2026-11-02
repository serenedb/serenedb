import json
from pathlib import Path


class Timing:
    def __init__(self, units=None, priors=None):
        self.units = dict(units or {})
        self.priors = dict(priors or {})

    @classmethod
    def load(cls, path):
        if not path:
            return cls()
        p = Path(path)
        if not p.is_file():
            return cls()
        data = json.loads(p.read_text())
        return cls(data.get("units", {}), data.get("priors", {}))

    def expect(self, unit_id):
        return self.units.get(unit_id, self.priors.get(unit_id))

    def measured(self, unit_id):
        return unit_id in self.units
