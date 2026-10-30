import os

from . import initdb, source
from .common import GENERATED_DIR, generated


class Generator:
    def __init__(self):
        self.version = f'PostgreSQL {source.version()} in third_party/postgres'
        self.relations, self.rows, self.indexes = source.bki()
        self.row_types = source.row_types(self.relations, self.rows['pg_type'])
        self.initdb = initdb.Initdb()
        self.outputs = {}

    def find(self, catalog, **match):
        return [row for row in self.rows[catalog]
                if all(row[key] == value for key, value in match.items())]

    def one(self, catalog, **match):
        rows = self.find(catalog, **match)
        if len(rows) != 1:
            raise SystemExit(f'{catalog} has {len(rows)} rows for {match}')
        return rows[0]

    def write(self, name, lines):
        self.outputs[os.path.join(GENERATED_DIR, name)] = generated(
            self.version, lines)
