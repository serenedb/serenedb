import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))

for path in (HERE, os.path.join(REPO, "tests", "harness", "python")):
    if path not in sys.path:
        sys.path.insert(0, path)
