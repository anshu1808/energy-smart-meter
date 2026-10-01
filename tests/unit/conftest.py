import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
for sub in ("ci", "scripts", "src"):
    sys.path.insert(0, str(ROOT / sub))
