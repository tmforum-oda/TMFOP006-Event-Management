"""Every local module the operator imports must be copied into the image.

The Dockerfile lists the files one by one, so a new module that is not added there starts
fine in tests and then crash-loops in the cluster with ModuleNotFoundError.
"""

import ast
import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "source" / "eventOperator"


def _local_modules() -> set:
    return {p.stem for p in SRC.glob("*.py")}


def _imports(path: Path) -> set:
    found = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.add(node.module.split(".")[0])
    return found


def test_every_imported_local_module_is_copied_into_the_image():
    copied = set()
    for line in (SRC / "Dockerfile").read_text().splitlines():
        if line.startswith("COPY ") and ".py" in line:
            copied |= {Path(t).stem for t in re.findall(r"[\w.]+\.py", line)}

    local = _local_modules()
    needed = set()
    queue = ["eventManagementOperator"]
    while queue:  # follow imports between local modules
        mod = queue.pop()
        if mod in needed:
            continue
        needed.add(mod)
        queue += [m for m in _imports(SRC / f"{mod}.py") if m in local]

    missing = needed - copied
    assert not missing, f"imported by the operator but not COPY'd in the Dockerfile: {sorted(missing)}"
