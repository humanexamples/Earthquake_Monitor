"""Run py_compile on every *.py file in the given folder (default: current dir)."""
import pathlib
import py_compile
import sys

folder = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(".")
ok = True

for path in sorted(folder.glob("*.py")):
    try:
        py_compile.compile(str(path), doraise=True)
        print(f"OK   {path.name}")
    except py_compile.PyCompileError as e:
        print(f"FAIL {path.name}: {e}")
        ok = False

sys.exit(0 if ok else 1)
